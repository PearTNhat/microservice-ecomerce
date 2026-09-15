package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/redislock"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/dto"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// FlashSaleProjectionWorker tiêu thụ FLASH_SALE_ORDER_CONFIRMED để cập nhật Redis Projection (Safety net)
type FlashSaleProjectionWorker struct {
	reader      *kafka.Reader
	redisClient *redis.Client
	fsRepo      domain.FlashSaleRepository
}

func NewFlashSaleProjectionWorker(
	brokers []string,
	redisClient *redis.Client,
	fsRepo domain.FlashSaleRepository,
) *FlashSaleProjectionWorker {
	if len(brokers) == 0 || redisClient == nil {
		return nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          pkgKafka.TopicFlashSaleConfirmed,
		GroupID:        pkgKafka.ConsumerGroupOrderRedisProjection,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})

	return &FlashSaleProjectionWorker{
		reader:      reader,
		redisClient: redisClient,
		fsRepo:      fsRepo,
	}
}

func (w *FlashSaleProjectionWorker) Start(ctx context.Context) {
	if w.reader == nil {
		return
	}

	logger.Info("⚡ [KAFKA FLASH SALE] Order Redis Projection Worker đang lắng nghe topic flashsale.confirmed...",
		"group_id", pkgKafka.ConsumerGroupOrderRedisProjection,
	)

	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("🛑 FlashSaleProjectionWorker nhận tín hiệu dừng")
				_ = w.reader.Close()
				return
			default:
				m, err := w.reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					time.Sleep(500 * time.Millisecond)
					continue
				}

				if err := w.processConfirmation(ctx, m); err != nil {
					logger.Error("❌ FlashSaleProjectionWorker lỗi cập nhật Redis projection, hoãn commit", "error", err.Error())
					time.Sleep(1 * time.Second)
					continue
				}

				_ = w.reader.CommitMessages(ctx, m)
			}
		}
	}()
}

func (w *FlashSaleProjectionWorker) processConfirmation(ctx context.Context, m kafka.Message) error {
	var payload pkgKafka.FlashSaleOrderConfirmedPayload
	if err := json.Unmarshal(m.Value, &payload); err != nil {
		logger.Error("❌ FlashSaleProjectionWorker: Lỗi deserialize JSON payload", "error", err.Error())
		return nil // Poison message
	}

	traceID := payload.TraceID
	if traceID == "" {
		traceID = fmt.Sprintf("proj-%s", payload.ReservationID)
	}
	reqCtx := logger.SetTraceID(ctx, traceID)

	// 1. Gọi idempotent script Confirm trên Redis
	confirmResp, err := redislock.ConfirmFlashSaleReservation(reqCtx, w.redisClient, payload.CampaignID, payload.ProductID, payload.ReservationID)
	if err != nil {
		logger.WarnContext(reqCtx, "⚠️ FlashSaleProjectionWorker: Confirm trên Redis gặp lỗi mạng/kết nối", "error", err.Error())
		return err
	}

	if confirmResp.Code == "INVALID_STATE" {
		// Key reservation không tồn tại trên Redis (ví dụ do Redis vừa bị flush hoặc key mất)
		// Cần rebuild projection từ Order DB thay vì retry vô tận
		logger.WarnContext(reqCtx, "⚠️ [PROJECTION REBUILD] Key reservation trên Redis không tồn tại hoặc INVALID_STATE, tiến hành phục hồi status snapshot",
			"reservation_id", payload.ReservationID,
		)
	}

	// 2. Ghi snapshot order status lên Redis
	statusSnapshot := dto.FlashSaleOrderStatusResponse{
		ReservationID: payload.ReservationID,
		Status:        "CONFIRMED",
		OrderID:       &payload.OrderID,
		OrderCode:     payload.OrderCode,
		UpdatedAt:     time.Now(),
	}
	statusJSON, err := json.Marshal(statusSnapshot)
	if err == nil {
		_ = w.redisClient.Set(reqCtx, redislock.KeyOrderStatus(payload.ReservationID), string(statusJSON), 24*time.Hour)
		// 3. Pub/Sub notification (lỗi Pub/Sub không được chặn commit offset vì Pub/Sub là ephemeral)
		_ = w.redisClient.Publish(reqCtx, fmt.Sprintf("pubsub:order-status:%s", payload.ReservationID), string(statusJSON))
	}

	logger.InfoContext(reqCtx, "✅ [REDIS PROJECTION SYNC] Đã cập nhật durable Redis projection thành công",
		"reservation_id", payload.ReservationID,
		"order_id", payload.OrderID,
		"lua_code", confirmResp.Code,
	)

	return nil
}

func (w *FlashSaleProjectionWorker) Close() error {
	if w.reader != nil {
		return w.reader.Close()
	}
	return nil
}
