package worker

import (
	"context"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/redislock"
	"ecomerce-service/services/order-service/internal/domain"
	pkgKafka "ecomerce-service/pkg/kafka"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// OrderSagaWorker trong Order Service lắng nghe topic stock.events để hoàn tất hoặc hủy đơn hàng
type OrderSagaWorker struct {
	reader      *kafka.Reader
	orderRepo   domain.OrderRepository
	redisClient *redis.Client
	producer    pkgKafka.OrderKafkaProducer
}

// NewOrderSagaWorker khởi tạo Kafka Consumer cho Order Service
func NewOrderSagaWorker(
	brokers []string,
	orderRepo domain.OrderRepository,
	rClient *redis.Client,
	producer pkgKafka.OrderKafkaProducer,
) *OrderSagaWorker {
	if len(brokers) == 0 {
		return nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          pkgKafka.TopicStockEvents,
		GroupID:        pkgKafka.ConsumerGroupOrderSaga,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})

	return &OrderSagaWorker{
		reader:      reader,
		orderRepo:   orderRepo,
		redisClient: rClient,
		producer:    producer,
	}
}

// Start khởi chạy tiến trình lắng nghe kết quả trừ kho
func (w *OrderSagaWorker) Start(ctx context.Context) {
	if w.reader == nil {
		return
	}

	logger.Info("🔄 [KAFKA SAGA] OrderSagaWorker đang lắng nghe topic stock.events...",
		"group_id", pkgKafka.ConsumerGroupOrderSaga,
	)

	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("🛑 OrderSagaWorker nhận tín hiệu dừng")
				_ = w.reader.Close()
				return
			default:
				m, err := w.reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					logger.Warn("⚠️ OrderSagaWorker fetch message lỗi", "error", err.Error())
					time.Sleep(500 * time.Millisecond)
					continue
				}

				w.processMessage(ctx, m)
				_ = w.reader.CommitMessages(ctx, m)
			}
		}
	}()
}

func (w *OrderSagaWorker) processMessage(ctx context.Context, m kafka.Message) {
	var payload pkgKafka.StockResultPayload
	if err := json.Unmarshal(m.Value, &payload); err != nil {
		logger.Error("❌ OrderSagaWorker lỗi deserialize StockResultPayload", "error", err.Error())
		if w.producer != nil {
			_ = w.producer.PublishDeadLetter(ctx, m.Topic, string(m.Key), m.Value, err.Error(), "")
		}
		return
	}

	traceID := payload.TraceID
	reqCtx := logger.SetTraceID(ctx, traceID)

	if payload.Success {
		// Trừ kho thành công -> Cập nhật trạng thái đơn hàng thành CONFIRMED
		err := w.orderRepo.UpdateStatus(payload.OrderID, domain.OrderStatusConfirmed)
		if err != nil {
			logger.ErrorContext(reqCtx, "❌ [SAGA] Lỗi cập nhật trạng thái đơn hàng sang CONFIRMED",
				"order_id", payload.OrderID,
				"error", err.Error(),
			)
			return
		}

		logger.InfoContext(reqCtx, "🎉 [SAGA COMPLETED] Đơn hàng đã được xác nhận thành công!",
			"order_id", payload.OrderID,
			"order_code", payload.OrderCode,
			"new_status", domain.OrderStatusConfirmed,
		)
	} else {
		// Trừ kho thất bại -> Hủy đơn hàng và hoàn lại tồn kho RAM Redis (nếu có)
		err := w.orderRepo.UpdateStatus(payload.OrderID, domain.OrderStatusCancelled)
		if err != nil {
			logger.ErrorContext(reqCtx, "❌ [SAGA] Lỗi cập nhật trạng thái đơn hàng sang CANCELLED",
				"order_id", payload.OrderID,
				"error", err.Error(),
			)
		}

		// Hoàn lại tồn kho RAM Redis nếu trước đó đã giữ
		if w.redisClient != nil && len(payload.Items) > 0 {
			for _, item := range payload.Items {
				_ = redislock.RevertStockAtomic(reqCtx, w.redisClient, item.ProductID, item.Quantity)
			}
		}

		logger.WarnContext(reqCtx, "🚫 [SAGA COMPENSATED] Đơn hàng đã bị HỦY do Product Service báo không đủ tồn kho",
			"order_id", payload.OrderID,
			"order_code", payload.OrderCode,
			"reason", payload.Reason,
			"new_status", domain.OrderStatusCancelled,
		)
	}
}

func (w *OrderSagaWorker) Close() error {
	if w.reader != nil {
		return w.reader.Close()
	}
	return nil
}
