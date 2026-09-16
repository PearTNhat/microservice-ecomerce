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
	"gorm.io/gorm"
)

type ReservationExpiryWorker struct {
	db          *gorm.DB
	fsRepo      domain.FlashSaleRepository
	redisClient *redis.Client
	interval    time.Duration
}

func NewReservationExpiryWorker(
	db *gorm.DB,
	fsRepo domain.FlashSaleRepository,
	redisClient *redis.Client,
) *ReservationExpiryWorker {
	return &ReservationExpiryWorker{
		db:          db,
		fsRepo:      fsRepo,
		redisClient: redisClient,
		interval:    5 * time.Second,
	}
}

func (w *ReservationExpiryWorker) Start(ctx context.Context) {
	if w.fsRepo == nil {
		return
	}

	logger.Info("⏰ [RESERVATION EXPIRY WORKER] Bắt đầu quét các đơn giữ chỗ hết hạn...")

	go func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Info("🛑 ReservationExpiryWorker nhận tín hiệu dừng")
				return
			case <-ticker.C:
				w.processExpiredBatch(ctx)
			}
		}
	}()
}

func (w *ReservationExpiryWorker) processExpiredBatch(ctx context.Context) {
	expiredList, err := w.fsRepo.ClaimExpiredReservations(50)
	if err != nil || len(expiredList) == 0 {
		return
	}

	for _, resv := range expiredList {
		// 1. Giải phóng tồn kho và đổi status trong Database bằng Transaction
		dbErr := w.db.Transaction(func(tx *gorm.DB) error {
			if err := w.fsRepo.ReleaseReservationDB(tx, resv.ID, domain.ReservationStatusExpired); err != nil {
				return err
			}
			// Nếu reservation gắn với đơn hàng đang PENDING -> Tự động hủy đơn và bồi hoàn kho thường nếu có
			if resv.OrderID != nil {
				var order domain.Order
				if err := tx.Preload("Items").Where("id = ? AND order_status = ?", *resv.OrderID, domain.OrderStatusPending).First(&order).Error; err == nil {
					// Tìm các món hàng thường để bồi hoàn (Point 4 & Section 17.2 fix)
					var regularItems []pkgKafka.OrderItemPayload
					for _, item := range order.Items {
						if !item.IsFlashSale {
							regularItems = append(regularItems, pkgKafka.OrderItemPayload{
								ProductID:   item.ProductID,
								ProductName: item.ProductName,
								Quantity:    item.Quantity,
							})
						}
					}

					targetStatus := domain.OrderStatusCancelled
					if len(regularItems) > 0 {
						targetStatus = domain.OrderStatusCompensating
					}

					if err := tx.Model(&order).Update("order_status", targetStatus).Error; err != nil {
						return err
					}

					if len(regularItems) > 0 {
						now := time.Now()
						compPayload := pkgKafka.MixedOrderStockCompensatePayload{
							EventID:   fmt.Sprintf("comp-expiry-%d-%s", order.ID, resv.ID),
							EventType: pkgKafka.EventMixedStockCompensate,
							OrderID:   order.ID,
							OrderCode: order.OrderCode,
							Items:     regularItems,
							Reason:    "Flash Sale reservation expired while order was PENDING",
							TraceID:   logger.GetTraceID(ctx),
							Timestamp: now,
						}
						compData, _ := json.Marshal(compPayload)
						outboxComp := &domain.OutboxEvent{
							ID:            fmt.Sprintf("outbox-comp-exp-%d", order.ID),
							AggregateType: "order",
							AggregateID:   fmt.Sprintf("%d", order.ID),
							EventType:     pkgKafka.EventMixedStockCompensate,
							Topic:         pkgKafka.TopicMixedOrderStockCompensate,
							PartitionKey:  fmt.Sprintf("order-%d", order.ID),
							Payload:       string(compData),
							Status:        domain.OutboxStatusPending,
							CreatedAt:     now,
						}
						if err := tx.Create(outboxComp).Error; err != nil {
							return fmt.Errorf("lỗi ghi outbox bồi hoàn kho thường: %w", err)
						}
					}
				}
			}
			return nil
		})


		if dbErr != nil {
			logger.ErrorContext(ctx, "Lỗi release reservation trong DB", "reservation_id", resv.ID, "error", dbErr.Error())
			continue
		}

		// 2. Sau DB commit: Hoàn lại tồn kho & Quota trên Redis
		if w.redisClient != nil {
			_, _ = redislock.ReleaseFlashSaleReservation(ctx, w.redisClient, resv.CampaignID, resv.ProductID, resv.ID, "EXPIRED")

			// Cập nhật Status Cache & Pub/Sub
			statusSnapshot := dto.FlashSaleOrderStatusResponse{
				ReservationID: resv.ID,
				Status:        "EXPIRED",
				FailureReason: "Đơn hàng đã hết thời gian giữ chỗ thanh toán",
				ExpiresAt:     resv.ExpiresAt,
				UpdatedAt:     time.Now(),
			}
			if statusJSON, err := json.Marshal(statusSnapshot); err == nil {
				_ = w.redisClient.Set(ctx, redislock.KeyOrderStatus(resv.ID), string(statusJSON), 24*time.Hour)
				_ = w.redisClient.Publish(ctx, fmt.Sprintf("pubsub:order-status:%s", resv.ID), string(statusJSON))
			}
		}

		logger.InfoContext(ctx, "⌛ [EXPIRED] Đã giải phóng giữ chỗ hết hạn thành công",
			"reservation_id", resv.ID,
			"campaign_id", resv.CampaignID,
			"product_id", resv.ProductID,
			"quantity", resv.Quantity,
		)
	}
}
