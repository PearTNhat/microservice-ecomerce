package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
			return w.fsRepo.ReleaseReservationDB(tx, resv.ID, domain.ReservationStatusExpired)
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
