package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/redislock"
	"ecomerce-service/services/order-service/internal/domain"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

type ReconciliationWorker struct {
	db          *gorm.DB
	fsRepo      domain.FlashSaleRepository
	redisClient *redis.Client
	interval    time.Duration
}

func NewReconciliationWorker(
	db *gorm.DB,
	fsRepo domain.FlashSaleRepository,
	redisClient *redis.Client,
) *ReconciliationWorker {
	return &ReconciliationWorker{
		db:          db,
		fsRepo:      fsRepo,
		redisClient: redisClient,
		interval:    1 * time.Minute,
	}
}

func (w *ReconciliationWorker) Start(ctx context.Context) {
	if w.fsRepo == nil || w.redisClient == nil {
		return
	}

	logger.Info("🛡️ [RECONCILIATION WORKER] Khởi chạy tiến trình đối soát và dọn dẹp Ghost Reservation...")

	go func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Info("🛑 ReconciliationWorker nhận tín hiệu dừng")
				return
			case <-ticker.C:
				w.reconcileActiveCampaigns(ctx)
			}
		}
	}()
}

func (w *ReconciliationWorker) reconcileActiveCampaigns(ctx context.Context) {
	// 1. Tìm các campaign đang ACTIVE
	campaigns, _, err := w.fsRepo.ListCampaigns(string(domain.CampaignStatusActive), 1, 50)
	if err != nil || len(campaigns) == 0 {
		return
	}

	now := time.Now()
	nowUnix := float64(now.Unix())

	for _, camp := range campaigns {
		for _, item := range camp.Items {
			expiryKey := redislock.KeyExpiry(camp.ID, item.ProductID)

			// 2. Tìm các reservation đã quá hạn trong Redis Expiry ZSet
			resvIDs, err := w.redisClient.ZRangeByScore(ctx, expiryKey, &redis.ZRangeBy{
				Min: "-inf",
				Max: fmt.Sprintf("%f", nowUnix-60), // Grace period 60s
			}).Result()

			if err != nil || len(resvIDs) == 0 {
				continue
			}

			for _, resvID := range resvIDs {
				// 3. Kiểm tra sự tồn tại trong PostgreSQL
				resv, dErr := w.fsRepo.FindReservationByID(resvID)
				if dErr != nil {
					if errors.Is(dErr, gorm.ErrRecordNotFound) {
						// PHÁT HIỆN GHOST RESERVATION: Có trên Redis nhưng không hề có trong DB
						logger.WarnContext(ctx, "🚨 [GHOST RESERVATION DETECTED] Thu hồi giữ chỗ mồ côi do App crash trước khi ghi DB",
							"reservation_id", resvID,
							"campaign_id", camp.ID,
							"product_id", item.ProductID,
						)
						_, _ = redislock.ReleaseFlashSaleReservation(ctx, w.redisClient, camp.ID, item.ProductID, resvID, "CANCELLED")
					}
					continue
				}

				// Nếu có trong DB nhưng trạng thái đã là EXPIRED / CANCELLED mà Redis còn sót
				if resv.Status == domain.ReservationStatusExpired || resv.Status == domain.ReservationStatusCancelled {
					_, _ = redislock.ReleaseFlashSaleReservation(ctx, w.redisClient, camp.ID, item.ProductID, resvID, string(resv.Status))
				} else if resv.Status == domain.ReservationStatusConfirmed {
					// Nếu đã CONFIRMED trong DB mà vẫn còn sót trong Expiry ZSet của Redis:
					// Chạy ConfirmFlashSaleReservation để chuyển counter atomic và ZREM.
					confirmResp, cErr := redislock.ConfirmFlashSaleReservation(ctx, w.redisClient, camp.ID, item.ProductID, resvID)
					if cErr == nil && (confirmResp.Code == "ALREADY_CONFIRMED" || confirmResp.Code == "INVALID_STATE") {
						// Hash đã confirmed hoặc không còn tồn tại -> xóa member mồ côi khỏi ZSet
						_ = w.redisClient.ZRem(ctx, expiryKey, resvID).Err()
					}
				}
			}
		}
	}
}
