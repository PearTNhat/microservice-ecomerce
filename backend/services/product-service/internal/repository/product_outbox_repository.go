package repository

import (
	"errors"
	"time"

	"ecomerce-service/services/product-service/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type productOutboxRepository struct {
	db *gorm.DB
}

func NewProductOutboxRepository(db *gorm.DB) domain.ProductOutboxRepository {
	return &productOutboxRepository{db: db}
}

func (r *productOutboxRepository) Create(tx *gorm.DB, event *domain.ProductOutboxEvent) error {
	db := r.db
	if tx != nil {
		db = tx
	}
	return db.Create(event).Error
}

func (r *productOutboxRepository) ClaimPendingBatch(batchSize int, workerID string, leaseDuration time.Duration) ([]*domain.ProductOutboxEvent, error) {
	var events []*domain.ProductOutboxEvent

	err := r.db.Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		var candidateIDs []string

		// 1. Tìm các event PENDING hoặc stale PROCESSING bằng SELECT ... FOR UPDATE SKIP LOCKED
		err := tx.Model(&domain.ProductOutboxEvent{}).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Select("id").
			Where("(status = ? AND next_attempt_at <= ?) OR (status = ? AND locked_until < ?)",
				domain.ProductOutboxStatusPending, now, domain.ProductOutboxStatusProcessing, now).
			Order("next_attempt_at ASC").
			Limit(batchSize).
			Pluck("id", &candidateIDs).Error

		if err != nil || len(candidateIDs) == 0 {
			return err
		}

		// 2. Chuyển sang PROCESSING và gán lease lock
		leaseUntil := now.Add(leaseDuration)
		err = tx.Model(&domain.ProductOutboxEvent{}).
			Where("id IN ?", candidateIDs).
			Updates(map[string]interface{}{
				"status":       domain.ProductOutboxStatusProcessing,
				"locked_by":    workerID,
				"locked_until": leaseUntil,
			}).Error

		if err != nil {
			return err
		}

		// 3. Đọc dữ liệu đầy đủ của các event đã claim
		return tx.Where("id IN ?", candidateIDs).Find(&events).Error
	})

	return events, err
}

func (r *productOutboxRepository) MarkPublished(id string, workerID string) error {
	now := time.Now()
	query := r.db.Model(&domain.ProductOutboxEvent{}).
		Where("id = ?", id).
		Where("status = ?", domain.ProductOutboxStatusProcessing)

	if workerID != "" {
		query = query.Where("locked_by = ?", workerID)
	}

	res := query.Updates(map[string]interface{}{
		"status":       domain.ProductOutboxStatusPublished,
		"published_at": now,
		"locked_by":    nil,
		"locked_until": nil,
	})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("product outbox event not found or lease expired/overwritten by another worker")
	}
	return nil
}

func (r *productOutboxRepository) MarkFailed(id string, workerID string, retryIn time.Duration) error {
	now := time.Now()
	nextAttempt := now.Add(retryIn)

	query := r.db.Model(&domain.ProductOutboxEvent{}).
		Where("id = ?", id).
		Where("status = ?", domain.ProductOutboxStatusProcessing)

	if workerID != "" {
		query = query.Where("locked_by = ?", workerID)
	}

	res := query.Updates(map[string]interface{}{
		"status":          domain.ProductOutboxStatusPending,
		"attempts":        gorm.Expr("attempts + 1"),
		"next_attempt_at": nextAttempt,
		"locked_by":       nil,
		"locked_until":    nil,
	})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("product outbox event not found or lease expired/overwritten by another worker")
	}
	return nil
}

func (r *productOutboxRepository) ReclaimStaleProcessing(now time.Time) error {
	return r.db.Model(&domain.ProductOutboxEvent{}).
		Where("status = ? AND locked_until < ?", domain.ProductOutboxStatusProcessing, now).
		Updates(map[string]interface{}{
			"status":       domain.ProductOutboxStatusPending,
			"locked_by":    nil,
			"locked_until": nil,
		}).Error
}
