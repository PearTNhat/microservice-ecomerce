package repository

import (
	"errors"
	"time"

	"ecomerce-service/services/order-service/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type outboxRepository struct {
	db *gorm.DB
}

func NewOutboxRepository(db *gorm.DB) domain.OutboxRepository {
	return &outboxRepository{db: db}
}

func (r *outboxRepository) ClaimPendingBatch(batchSize int, workerID string, leaseDuration time.Duration) ([]*domain.OutboxEvent, error) {
	var events []*domain.OutboxEvent

	err := r.db.Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		var candidateIDs []string

		// 1. Tìm các event PENDING hoặc stale PROCESSING bằng SELECT ... FOR UPDATE SKIP LOCKED
		err := tx.Model(&domain.OutboxEvent{}).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Select("id").
			Where("(status = ? AND next_attempt_at <= ?) OR (status = ? AND locked_until < ?)",
				domain.OutboxStatusPending, now, domain.OutboxStatusProcessing, now).
			Order("next_attempt_at ASC").
			Limit(batchSize).
			Pluck("id", &candidateIDs).Error

		if err != nil || len(candidateIDs) == 0 {
			return err
		}

		// 2. Chuyển sang PROCESSING và gán lease lock
		leaseUntil := now.Add(leaseDuration)
		err = tx.Model(&domain.OutboxEvent{}).
			Where("id IN ?", candidateIDs).
			Updates(map[string]interface{}{
				"status":       domain.OutboxStatusProcessing,
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

func (r *outboxRepository) MarkPublished(id string, workerID string) error {
	now := time.Now()
	query := r.db.Model(&domain.OutboxEvent{}).
		Where("id = ?", id).
		Where("status = ?", domain.OutboxStatusProcessing)

	if workerID != "" {
		query = query.Where("locked_by = ?", workerID)
	}

	res := query.Updates(map[string]interface{}{
		"status":       domain.OutboxStatusPublished,
		"published_at": now,
		"locked_by":    nil,
		"locked_until": nil,
	})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("outbox event not found or lease expired/overwritten by another worker")
	}
	return nil
}

func (r *outboxRepository) MarkFailed(id string, workerID string, retryIn time.Duration) error {
	now := time.Now()
	nextAttempt := now.Add(retryIn)

	query := r.db.Model(&domain.OutboxEvent{}).
		Where("id = ?", id).
		Where("status = ?", domain.OutboxStatusProcessing)

	if workerID != "" {
		query = query.Where("locked_by = ?", workerID)
	}

	res := query.Updates(map[string]interface{}{
		"status":          domain.OutboxStatusPending,
		"attempts":        gorm.Expr("attempts + 1"),
		"next_attempt_at": nextAttempt,
		"locked_by":       nil,
		"locked_until":    nil,
	})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("outbox event not found or lease expired/overwritten by another worker")
	}
	return nil
}

func (r *outboxRepository) ReclaimStaleProcessing(now time.Time) error {
	return r.db.Model(&domain.OutboxEvent{}).
		Where("status = ? AND locked_until < ?", domain.OutboxStatusProcessing, now).
		Updates(map[string]interface{}{
			"status":       domain.OutboxStatusPending,
			"locked_by":    nil,
			"locked_until": nil,
		}).Error
}
