package repository

import (
	"errors"
	"strings"
	"time"

	"ecomerce-service/services/order-service/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type processedEventRepository struct {
	db *gorm.DB
}

func NewProcessedEventRepository(db *gorm.DB) domain.ProcessedEventRepository {
	return &processedEventRepository{db: db}
}

func (r *processedEventRepository) HasProcessed(consumerName string, eventID string) (bool, error) {
	var count int64
	err := r.db.Model(&domain.ProcessedEvent{}).
		Where("consumer_name = ? AND event_id = ?", consumerName, eventID).
		Count(&count).Error

	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *processedEventRepository) MarkProcessed(tx *gorm.DB, consumerName string, eventID string) error {
	db := r.db
	if tx != nil {
		db = tx
	}

	event := domain.ProcessedEvent{
		ConsumerName: consumerName,
		EventID:      eventID,
		ProcessedAt:  time.Now(),
	}

	err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&event).Error
	if err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) || strings.Contains(err.Error(), "UNIQUE") || strings.Contains(err.Error(), "duplicate key") {
			return nil
		}
		return err
	}
	return nil
}
