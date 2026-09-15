package repository

import (
	"errors"
	"time"

	"ecomerce-service/services/product-service/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrDuplicateEvent = errors.New("event has already been processed")

type processedEventRepository struct {
	db *gorm.DB
}

func NewProcessedEventRepository(db *gorm.DB) domain.ProcessedEventRepository {
	return &processedEventRepository{db: db}
}

// InsertIfNew chèn vào processed_events với ON CONFLICT DO NOTHING.
// Nếu sự kiện đã tồn tại (RowsAffected == 0), trả về (false, ErrDuplicateEvent).
func (r *processedEventRepository) InsertIfNew(tx *gorm.DB, consumerName, eventID string) (bool, error) {
	if tx == nil {
		tx = r.db
	}

	processed := domain.ProcessedEvent{
		ConsumerName: consumerName,
		EventID:      eventID,
		ProcessedAt:  time.Now(),
	}

	result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&processed)
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected == 0 {
		return false, ErrDuplicateEvent
	}

	return true, nil
}
