package domain

import (
	"time"

	"gorm.io/gorm"
)

// ProcessedEvent ghi nhận các event/request đã xử lý để đảm bảo Idempotent Consumer
type ProcessedEvent struct {
	ConsumerName string    `json:"consumer_name" gorm:"primaryKey;type:varchar(100)"`
	EventID      string    `json:"event_id" gorm:"primaryKey;type:varchar(64)"`
	ProcessedAt  time.Time `json:"processed_at" gorm:"default:current_timestamp"`
}

type ProcessedEventRepository interface {
	InsertIfNew(tx *gorm.DB, consumerName, eventID string) (bool, error)
}
