package domain

import (
	"time"

	"gorm.io/gorm"
)

// ProcessedEvent ghi nhận các event/request đã xử lý để đảm bảo Idempotent Consumer
type ProcessedEvent struct {
	ConsumerName  string    `json:"consumer_name" gorm:"primaryKey;type:varchar(100)"`
	EventID       string    `json:"event_id" gorm:"primaryKey;type:varchar(64)"`
	Status        string    `json:"status" gorm:"type:varchar(20);default:'SUCCESS'"`
	ResultPayload string    `json:"result_payload" gorm:"type:text"`
	ProcessedAt   time.Time `json:"processed_at" gorm:"default:current_timestamp"`
}

type ProcessedEventRepository interface {
	InsertIfNew(tx *gorm.DB, consumerName, eventID string) (bool, error)
	GetEvent(tx *gorm.DB, consumerName, eventID string) (*ProcessedEvent, error)
	SaveEvent(tx *gorm.DB, consumerName, eventID, status, resultPayload string) error
}
