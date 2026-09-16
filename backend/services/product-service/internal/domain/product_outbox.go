package domain

import (
	"time"

	"gorm.io/gorm"
)

type ProductOutboxStatus string

const (
	ProductOutboxStatusPending    ProductOutboxStatus = "PENDING"
	ProductOutboxStatusProcessing ProductOutboxStatus = "PROCESSING"
	ProductOutboxStatusPublished  ProductOutboxStatus = "PUBLISHED"
	ProductOutboxStatusFailed     ProductOutboxStatus = "FAILED"
)

// ProductOutboxEvent đại diện cho sự kiện outbox bền vững trong product-service (18.5)
type ProductOutboxEvent struct {
	ID            string              `json:"id" gorm:"primaryKey;type:varchar(64)"`
	AggregateType string              `json:"aggregate_type" gorm:"type:varchar(50);not null"`
	AggregateID   string              `json:"aggregate_id" gorm:"type:varchar(64);not null"`
	EventType     string              `json:"event_type" gorm:"type:varchar(80);not null"`
	Topic         string              `json:"topic" gorm:"type:varchar(100);not null"`
	PartitionKey  string              `json:"partition_key" gorm:"type:varchar(100);not null"`
	Payload       string              `json:"payload" gorm:"type:text;not null"`
	Status        ProductOutboxStatus `json:"status" gorm:"type:varchar(20);default:'PENDING';index"`
	Attempts      int                 `json:"attempts" gorm:"default:0"`
	NextAttemptAt time.Time           `json:"next_attempt_at" gorm:"default:current_timestamp;index"`
	LockedBy      string              `json:"locked_by,omitempty" gorm:"type:varchar(100)"`
	LockedUntil   *time.Time          `json:"locked_until,omitempty" gorm:"index"`
	CreatedAt     time.Time           `json:"created_at" gorm:"default:current_timestamp"`
	PublishedAt   *time.Time          `json:"published_at,omitempty"`
}

func (ProductOutboxEvent) TableName() string {
	return "product_outbox_events"
}

type ProductOutboxRepository interface {
	Create(tx *gorm.DB, event *ProductOutboxEvent) error
	ClaimPendingBatch(batchSize int, workerID string, leaseDuration time.Duration) ([]*ProductOutboxEvent, error)
	MarkPublished(id string, workerID string) error
	MarkFailed(id string, workerID string, retryIn time.Duration) error
	ReclaimStaleProcessing(now time.Time) error
}
