package domain

import (
	"time"

	"gorm.io/gorm"
)

type CampaignStatus string

const (
	CampaignStatusDraft            CampaignStatus = "DRAFT"
	CampaignStatusAllocating       CampaignStatus = "ALLOCATING"
	CampaignStatusPrewarming       CampaignStatus = "PREWARMING"
	CampaignStatusActive           CampaignStatus = "ACTIVE"
	CampaignStatusEnding           CampaignStatus = "ENDING"
	CampaignStatusEnded            CampaignStatus = "ENDED"
	CampaignStatusActivationFailed CampaignStatus = "ACTIVATION_FAILED"
	CampaignStatusCompensating     CampaignStatus = "COMPENSATING"
	CampaignStatusCancelled        CampaignStatus = "CANCELLED"
)

type FlashSaleCampaign struct {
	ID          uint           `json:"id" gorm:"primaryKey"`
	Name        string         `json:"name" gorm:"not null"`
	Description string         `json:"description"`
	StartsAt    time.Time      `json:"starts_at" gorm:"not null;index"`
	EndsAt      time.Time      `json:"ends_at" gorm:"not null;index"`
	Status      CampaignStatus `json:"status" gorm:"type:varchar(30);default:'DRAFT';index"`
	Version     int64          `json:"version" gorm:"default:0"`
	Items       []FlashSaleItem `json:"items,omitempty" gorm:"foreignKey:CampaignID"`
	CreatedAt   time.Time      `json:"created_at" gorm:"default:current_timestamp"`
	UpdatedAt   time.Time      `json:"updated_at" gorm:"default:current_timestamp"`
}

type FlashSaleItem struct {
	ID                  uint      `json:"id" gorm:"primaryKey"`
	CampaignID          uint      `json:"campaign_id" gorm:"not null;uniqueIndex:uq_fs_campaign_product,priority:1"`
	ProductID           uint      `json:"product_id" gorm:"not null;uniqueIndex:uq_fs_campaign_product,priority:2"`
	SalePrice           float64   `json:"sale_price" gorm:"type:numeric(15,2);not null"`
	OriginalPrice       float64   `json:"original_price" gorm:"type:numeric(15,2);not null"`
	AllocatedStock      int       `json:"allocated_stock" gorm:"not null"`
	ReservedStock       int       `json:"reserved_stock" gorm:"default:0"`
	SoldStock           int       `json:"sold_stock" gorm:"default:0"`
	MaxQuantityPerUser  int       `json:"max_quantity_per_user" gorm:"default:1"`  // 1: Mua 1 lần; >1: Mua tích lũy; 0: Unlimited
	MaxQuantityPerOrder int       `json:"max_quantity_per_order" gorm:"default:1"` // Chống gom hàng 1 lần
	ReservationSeconds  int       `json:"reservation_seconds" gorm:"default:120"`  // Thời gian giữ chỗ
	Version             int64     `json:"version" gorm:"default:0"`
	CreatedAt           time.Time `json:"created_at" gorm:"default:current_timestamp"`
	UpdatedAt           time.Time `json:"updated_at" gorm:"default:current_timestamp"`
}

type ReservationStatus string

const (
	ReservationStatusReserved   ReservationStatus = "RESERVED"
	ReservationStatusProcessing ReservationStatus = "PROCESSING"
	ReservationStatusConfirmed  ReservationStatus = "CONFIRMED"
	ReservationStatusCancelled  ReservationStatus = "CANCELLED"
	ReservationStatusExpired    ReservationStatus = "EXPIRED"
)

type FlashSaleReservation struct {
	ID                 string            `json:"id" gorm:"primaryKey;type:varchar(64)"` // order_token / reservation_id
	RequestID          string            `json:"request_id" gorm:"type:varchar(64);not null"`
	RequestFingerprint string            `json:"request_fingerprint" gorm:"type:varchar(64);not null"`
	CampaignID         uint              `json:"campaign_id" gorm:"not null;uniqueIndex:uq_fs_idempotency,priority:1;index:idx_fs_resv_user,priority:1"`
	FlashSaleItemID    uint              `json:"flash_sale_item_id" gorm:"not null"`
	ProductID          uint              `json:"product_id" gorm:"not null;uniqueIndex:uq_fs_idempotency,priority:2;index:idx_fs_resv_user,priority:2"`
	UserID             string            `json:"user_id" gorm:"type:varchar(64);not null;uniqueIndex:uq_fs_idempotency,priority:3;index:idx_fs_resv_user,priority:3"`
	Quantity           int               `json:"quantity" gorm:"not null"`
	UnitPrice          float64           `json:"unit_price" gorm:"type:numeric(15,2);not null"`
	TotalAmount        float64           `json:"total_amount" gorm:"type:numeric(15,2);not null"`
	PaymentMethod      string            `json:"payment_method" gorm:"type:varchar(20);not null"`
	Status             ReservationStatus `json:"status" gorm:"type:varchar(30);default:'RESERVED';index:idx_fs_resv_expiry,priority:1"`
	OrderID            *uint             `json:"order_id,omitempty" gorm:"index"`
	ExpiresAt          time.Time         `json:"expires_at" gorm:"not null;index:idx_fs_resv_expiry,priority:2"`
	FailureReason      string            `json:"failure_reason,omitempty" gorm:"type:varchar(255)"`
	Version            int64             `json:"version" gorm:"default:0"`
	CreatedAt          time.Time         `json:"created_at" gorm:"default:current_timestamp"`
	UpdatedAt          time.Time         `json:"updated_at" gorm:"default:current_timestamp"`
}

type OutboxStatus string

const (
	OutboxStatusPending    OutboxStatus = "PENDING"
	OutboxStatusProcessing OutboxStatus = "PROCESSING"
	OutboxStatusPublished  OutboxStatus = "PUBLISHED"
	OutboxStatusFailed     OutboxStatus = "FAILED"
)

type OutboxEvent struct {
	ID            string       `json:"id" gorm:"primaryKey;type:varchar(64)"`
	AggregateType string       `json:"aggregate_type" gorm:"type:varchar(50);not null"`
	AggregateID   string       `json:"aggregate_id" gorm:"type:varchar(64);not null"`
	EventType     string       `json:"event_type" gorm:"type:varchar(80);not null"`
	Topic         string       `json:"topic" gorm:"type:varchar(100);not null"`
	PartitionKey  string       `json:"partition_key" gorm:"type:varchar(100);not null"`
	Payload       string       `json:"payload" gorm:"type:text;not null"`
	Status        OutboxStatus `json:"status" gorm:"type:varchar(20);default:'PENDING';index"`
	Attempts      int          `json:"attempts" gorm:"default:0"`
	NextAttemptAt time.Time    `json:"next_attempt_at" gorm:"default:current_timestamp;index"`
	LockedBy      string       `json:"locked_by,omitempty" gorm:"type:varchar(100)"`
	LockedUntil   *time.Time   `json:"locked_until,omitempty" gorm:"index"`
	CreatedAt     time.Time    `json:"created_at" gorm:"default:current_timestamp"`
	PublishedAt   *time.Time   `json:"published_at,omitempty"`
}

type ProcessedEvent struct {
	ConsumerName string    `json:"consumer_name" gorm:"primaryKey;type:varchar(100)"`
	EventID      string    `json:"event_id" gorm:"primaryKey;type:varchar(64)"`
	ProcessedAt  time.Time `json:"processed_at" gorm:"default:current_timestamp"`
}

// FlashSaleRepository định nghĩa interface thao tác dữ liệu Flash Sale
type FlashSaleRepository interface {
	CreateCampaign(campaign *FlashSaleCampaign) error
	GetCampaignByID(id uint) (*FlashSaleCampaign, error)
	UpdateCampaignStatus(id uint, fromStatus CampaignStatus, toStatus CampaignStatus) error
	ListCampaigns(status string, page int, limit int) ([]*FlashSaleCampaign, int64, error)
	AddItem(item *FlashSaleItem) error
	GetItem(campaignID uint, productID uint) (*FlashSaleItem, error)
	GetItemByID(id uint) (*FlashSaleItem, error)
	GetActiveCampaign() (*FlashSaleCampaign, error)

	CreateReservationWithOutbox(reservation *FlashSaleReservation, outbox *OutboxEvent) error
	FindReservationByIdempotency(campaignID uint, productID uint, userID string, requestID string) (*FlashSaleReservation, error)
	FindReservationByID(id string) (*FlashSaleReservation, error)
	UpdateReservationStatusCAS(id string, fromStatus ReservationStatus, toStatus ReservationStatus, orderID *uint) error
	ClaimExpiredReservations(batchSize int) ([]*FlashSaleReservation, error)
	ConfirmReservationDB(tx *gorm.DB, reservationID string, orderID uint) error
	ReleaseReservationDB(tx *gorm.DB, reservationID string, newStatus ReservationStatus) error
}

type OutboxRepository interface {
	ClaimPendingBatch(batchSize int, workerID string, leaseDuration time.Duration) ([]*OutboxEvent, error)
	MarkPublished(id string) error
	MarkFailed(id string, retryIn time.Duration) error
	ReclaimStaleProcessing(now time.Time) error
}

type ProcessedEventRepository interface {
	HasProcessed(consumerName string, eventID string) (bool, error)
	MarkProcessed(tx *gorm.DB, consumerName string, eventID string) error
}
