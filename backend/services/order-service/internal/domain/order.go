package domain

import (
	"time"

	"gorm.io/gorm"
)

// Trạng thái đơn hàng
const (
	OrderStatusPending      = "PENDING"
	OrderStatusConfirmed    = "CONFIRMED"
	OrderStatusProcessing   = "PROCESSING"
	OrderStatusShipped      = "SHIPPED"
	OrderStatusDelivered    = "DELIVERED"
	OrderStatusCancelled    = "CANCELLED"
	OrderStatusCompensating = "COMPENSATING" // 17.2: Trạng thái trung gian chờ Product Service bồi hoàn tồn kho thường
)

// Roles
const (
	RoleAdmin      = "ADMIN"
	RoleTechnician = "TECHNICIAN"
	RoleCustomer   = "CUSTOMER"
)

// Phương thức thanh toán
const (
	PaymentMethodCOD          = "COD"
	PaymentMethodVNPAY        = "VNPAY"
	PaymentMethodMOMO         = "MOMO"
	PaymentMethodBankTransfer = "BANK_TRANSFER"
)

// Trạng thái thanh toán
const (
	PaymentStatusPending  = "PENDING"
	PaymentStatusPaid     = "PAID"
	PaymentStatusFailed   = "FAILED"
	PaymentStatusRefunded = "REFUNDED"
)

// Order đại diện cho đơn hàng thương mại điện tử
type Order struct {
	ID                uint        `json:"id" gorm:"primaryKey"`
	OrderCode         string      `json:"order_code" gorm:"uniqueIndex;not null"`
	UserID            string      `json:"user_id" gorm:"not null;index"`
	CustomerName      string      `json:"customer_name" gorm:"not null"`
	CustomerEmail     string      `json:"customer_email" gorm:"not null"`
	CustomerPhone     string      `json:"customer_phone" gorm:"not null"`
	ShippingAddress   string      `json:"shipping_address" gorm:"not null;type:text"`
	Note              string      `json:"note,omitempty" gorm:"type:text"`
	PaymentMethod     string      `json:"payment_method" gorm:"not null;default:'COD'"`
	PaymentStatus     string      `json:"payment_status" gorm:"not null;default:'PENDING'"`
	OrderStatus       string      `json:"order_status" gorm:"not null;default:'PENDING'"`
	TotalAmount       float64     `json:"total_amount" gorm:"not null"`
	CheckoutAttemptID *uint       `json:"checkout_attempt_id,omitempty" gorm:"uniqueIndex"`
	Items             []OrderItem `json:"items" gorm:"foreignKey:OrderID;constraint:OnDelete:CASCADE"`
	CreatedAt         time.Time   `json:"created_at" gorm:"default:current_timestamp"`
	UpdatedAt         time.Time   `json:"updated_at" gorm:"default:current_timestamp"`
}

// OrderItem đại diện cho một sản phẩm trong đơn hàng
type OrderItem struct {
	ID            uint      `json:"id" gorm:"primaryKey"`
	OrderID       uint      `json:"order_id" gorm:"not null;index"`
	ProductID     uint      `json:"product_id" gorm:"not null"`
	ProductName   string    `json:"product_name" gorm:"not null"`
	ProductSlug   string    `json:"product_slug,omitempty"`
	Thumbnail     string    `json:"thumbnail,omitempty"`
	Price         float64   `json:"price" gorm:"not null"`
	Quantity      int       `json:"quantity" gorm:"not null"`
	Subtotal      float64   `json:"subtotal" gorm:"not null"`
	IsFlashSale   bool      `json:"is_flash_sale" gorm:"default:false"`
	CampaignID    *uint     `json:"campaign_id,omitempty" gorm:"index"`
	ReservationID string    `json:"reservation_id,omitempty" gorm:"size:100;index"`
	CreatedAt     time.Time `json:"created_at" gorm:"default:current_timestamp"`
}


// CanCancel kiểm tra xem đơn hàng có đủ điều kiện hủy không
func (o *Order) CanCancel() bool {
	return o.OrderStatus == OrderStatusPending || o.OrderStatus == OrderStatusConfirmed
}

// IsPaid kiểm tra đơn hàng đã được thanh toán chưa
func (o *Order) IsPaid() bool {
	return o.PaymentStatus == PaymentStatusPaid
}

// CalculateTotal tính tổng tiền từ danh sách items
func (o *Order) CalculateTotal() float64 {
	total := 0.0
	for _, item := range o.Items {
		total += item.Price * float64(item.Quantity)
	}
	return total
}

// Trạng thái của CheckoutAttempt theo State Machine phân tán (R1 + R2)
const (
	CheckoutAttemptStatusPending    = "PENDING"
	CheckoutAttemptStatusRecovering = "RECOVERING"
	CheckoutAttemptStatusRetryable  = "RETRYABLE"
	CheckoutAttemptStatusRejected   = "REJECTED"
	CheckoutAttemptStatusCompleted  = "COMPLETED"
	CheckoutAttemptStatusFailed     = "FAILED" // Legacy status để tương thích ngược và migration
)

// CheckoutAttempt lưu vết và đảm bảo idempotency bền vững có Fencing Token ở tầng Database (R1, R2, R4, R5)
type CheckoutAttempt struct {
	ID                 uint       `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID             string     `json:"user_id" gorm:"size:100;not null;index:idx_user_checkout_key,unique"`
	IdempotencyKey     string     `json:"idempotency_key" gorm:"size:100;not null;index:idx_user_checkout_key,unique"`
	RequestFingerprint string     `json:"request_fingerprint" gorm:"type:text;not null"`
	FingerprintVersion int        `json:"fingerprint_version" gorm:"default:1"`
	Status             string     `json:"status" gorm:"size:30;not null;default:'PENDING'"`
	OwnerToken         string     `json:"owner_token,omitempty" gorm:"size:100"`
	Version            uint64     `json:"version" gorm:"default:1"`
	LeaseExpiresAt     *time.Time `json:"lease_expires_at,omitempty"`
	OrderID            *uint      `json:"order_id,omitempty"`
	OrderCode          string     `json:"order_code,omitempty" gorm:"size:50"`
	ResponsePayload    string     `json:"response_payload,omitempty" gorm:"type:text"`
	RecoveryTarget     string     `json:"recovery_target,omitempty" gorm:"size:50"`
	ManifestJSON       string     `json:"manifest_json,omitempty" gorm:"type:text"`
	CreatedAt          time.Time  `json:"created_at" gorm:"default:current_timestamp"`
	UpdatedAt          time.Time  `json:"updated_at" gorm:"default:current_timestamp"`
}

// OrderRepository định nghĩa interface thao tác dữ liệu đơn hàng
type OrderRepository interface {
	CreateOrder(order *Order) error
	FindByID(id uint) (*Order, error)
	FindByOrderCode(orderCode string) (*Order, error)
	FindByUserID(userID string, page int, limit int) ([]*Order, int64, error)
	UpdateStatus(orderID uint, status string) error
	UpdatePaymentStatus(orderID uint, status string) error
	FindOrderByCheckoutAttemptID(attemptID uint) (*Order, error)

	// Thao tác CheckoutAttempt & Fencing CAS
	GetCheckoutAttempt(userID, key string) (*CheckoutAttempt, error)
	GetCheckoutAttemptByID(id uint) (*CheckoutAttempt, error)
	CreateCheckoutAttempt(attempt *CheckoutAttempt) error
	SaveCheckoutAttempt(attempt *CheckoutAttempt) error
	CASClaimPending(attempt *CheckoutAttempt) (bool, error)
	CASRecoveringTakeover(id uint, expectedVersion uint64, newOwner string, newLease time.Time, recoveryTarget string) (*CheckoutAttempt, bool, error)
	CASRenewLease(id uint, expectedVersion uint64, ownerToken string, newLease time.Time) (bool, error)
	TransitionAttemptStatus(id uint, expectedVersion uint64, newStatus string, recoveryTarget string) (bool, error)
	CompleteAttemptInTx(tx *gorm.DB, attemptID uint, expectedVersion uint64, orderID uint, orderCode string, responsePayload string) error
}
