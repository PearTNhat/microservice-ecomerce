package domain

import "time"

// Trạng thái đơn hàng
const (
	OrderStatusPending    = "PENDING"
	OrderStatusConfirmed  = "CONFIRMED"
	OrderStatusProcessing = "PROCESSING"
	OrderStatusShipped    = "SHIPPED"
	OrderStatusDelivered  = "DELIVERED"
	OrderStatusCancelled  = "CANCELLED"
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
	ID              uint        `json:"id" gorm:"primaryKey"`
	OrderCode       string      `json:"order_code" gorm:"uniqueIndex;not null"`
	UserID          string      `json:"user_id" gorm:"not null;index"`
	CustomerName    string      `json:"customer_name" gorm:"not null"`
	CustomerEmail   string      `json:"customer_email" gorm:"not null"`
	CustomerPhone   string      `json:"customer_phone" gorm:"not null"`
	ShippingAddress string      `json:"shipping_address" gorm:"not null;type:text"`
	Note            string      `json:"note,omitempty" gorm:"type:text"`
	PaymentMethod   string      `json:"payment_method" gorm:"not null;default:'COD'"`
	PaymentStatus   string      `json:"payment_status" gorm:"not null;default:'PENDING'"`
	OrderStatus     string      `json:"order_status" gorm:"not null;default:'PENDING'"`
	TotalAmount     float64     `json:"total_amount" gorm:"not null"`
	Items           []OrderItem `json:"items" gorm:"foreignKey:OrderID;constraint:OnDelete:CASCADE"`
	CreatedAt       time.Time   `json:"created_at" gorm:"default:current_timestamp"`
	UpdatedAt       time.Time   `json:"updated_at" gorm:"default:current_timestamp"`
}

// OrderItem đại diện cho một sản phẩm trong đơn hàng
type OrderItem struct {
	ID          uint      `json:"id" gorm:"primaryKey"`
	OrderID     uint      `json:"order_id" gorm:"not null;index"`
	ProductID   uint      `json:"product_id" gorm:"not null;index"`
	ProductName string    `json:"product_name" gorm:"not null"`
	ProductSlug string    `json:"product_slug,omitempty"`
	Thumbnail   string    `json:"thumbnail,omitempty"`
	Price       float64   `json:"price" gorm:"not null"`
	Quantity    int       `json:"quantity" gorm:"not null"`
	Subtotal    float64   `json:"subtotal" gorm:"not null"`
	CreatedAt   time.Time `json:"created_at" gorm:"default:current_timestamp"`
	UpdatedAt   time.Time `json:"updated_at" gorm:"default:current_timestamp"`
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
	var total float64
	for _, item := range o.Items {
		total += item.Price * float64(item.Quantity)
	}
	return total
}

// OrderRepository định nghĩa interface thao tác dữ liệu đơn hàng
type OrderRepository interface {
	CreateOrder(order *Order) error
	FindByID(id uint) (*Order, error)
	FindByOrderCode(orderCode string) (*Order, error)
	FindByUserID(userID string, page int, limit int) ([]*Order, int64, error)
	UpdateStatus(orderID uint, status string) error
	UpdatePaymentStatus(orderID uint, status string) error
}
