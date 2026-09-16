package domain

import (
	"time"

	"gorm.io/gorm"
)

const (
	MixedStockOpPending               = "PENDING"
	MixedStockOpDeducted              = "DEDUCTED"
	MixedStockOpFailed                = "FAILED"
	MixedStockOpCancelledBeforeDeduct = "CANCELLED_BEFORE_DEDUCT"
	MixedStockOpCompensated           = "COMPENSATED"
)

// MixedOrderStockOperation ghi nhận trạng thái xử lý trừ/hoàn kho cho đơn hàng hỗn hợp
type MixedOrderStockOperation struct {
	ID            uint           `gorm:"primaryKey;autoIncrement" json:"id"`
	OrderID       uint           `gorm:"uniqueIndex:uq_mixed_stock_order_id;not null" json:"order_id"`
	OrderCode     string         `gorm:"type:varchar(64);not null" json:"order_code"`
	Status        string         `gorm:"type:varchar(32);not null;index" json:"status"`
	DeductedItems string         `gorm:"type:text" json:"deducted_items"` // JSON lưu danh sách món và số lượng đã thực tế trừ
	ResultPayload string         `gorm:"type:text" json:"result_payload"` // JSON lưu kết quả MixedOrderStockResultPayload
	Reason        string         `gorm:"type:text" json:"reason"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
	DeletedAt     gorm.DeletedAt `gorm:"index" json:"-"`
}

func (MixedOrderStockOperation) TableName() string {
	return "mixed_order_stock_operations"
}

type MixedOrderStockOperationRepository interface {
	GetByOrderID(tx *gorm.DB, orderID uint) (*MixedOrderStockOperation, error)
	GetByOrderIDWithLock(tx *gorm.DB, orderID uint) (*MixedOrderStockOperation, error)
	Create(tx *gorm.DB, op *MixedOrderStockOperation) error
	Update(tx *gorm.DB, op *MixedOrderStockOperation) error
}
