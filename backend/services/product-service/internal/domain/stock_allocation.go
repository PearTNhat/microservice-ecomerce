package domain

import (
	"time"

	"gorm.io/gorm"
)

const (
	StockAllocationStatusAllocated = "ALLOCATED"
	StockAllocationStatusReleased  = "RELEASED"
)

// ProductStockAllocation ghi nhận sổ cái phân bổ tồn kho của từng Campaign
type ProductStockAllocation struct {
	ID                uint      `json:"id" gorm:"primaryKey"`
	CampaignID        uint      `json:"campaign_id" gorm:"not null;uniqueIndex:uq_product_campaign_allocation,priority:1"`
	ProductID         uint      `json:"product_id" gorm:"not null;uniqueIndex:uq_product_campaign_allocation,priority:2;index"`
	RequestID         string    `json:"request_id" gorm:"type:varchar(64);uniqueIndex;not null"`
	AllocatedQuantity int       `json:"allocated_quantity" gorm:"not null"`
	SoldQuantity      int       `json:"sold_quantity" gorm:"default:0"`
	ReleasedQuantity  int       `json:"released_quantity" gorm:"default:0"`
	Status            string    `json:"status" gorm:"type:varchar(20);not null"`
	CreatedAt         time.Time `json:"created_at" gorm:"default:current_timestamp"`
	UpdatedAt         time.Time `json:"updated_at" gorm:"default:current_timestamp"`
}

type StockAllocationRepository interface {
	AllocateStock(campaignID uint, productID uint, requestID string, quantity int) (*ProductStockAllocation, error)
	ReleaseStock(campaignID uint, productID uint, requestID string) (int, error)
	IncrementSoldQuantity(campaignID uint, productID uint, quantity int) error
	IncrementSoldQuantityTx(tx *gorm.DB, campaignID uint, productID uint, quantity int) error
	FindByCampaignAndProduct(campaignID uint, productID uint) (*ProductStockAllocation, error)
}
