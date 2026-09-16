package dto

import "time"

type CreateCampaignRequest struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	StartsAt    time.Time `json:"starts_at"`
	EndsAt      time.Time `json:"ends_at"`
}

type AddFlashSaleItemRequest struct {
	ProductID           uint    `json:"product_id"`
	SalePrice           float64 `json:"sale_price"`
	OriginalPrice       float64 `json:"original_price"`
	AllocatedStock      int     `json:"allocated_stock"`
	MaxQuantityPerUser  int     `json:"max_quantity_per_user"`
	MaxQuantityPerOrder int     `json:"max_quantity_per_order"`
	ReservationSeconds  int     `json:"reservation_seconds"`
}

type FlashSaleItemResponse struct {
	ID                  uint      `json:"id"`
	CampaignID          uint      `json:"campaign_id"`
	ProductID           uint      `json:"product_id"`
	SalePrice           float64   `json:"sale_price"`
	OriginalPrice       float64   `json:"original_price"`
	AllocatedStock      int       `json:"allocated_stock"`
	ReservedStock       int       `json:"reserved_stock"`
	SoldStock           int       `json:"sold_stock"`
	MaxQuantityPerUser  int       `json:"max_quantity_per_user"`
	MaxQuantityPerOrder int       `json:"max_quantity_per_order"`
	ReservationSeconds  int       `json:"reservation_seconds"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type FlashSaleCampaignResponse struct {
	ID          uint                     `json:"id"`
	Name        string                   `json:"name"`
	Description string                   `json:"description"`
	StartsAt    time.Time                `json:"starts_at"`
	EndsAt      time.Time                `json:"ends_at"`
	Status      string                   `json:"status"`
	Items       []*FlashSaleItemResponse `json:"items,omitempty"`
	CreatedAt   time.Time                `json:"created_at"`
	UpdatedAt   time.Time                `json:"updated_at"`
}

type FlashSaleCustomerOrderRequest struct {
	Quantity        int    `json:"quantity"`
	PaymentMethod   string `json:"payment_method"`
	CustomerName    string `json:"customer_name"`
	CustomerEmail   string `json:"customer_email"`
	CustomerPhone   string `json:"customer_phone"`
	ShippingAddress string `json:"shipping_address"`
}

type FlashSaleOrderResponse struct {
	ReservationID string    `json:"reservation_id"`
	Status        string    `json:"status"`
	ExpiresAt     time.Time `json:"expires_at"`
	StatusURL     string    `json:"status_url"`
	StreamURL     string    `json:"stream_url"`
}

type FlashSaleOrderStatusResponse struct {
	ReservationID string     `json:"reservation_id"`
	Status        string     `json:"status"`
	OrderID       *uint      `json:"order_id,omitempty"`
	OrderCode     string     `json:"order_code,omitempty"`
	ExpiresAt     time.Time  `json:"expires_at"`
	FailureReason string     `json:"failure_reason,omitempty"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

type ActiveCampaignItemDTO struct {
	ID                  uint    `json:"id"`
	CampaignID          uint    `json:"campaign_id"`
	ProductID           uint    `json:"product_id"`
	ProductName         string  `json:"product_name"`
	ProductThumbnail    string  `json:"product_thumbnail"`
	SalePrice           float64 `json:"sale_price"`
	OriginalPrice       float64 `json:"original_price"`
	DiscountPercentage  int     `json:"discount_percentage"`
	AllocatedStock      int     `json:"allocated_stock"`
	ReservedStock       int     `json:"reserved_stock"`
	SoldStock           int     `json:"sold_stock"`
	RemainingStock      int     `json:"remaining_stock"`
	MaxQuantityPerUser  int     `json:"max_quantity_per_user"`
	MaxQuantityPerOrder int     `json:"max_quantity_per_order"`
	ReservationSeconds  int     `json:"reservation_seconds"`
}

type ActiveCampaignResponse struct {
	ID               uint                     `json:"id"`
	Name             string                   `json:"name"`
	Description      string                   `json:"description"`
	StartsAt         time.Time                `json:"starts_at"`
	EndsAt           time.Time                `json:"ends_at"`
	Status           string                   `json:"status"`
	RemainingSeconds int64                    `json:"remaining_seconds"`
	Items            []*ActiveCampaignItemDTO `json:"items"`
}

type StockAllocationResponse struct {
	ID                uint   `json:"id"`
	CampaignID        uint   `json:"campaign_id"`
	ProductID         uint   `json:"product_id"`
	RequestID         string `json:"request_id"`
	AllocatedQuantity int    `json:"allocated_quantity"`
	SoldQuantity      int    `json:"sold_quantity"`
	ReleasedQuantity  int    `json:"released_quantity"`
	Status            string `json:"status"`
}

type ProductOfferResponse struct {
	ProductID          uint       `json:"product_id"`
	PurchaseMode       string     `json:"purchase_mode"` // "FLASH_SALE" or "REGULAR"
	HasFlashSale       bool       `json:"has_flash_sale"`
	EffectivePrice     float64    `json:"effective_price"`
	RegularPrice       float64    `json:"regular_price"`
	OriginalPrice      float64    `json:"original_price"`
	CampaignID         *uint      `json:"campaign_id,omitempty"`
	CampaignName       string     `json:"campaign_name,omitempty"`
	SalePrice          *float64   `json:"sale_price,omitempty"`
	DiscountPercent    int        `json:"discount_percentage,omitempty"`
	RemainingStock     int        `json:"remaining_stock,omitempty"`
	RemainingDisplay   int        `json:"remaining_display,omitempty"`
	MaxQuantityPerUser int        `json:"max_quantity_per_user,omitempty"`
	EndsAt             *time.Time `json:"ends_at,omitempty"`
}

type BatchOfferRequest struct {
	ProductIDs []uint `json:"product_ids"`
}

type BatchOfferResponse struct {
	Offers map[uint]*ProductOfferResponse `json:"offers"`
}

