package dto

// CreateOrderItemRequest món hàng khi tạo đơn trực tiếp
type CreateOrderItemRequest struct {
	ProductID uint `json:"product_id" validate:"required,gt=0"`
	Quantity  int  `json:"quantity" validate:"required,gt=0"`
}

// CreateOrderRequest yêu cầu đặt hàng
type CreateOrderRequest struct {
	CustomerName    string                   `json:"customer_name" validate:"required"`
	CustomerEmail   string                   `json:"customer_email" validate:"required,email"`
	CustomerPhone   string                   `json:"customer_phone" validate:"required"`
	ShippingAddress string                   `json:"shipping_address" validate:"required"`
	Note            string                   `json:"note,omitempty"`
	PaymentMethod   string                   `json:"payment_method" validate:"required,oneof=COD VNPAY MOMO BANK_TRANSFER"`
	FromCart        bool                     `json:"from_cart"`
	QuoteToken      string                   `json:"quote_token,omitempty"`
	IdempotencyKey  string                   `json:"idempotency_key,omitempty"`
	Items           []CreateOrderItemRequest `json:"items,omitempty"`
}

// Mã lỗi HTTP Contract cho Checkout và Idempotency (R3 + R5)
const (
	ErrCodeIdempotencyKeyRequired = "IDEMPOTENCY_KEY_REQUIRED"
	ErrCodeIdempotencyKeyMismatch = "IDEMPOTENCY_KEY_MISMATCH"
	ErrCodeOrderProcessing        = "ORDER_PROCESSING"
	ErrCodeIdempotencyConflict    = "IDEMPOTENCY_CONFLICT"
	ErrCodeQuoteChanged           = "QUOTE_CHANGED"
	ErrCodeQuoteExpired           = "QUOTE_EXPIRED"
	ErrCodeCheckoutRetryable      = "CHECKOUT_RETRYABLE"
	ErrCodeCheckoutOutcomeUnknown = "CHECKOUT_OUTCOME_UNKNOWN"
	ErrCodeFlashSaleUnavailable   = "FLASH_SALE_SERVICE_UNAVAILABLE"
)

// PriceConflictItem mô tả món hàng bị thay đổi giá hoặc hết suất Flash Sale
type PriceConflictItem struct {
	ProductID    uint    `json:"product_id"`
	ProductName  string  `json:"product_name"`
	WasFlashSale bool    `json:"was_flash_sale"`
	QuotedPrice  float64 `json:"quoted_price"`
	UpdatedPrice float64 `json:"updated_price"`
	Reason       string  `json:"reason"` // "FLASH_SALE_EXPIRED", "FLASH_SALE_OUT_OF_STOCK", "PRICE_CHANGED"
}

// PriceConflictResponse dữ liệu trả về khi gặp HTTP 409 Conflict kèm token báo giá mới
type PriceConflictResponse struct {
	ErrorCode     string              `json:"error_code"`
	Message       string              `json:"message"`
	NewQuoteToken string              `json:"new_quote_token"`
	NewTotal      float64             `json:"new_total"`
	AffectedItems []PriceConflictItem `json:"affected_items"`
	NewItems      []QuoteLineDTO      `json:"new_items,omitempty"`
}

// BasketQuoteItem yêu cầu báo giá cho 1 món hàng
type BasketQuoteItem struct {
	ProductID uint `json:"product_id" validate:"required,gt=0"`
	Quantity  int  `json:"quantity" validate:"required,gt=0"`
}

// BasketQuoteRequest yêu cầu báo giá toàn bộ giỏ hàng trước khi checkout (17.1)
type BasketQuoteRequest struct {
	Items    []BasketQuoteItem `json:"items,omitempty"`
	FromCart bool              `json:"from_cart"`
}

// QuoteLineDTO chi tiết từng dòng trong báo giá giỏ hàng
type QuoteLineDTO struct {
	ProductID    uint    `json:"product_id"`
	ProductName  string  `json:"product_name"`
	Quantity     int     `json:"quantity"`
	UnitPrice    float64 `json:"unit_price"`
	Subtotal     float64 `json:"subtotal"`
	IsFlashSale  bool    `json:"is_flash_sale"`
	CampaignID   *uint   `json:"campaign_id,omitempty"`
	PurchaseMode string  `json:"purchase_mode"` // "FLASH_SALE" hoặc "REGULAR"
}

// BasketQuoteResponse kết quả báo giá toàn bộ giỏ hàng
type BasketQuoteResponse struct {
	QuoteToken string         `json:"quote_token"`
	Total      float64        `json:"total"`
	ExpiresAt  int64          `json:"expires_at"`
	Items      []QuoteLineDTO `json:"items"`
}

// OrderItemResponse trả về thông tin món hàng trong đơn
type OrderItemResponse struct {
	ID          uint    `json:"id"`
	ProductID   uint    `json:"product_id"`
	ProductName string  `json:"product_name"`
	ProductSlug string  `json:"product_slug,omitempty"`
	Thumbnail   string  `json:"thumbnail,omitempty"`
	Price       float64 `json:"price"`
	Quantity    int     `json:"quantity"`
	Subtotal    float64 `json:"subtotal"`
	IsFlashSale bool    `json:"is_flash_sale,omitempty"`
	CampaignID  *uint   `json:"campaign_id,omitempty"`
}


// OrderResponse trả về thông tin chi tiết đơn hàng
type OrderResponse struct {
	ID              uint                `json:"id"`
	OrderCode       string              `json:"order_code"`
	UserID          string              `json:"user_id"`
	CustomerName    string              `json:"customer_name"`
	CustomerEmail   string              `json:"customer_email"`
	CustomerPhone   string              `json:"customer_phone"`
	ShippingAddress string              `json:"shipping_address"`
	Note            string              `json:"note,omitempty"`
	PaymentMethod   string              `json:"payment_method"`
	PaymentStatus   string              `json:"payment_status"`
	OrderStatus     string              `json:"order_status"`
	TotalAmount     float64             `json:"total_amount"`
	Items           []OrderItemResponse `json:"items"`
	CreatedAt       string              `json:"created_at"`
}

// OrderListResponse danh sách đơn hàng phân trang
type OrderListResponse struct {
	Orders     []*OrderResponse `json:"orders"`
	Total      int64            `json:"total"`
	Page       int              `json:"page"`
	Limit      int              `json:"limit"`
	TotalPages int              `json:"total_pages"`
}

// UpdateOrderStatusRequest cập nhật trạng thái đơn
type UpdateOrderStatusRequest struct {
	Status string `json:"status" validate:"required,oneof=PENDING CONFIRMED PROCESSING SHIPPED DELIVERED CANCELLED"`
}

// PrewarmStockRequest yêu cầu nạp trước tồn kho Flash Sale
type PrewarmStockRequest struct {
	ProductID uint `json:"product_id" validate:"required,gt=0"`
	Stock     int  `json:"stock" validate:"required,gte=0"`
}

// ProductDetailResponse trả về từ Product Service qua internal HTTP client
type ProductDetailResponse struct {
	ID            uint    `json:"id"`
	Name          string  `json:"name"`
	Slug          string  `json:"slug"`
	Price         float64 `json:"price"`
	DiscountPrice float64 `json:"discount_price"`
	Stock         int     `json:"stock"`
	Thumbnail     string  `json:"thumbnail"`
}
