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
	Items           []CreateOrderItemRequest `json:"items,omitempty"`
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
