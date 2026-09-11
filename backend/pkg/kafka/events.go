package kafka

import "time"

// OrderItemPayload chứa thông tin từng món hàng trong sự kiện
type OrderItemPayload struct {
	ProductID   uint    `json:"product_id"`
	ProductName string  `json:"product_name"`
	ProductSlug string  `json:"product_slug,omitempty"`
	Thumbnail   string  `json:"thumbnail,omitempty"`
	Price       float64 `json:"price"`
	Quantity    int     `json:"quantity"`
	Subtotal    float64 `json:"subtotal"`
}

// OrderCreatedPayload sự kiện đơn hàng được tạo (PENDING) bắn vào topic "order.events"
type OrderCreatedPayload struct {
	EventType       string             `json:"event_type"` // ORDER_CREATED
	OrderID         uint               `json:"order_id"`
	OrderCode       string             `json:"order_code"`
	UserID          string             `json:"user_id"`
	CustomerEmail   string             `json:"customer_email"`
	CustomerName    string             `json:"customer_name"`
	CustomerPhone   string             `json:"customer_phone"`
	ShippingAddress string             `json:"shipping_address"`
	TotalAmount     float64            `json:"total_amount"`
	PaymentMethod   string             `json:"payment_method"`
	Items           []OrderItemPayload `json:"items"`
	TraceID         string             `json:"trace_id,omitempty"`
	CreatedAt       time.Time          `json:"created_at"`
}

// StockResultPayload sự kiện kết quả trừ tồn kho do Product Service bắn vào "stock.events" (Saga Choreography)
type StockResultPayload struct {
	EventType string             `json:"event_type"` // STOCK_DEDUCTED_SUCCESS hoặc STOCK_DEDUCTED_FAILED
	OrderID   uint               `json:"order_id"`
	OrderCode string             `json:"order_code"`
	Success   bool               `json:"success"`
	Reason    string             `json:"reason,omitempty"`
	Items     []OrderItemPayload `json:"items,omitempty"`
	TraceID   string             `json:"trace_id,omitempty"`
	Timestamp time.Time          `json:"timestamp"`
}

// FlashSaleOrderTaskPayload tác vụ tạo đơn Flash Sale bất đồng bộ bắn vào "flashsale.orders"
type FlashSaleOrderTaskPayload struct {
	OrderToken      string    `json:"order_token"`
	UserID          string    `json:"user_id"`
	ProductID       uint      `json:"product_id"`
	Quantity        int       `json:"quantity"`
	Price           float64   `json:"price"`
	CustomerName    string    `json:"customer_name"`
	CustomerEmail   string    `json:"customer_email"`
	CustomerPhone   string    `json:"customer_phone"`
	ShippingAddress string    `json:"shipping_address"`
	PaymentMethod   string    `json:"payment_method"`
	TraceID         string    `json:"trace_id,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
}

// DeadLetterPayload thông tin sự kiện lỗi được chuyển vào Dead Letter Topic "orders.dead_letter"
type DeadLetterPayload struct {
	OriginalTopic string    `json:"original_topic"`
	PartitionKey  string    `json:"partition_key"`
	RawPayload    string    `json:"raw_payload"`
	ErrorMessage  string    `json:"error_message"`
	TraceID       string    `json:"trace_id,omitempty"`
	FailedAt      time.Time `json:"failed_at"`
}
