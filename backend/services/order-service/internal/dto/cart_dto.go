package dto

// AddToCartRequest yêu cầu thêm sản phẩm vào giỏ hàng
type AddToCartRequest struct {
	ProductID uint `json:"product_id" validate:"required,gt=0"`
	Quantity  int  `json:"quantity" validate:"required,gt=0"`
}

// UpdateCartItemRequest yêu cầu cập nhật số lượng món hàng trong giỏ
type UpdateCartItemRequest struct {
	Quantity int `json:"quantity" validate:"required,gt=0"`
}

// CartItemResponse trả về chi tiết món hàng trong giỏ
type CartItemResponse struct {
	ID          uint    `json:"id"`
	ProductID   uint    `json:"product_id"`
	ProductName string  `json:"product_name"`
	ProductSlug string  `json:"product_slug,omitempty"`
	Thumbnail   string  `json:"thumbnail,omitempty"`
	Price       float64 `json:"price"`
	Quantity    int     `json:"quantity"`
	Subtotal    float64 `json:"subtotal"`
}

// CartResponse trả về toàn bộ giỏ hàng
type CartResponse struct {
	ID         uint               `json:"id"`
	UserID     string             `json:"user_id"`
	Items      []CartItemResponse `json:"items"`
	TotalItems int                `json:"total_items"`
	TotalPrice float64            `json:"total_price"`
}
