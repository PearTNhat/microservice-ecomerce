package domain

import "time"

// Cart đại diện cho giỏ hàng của một khách hàng
type Cart struct {
	ID        uint       `json:"id" gorm:"primaryKey"`
	UserID    string     `json:"user_id" gorm:"uniqueIndex;not null"`
	Items     []CartItem `json:"items" gorm:"foreignKey:CartID;constraint:OnDelete:CASCADE"`
	CreatedAt time.Time  `json:"created_at" gorm:"default:current_timestamp"`
	UpdatedAt time.Time  `json:"updated_at" gorm:"default:current_timestamp"`
}

// CartItem đại diện cho một món hàng trong giỏ
type CartItem struct {
	ID          uint      `json:"id" gorm:"primaryKey"`
	CartID      uint      `json:"cart_id" gorm:"not null;index"`
	ProductID   uint      `json:"product_id" gorm:"not null;index"`
	ProductName string    `json:"product_name" gorm:"not null"`
	ProductSlug string    `json:"product_slug,omitempty"`
	Thumbnail   string    `json:"thumbnail,omitempty"`
	Price       float64   `json:"price" gorm:"not null"`
	Quantity    int       `json:"quantity" gorm:"not null;default:1"`
	CreatedAt   time.Time `json:"created_at" gorm:"default:current_timestamp"`
	UpdatedAt   time.Time `json:"updated_at" gorm:"default:current_timestamp"`
}

// CalculateTotal tính tổng giá trị toàn bộ giỏ hàng
func (c *Cart) CalculateTotal() float64 {
	var total float64
	for _, item := range c.Items {
		total += item.Price * float64(item.Quantity)
	}
	return total
}

// GetItemCount tính tổng số lượng món hàng trong giỏ
func (c *Cart) GetItemCount() int {
	var count int
	for _, item := range c.Items {
		count += item.Quantity
	}
	return count
}

// CartRepository định nghĩa interface thao tác dữ liệu giỏ hàng
type CartRepository interface {
	GetCartByUserID(userID string) (*Cart, error)
	AddItem(cartID uint, item *CartItem) error
	UpdateItemQuantity(cartID uint, itemID uint, quantity int) error
	RemoveItem(cartID uint, itemID uint) error
	ClearCart(cartID uint) error
}
