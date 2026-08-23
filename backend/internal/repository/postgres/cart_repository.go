package postgres

import (
	"errors"
	"ecomerce-service/internal/core/domain"

	"gorm.io/gorm"
)

type cartRepository struct {
	db *gorm.DB
}

func NewCartRepository(db *gorm.DB) domain.CartRepository {
	return &cartRepository{db: db}
}

func (r *cartRepository) GetCartByUserID(userID string) (*domain.Cart, error) {
	var cart domain.Cart
	err := r.db.Preload("Items").Where("user_id = ?", userID).First(&cart).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Tạo giỏ hàng mới cho user nếu chưa có
			newCart := domain.Cart{
				UserID: userID,
				Items:  []domain.CartItem{},
			}
			if createErr := r.db.Create(&newCart).Error; createErr != nil {
				return nil, createErr
			}
			return &newCart, nil
		}
		return nil, err
	}
	return &cart, nil
}

func (r *cartRepository) AddItem(cartID uint, item *domain.CartItem) error {
	var existing domain.CartItem
	err := r.db.Where("cart_id = ? AND product_id = ?", cartID, item.ProductID).First(&existing).Error
	if err == nil {
		// Món hàng đã có trong giỏ -> Cộng dồn số lượng
		existing.Quantity += item.Quantity
		existing.Price = item.Price
		return r.db.Save(&existing).Error
	}

	if errors.Is(err, gorm.ErrRecordNotFound) {
		item.CartID = cartID
		return r.db.Create(item).Error
	}

	return err
}

func (r *cartRepository) UpdateItemQuantity(cartID uint, itemID uint, quantity int) error {
	if quantity <= 0 {
		return r.RemoveItem(cartID, itemID)
	}
	return r.db.Model(&domain.CartItem{}).
		Where("id = ? AND cart_id = ?", itemID, cartID).
		Update("quantity", quantity).Error
}

func (r *cartRepository) RemoveItem(cartID uint, itemID uint) error {
	return r.db.Where("id = ? AND cart_id = ?", itemID, cartID).
		Delete(&domain.CartItem{}).Error
}

func (r *cartRepository) ClearCart(cartID uint) error {
	return r.db.Where("cart_id = ?", cartID).
		Delete(&domain.CartItem{}).Error
}
