package postgres

import (
	"errors"
	"ecomerce-service/internal/core/domain"

	"gorm.io/gorm"
)

type orderRepository struct {
	db *gorm.DB
}

func NewOrderRepository(db *gorm.DB) domain.OrderRepository {
	return &orderRepository{db: db}
}

func (r *orderRepository) CreateOrder(order *domain.Order) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(order).Error; err != nil {
			return err
		}
		return nil
	})
}

func (r *orderRepository) FindByID(id uint) (*domain.Order, error) {
	var order domain.Order
	err := r.db.Preload("Items").First(&order, id).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &order, nil
}

func (r *orderRepository) FindByOrderCode(orderCode string) (*domain.Order, error) {
	var order domain.Order
	err := r.db.Preload("Items").Where("order_code = ?", orderCode).First(&order).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &order, nil
}

func (r *orderRepository) FindByUserID(userID string, page int, limit int) ([]*domain.Order, int64, error) {
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 10
	}
	offset := (page - 1) * limit

	var total int64
	var orders []*domain.Order

	query := r.db.Model(&domain.Order{}).Where("user_id = ?", userID)
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	err := query.Preload("Items").
		Order("created_at DESC").
		Offset(offset).
		Limit(limit).
		Find(&orders).Error

	if err != nil {
		return nil, 0, err
	}

	return orders, total, nil
}

func (r *orderRepository) UpdateStatus(orderID uint, status string) error {
	return r.db.Model(&domain.Order{}).
		Where("id = ?", orderID).
		Update("order_status", status).Error
}

func (r *orderRepository) UpdatePaymentStatus(orderID uint, status string) error {
	return r.db.Model(&domain.Order{}).
		Where("id = ?", orderID).
		Update("payment_status", status).Error
}
