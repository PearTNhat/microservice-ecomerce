package repository

import (
	"errors"
	"time"

	"ecomerce-service/services/order-service/internal/domain"

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

func (r *orderRepository) FindOrderByCheckoutAttemptID(attemptID uint) (*domain.Order, error) {
	var order domain.Order
	err := r.db.Preload("Items").Where("checkout_attempt_id = ?", attemptID).First(&order).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &order, nil
}

func (r *orderRepository) GetCheckoutAttempt(userID, key string) (*domain.CheckoutAttempt, error) {
	var attempt domain.CheckoutAttempt
	err := r.db.Where("user_id = ? AND idempotency_key = ?", userID, key).First(&attempt).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &attempt, nil
}

func (r *orderRepository) GetCheckoutAttemptByID(id uint) (*domain.CheckoutAttempt, error) {
	var attempt domain.CheckoutAttempt
	err := r.db.Where("id = ?", id).First(&attempt).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &attempt, nil
}

func (r *orderRepository) CreateCheckoutAttempt(attempt *domain.CheckoutAttempt) error {
	return r.db.Create(attempt).Error
}

func (r *orderRepository) SaveCheckoutAttempt(attempt *domain.CheckoutAttempt) error {
	return r.db.Save(attempt).Error
}

func (r *orderRepository) CASClaimPending(attempt *domain.CheckoutAttempt) (bool, error) {
	err := r.db.Create(attempt).Error
	if err != nil {
		// Trùng lặp unique constraint (user_id, idempotency_key)
		return false, err
	}
	return true, nil
}

func (r *orderRepository) CASRecoveringTakeover(id uint, expectedVersion uint64, newOwner string, newLease time.Time, recoveryTarget string) (*domain.CheckoutAttempt, bool, error) {
	res := r.db.Model(&domain.CheckoutAttempt{}).
		Where("id = ? AND status IN (?, ?) AND version = ? AND (lease_expires_at IS NULL OR lease_expires_at <= ?)", 
			id, domain.CheckoutAttemptStatusPending, domain.CheckoutAttemptStatusFailed, expectedVersion, time.Now()).
		Updates(map[string]interface{}{
			"status":           domain.CheckoutAttemptStatusRecovering,
			"version":          gorm.Expr("version + 1"),
			"owner_token":      newOwner,
			"lease_expires_at": newLease,
			"recovery_target":  recoveryTarget,
			"updated_at":       time.Now(),
		})

	if res.Error != nil {
		return nil, false, res.Error
	}
	if res.RowsAffected == 0 {
		return nil, false, nil
	}

	updated, err := r.GetCheckoutAttemptByID(id)
	if err != nil {
		return nil, true, err
	}
	return updated, true, nil
}

func (r *orderRepository) CASRenewLease(id uint, expectedVersion uint64, ownerToken string, newLease time.Time) (bool, error) {
	res := r.db.Model(&domain.CheckoutAttempt{}).
		Where("id = ? AND version = ? AND owner_token = ?", id, expectedVersion, ownerToken).
		Updates(map[string]interface{}{
			"lease_expires_at": newLease,
			"updated_at":       time.Now(),
		})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}

func (r *orderRepository) TransitionAttemptStatus(id uint, expectedVersion uint64, newStatus string, recoveryTarget string) (bool, error) {
	updates := map[string]interface{}{
		"status":     newStatus,
		"updated_at": time.Now(),
	}
	if recoveryTarget != "" {
		updates["recovery_target"] = recoveryTarget
	}
	res := r.db.Model(&domain.CheckoutAttempt{}).
		Where("id = ? AND version = ?", id, expectedVersion).
		Updates(updates)
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}

func (r *orderRepository) CompleteAttemptInTx(tx *gorm.DB, attemptID uint, expectedVersion uint64, orderID uint, orderCode string, responsePayload string) error {
	res := tx.Model(&domain.CheckoutAttempt{}).
		Where("id = ? AND version = ?", attemptID, expectedVersion).
		Updates(map[string]interface{}{
			"status":           domain.CheckoutAttemptStatusCompleted,
			"order_id":         orderID,
			"order_code":       orderCode,
			"response_payload": responsePayload,
			"updated_at":       time.Now(),
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("FENCING_LOST: attempt version đã bị thay đổi hoặc không tìm thấy khi hoàn tất transaction")
	}
	return nil
}

