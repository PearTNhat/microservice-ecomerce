package repository

import (
	"ecomerce-service/services/product-service/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type mixedOrderStockOperationRepository struct {
	db *gorm.DB
}

func NewMixedOrderStockOperationRepository(db *gorm.DB) domain.MixedOrderStockOperationRepository {
	return &mixedOrderStockOperationRepository{db: db}
}

func (r *mixedOrderStockOperationRepository) getDB(tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return tx
	}
	return r.db
}

func (r *mixedOrderStockOperationRepository) GetByOrderID(tx *gorm.DB, orderID uint) (*domain.MixedOrderStockOperation, error) {
	var op domain.MixedOrderStockOperation
	err := r.getDB(tx).Where("order_id = ?", orderID).First(&op).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &op, nil
}

func (r *mixedOrderStockOperationRepository) GetByOrderIDWithLock(tx *gorm.DB, orderID uint) (*domain.MixedOrderStockOperation, error) {
	var op domain.MixedOrderStockOperation
	err := r.getDB(tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("order_id = ?", orderID).First(&op).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &op, nil
}

func (r *mixedOrderStockOperationRepository) Create(tx *gorm.DB, op *domain.MixedOrderStockOperation) error {
	return r.getDB(tx).Create(op).Error
}

func (r *mixedOrderStockOperationRepository) Update(tx *gorm.DB, op *domain.MixedOrderStockOperation) error {
	return r.getDB(tx).Save(op).Error
}
