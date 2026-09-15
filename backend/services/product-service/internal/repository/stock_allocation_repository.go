package repository

import (
	"errors"
	"fmt"
	"time"

	"ecomerce-service/services/product-service/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type stockAllocationRepository struct {
	db *gorm.DB
}

func NewStockAllocationRepository(db *gorm.DB) domain.StockAllocationRepository {
	return &stockAllocationRepository{db: db}
}

func (r *stockAllocationRepository) AllocateStock(campaignID uint, productID uint, requestID string, quantity int) (*domain.ProductStockAllocation, error) {
	if quantity <= 0 {
		return nil, errors.New("số lượng phân bổ phải lớn hơn 0")
	}

	var result *domain.ProductStockAllocation

	err := r.db.Transaction(func(tx *gorm.DB) error {
		// 1. Kiểm tra Idempotency theo request_id
		var existingByReq domain.ProductStockAllocation
		if err := tx.Where("request_id = ?", requestID).First(&existingByReq).Error; err == nil {
			result = &existingByReq
			return nil
		}

		// 2. Kiểm tra xem cặp (campaign_id, product_id) đã được phân bổ chưa
		var existingByCamp domain.ProductStockAllocation
		if err := tx.Where("campaign_id = ? AND product_id = ?", campaignID, productID).First(&existingByCamp).Error; err == nil {
			return fmt.Errorf("sản phẩm #%d đã được phân bổ cho campaign #%d", productID, campaignID)
		}

		// 3. Trừ tồn kho thường trong bảng products
		res := tx.Model(&domain.Product{}).
			Where("id = ? AND stock >= ?", productID, quantity).
			UpdateColumn("stock", gorm.Expr("stock - ?", quantity))

		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return errors.New("không đủ tồn kho bán thường để phân bổ cho Flash Sale")
		}

		// 4. Ghi nhận vào sổ cái phân bổ
		allocation := &domain.ProductStockAllocation{
			CampaignID:        campaignID,
			ProductID:         productID,
			RequestID:         requestID,
			AllocatedQuantity: quantity,
			SoldQuantity:      0,
			ReleasedQuantity:  0,
			Status:            domain.StockAllocationStatusAllocated,
			CreatedAt:         time.Now(),
			UpdatedAt:         time.Now(),
		}

		if err := tx.Create(allocation).Error; err != nil {
			return err
		}

		result = allocation
		return nil
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

func (r *stockAllocationRepository) ReleaseStock(campaignID uint, productID uint, requestID string) (int, error) {
	var releasedCount int

	err := r.db.Transaction(func(tx *gorm.DB) error {
		// 1. Kiểm tra Idempotency theo requestID thông qua bảng processed_events
		if requestID != "" {
			var existing domain.ProcessedEvent
			if err := tx.Where("consumer_name = ? AND event_id = ?", "stock-release", requestID).First(&existing).Error; err == nil {
				// Đã xử lý request release này trước đó
				releasedCount = 0
				return nil
			}
		}

		// 2. Khóa dòng allocation với SELECT ... FOR UPDATE để chống race condition khi có 2 request đồng thời
		var allocation domain.ProductStockAllocation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("campaign_id = ? AND product_id = ?", campaignID, productID).
			First(&allocation).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("không tìm thấy bản ghi phân bổ tồn kho")
			}
			return err
		}

		toRelease := allocation.AllocatedQuantity - allocation.SoldQuantity - allocation.ReleasedQuantity
		if toRelease <= 0 {
			releasedCount = 0
			// Vẫn ghi nhận requestID để các lần gọi sau không phải lock lại vô ích
			if requestID != "" {
				_ = tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&domain.ProcessedEvent{
					ConsumerName: "stock-release",
					EventID:      requestID,
					ProcessedAt:  time.Now(),
				})
			}
			return nil
		}

		// 3. Hoàn lại tồn kho cho Product
		if err := tx.Model(&domain.Product{}).
			Where("id = ?", productID).
			UpdateColumn("stock", gorm.Expr("stock + ?", toRelease)).Error; err != nil {
			return err
		}

		// 4. Cập nhật allocation
		newReleased := allocation.ReleasedQuantity + toRelease
		status := allocation.Status
		if newReleased+allocation.SoldQuantity >= allocation.AllocatedQuantity {
			status = domain.StockAllocationStatusReleased
		}

		if err := tx.Model(&domain.ProductStockAllocation{}).
			Where("id = ?", allocation.ID).
			Updates(map[string]interface{}{
				"released_quantity": newReleased,
				"status":            status,
				"updated_at":        time.Now(),
			}).Error; err != nil {
			return err
		}

		// 5. Ghi nhận requestID vào sổ cái để chống duplicate
		if requestID != "" {
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&domain.ProcessedEvent{
				ConsumerName: "stock-release",
				EventID:      requestID,
				ProcessedAt:  time.Now(),
			}).Error; err != nil {
				return err
			}
		}

		releasedCount = toRelease
		return nil
	})

	return releasedCount, err
}

func (r *stockAllocationRepository) IncrementSoldQuantity(campaignID uint, productID uint, quantity int) error {
	return r.IncrementSoldQuantityTx(r.db, campaignID, productID, quantity)
}

func (r *stockAllocationRepository) IncrementSoldQuantityTx(tx *gorm.DB, campaignID uint, productID uint, quantity int) error {
	if tx == nil {
		tx = r.db
	}

	res := tx.Model(&domain.ProductStockAllocation{}).
		Where("campaign_id = ? AND product_id = ? AND sold_quantity + released_quantity + ? <= allocated_quantity", campaignID, productID, quantity).
		Updates(map[string]interface{}{
			"sold_quantity": gorm.Expr("sold_quantity + ?", quantity),
			"updated_at":    time.Now(),
		})

	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("không thể tăng số lượng đã bán: vượt quá số lượng phân bổ hoặc không tìm thấy allocation")
	}
	return nil
}

func (r *stockAllocationRepository) FindByCampaignAndProduct(campaignID uint, productID uint) (*domain.ProductStockAllocation, error) {
	var allocation domain.ProductStockAllocation
	err := r.db.Where("campaign_id = ? AND product_id = ?", campaignID, productID).First(&allocation).Error
	if err != nil {
		return nil, err
	}
	return &allocation, nil
}
