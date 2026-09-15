package repository

import (
	"errors"
	"fmt"
	"time"

	"ecomerce-service/services/order-service/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type flashSaleRepository struct {
	db *gorm.DB
}

func NewFlashSaleRepository(db *gorm.DB) domain.FlashSaleRepository {
	return &flashSaleRepository{db: db}
}

func (r *flashSaleRepository) CreateCampaign(campaign *domain.FlashSaleCampaign) error {
	return r.db.Create(campaign).Error
}

func (r *flashSaleRepository) GetCampaignByID(id uint) (*domain.FlashSaleCampaign, error) {
	var campaign domain.FlashSaleCampaign
	err := r.db.Preload("Items").First(&campaign, id).Error
	if err != nil {
		return nil, err
	}
	return &campaign, nil
}

func (r *flashSaleRepository) GetActiveCampaign() (*domain.FlashSaleCampaign, error) {
	var campaign domain.FlashSaleCampaign
	now := time.Now()
	// 1. Ưu tiên tìm campaign ACTIVE trong khung giờ starts_at <= now <= ends_at
	err := r.db.Preload("Items").
		Where("status = ? AND starts_at <= ? AND ends_at >= ?", domain.CampaignStatusActive, now, now).
		Order("id DESC").
		First(&campaign).Error
	if err == nil {
		return &campaign, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	// 2. Dự phòng: tìm campaign có status = ACTIVE gần nhất
	err = r.db.Preload("Items").
		Where("status = ?", domain.CampaignStatusActive).
		Order("id DESC").
		First(&campaign).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil // Không có campaign active nào
		}
		return nil, err
	}
	return &campaign, nil
}

func (r *flashSaleRepository) UpdateCampaignStatus(id uint, fromStatus domain.CampaignStatus, toStatus domain.CampaignStatus) error {
	res := r.db.Model(&domain.FlashSaleCampaign{}).
		Where("id = ? AND status = ?", id, fromStatus).
		Updates(map[string]interface{}{
			"status":     toStatus,
			"version":    gorm.Expr("version + 1"),
			"updated_at": time.Now(),
		})

	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("không thể chuyển trạng thái campaign từ %s sang %s", fromStatus, toStatus)
	}
	return nil
}

func (r *flashSaleRepository) ListCampaigns(status string, page int, limit int) ([]*domain.FlashSaleCampaign, int64, error) {
	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 100 {
		limit = 10
	}
	offset := (page - 1) * limit

	var campaigns []*domain.FlashSaleCampaign
	var total int64

	query := r.db.Model(&domain.FlashSaleCampaign{})
	if status != "" {
		query = query.Where("status = ?", status)
	}

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	err := query.Preload("Items").Order("starts_at DESC").Offset(offset).Limit(limit).Find(&campaigns).Error
	return campaigns, total, err
}

func (r *flashSaleRepository) AddItem(item *domain.FlashSaleItem) error {
	return r.db.Create(item).Error
}

func (r *flashSaleRepository) GetItem(campaignID uint, productID uint) (*domain.FlashSaleItem, error) {
	var item domain.FlashSaleItem
	err := r.db.Where("campaign_id = ? AND product_id = ?", campaignID, productID).First(&item).Error
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (r *flashSaleRepository) GetItemByID(id uint) (*domain.FlashSaleItem, error) {
	var item domain.FlashSaleItem
	err := r.db.First(&item, id).Error
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (r *flashSaleRepository) CreateReservationWithOutbox(reservation *domain.FlashSaleReservation, outbox *domain.OutboxEvent) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		// 1. Ghi reservation
		if err := tx.Create(reservation).Error; err != nil {
			return err
		}

		// 2. Cập nhật reserved_stock trong flash_sale_items bằng conditional update
		res := tx.Model(&domain.FlashSaleItem{}).
			Where("id = ? AND reserved_stock + sold_stock + ? <= allocated_stock", reservation.FlashSaleItemID, reservation.Quantity).
			Updates(map[string]interface{}{
				"reserved_stock": gorm.Expr("reserved_stock + ?", reservation.Quantity),
				"version":        gorm.Expr("version + 1"),
				"updated_at":     time.Now(),
			})

		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return errors.New("không đủ tồn kho Flash Sale để giữ chỗ trong Database")
		}

		// 3. Ghi outbox_event trong cùng transaction
		if outbox != nil {
			if err := tx.Create(outbox).Error; err != nil {
				return err
			}
		}

		return nil
	})
}

func (r *flashSaleRepository) FindReservationByIdempotency(campaignID uint, productID uint, userID string, requestID string) (*domain.FlashSaleReservation, error) {
	var reservation domain.FlashSaleReservation
	err := r.db.Where("campaign_id = ? AND product_id = ? AND user_id = ? AND request_id = ?",
		campaignID, productID, userID, requestID).First(&reservation).Error
	if err != nil {
		return nil, err
	}
	return &reservation, nil
}

func (r *flashSaleRepository) FindReservationByID(id string) (*domain.FlashSaleReservation, error) {
	var reservation domain.FlashSaleReservation
	err := r.db.Where("id = ?", id).First(&reservation).Error
	if err != nil {
		return nil, err
	}
	return &reservation, nil
}

func (r *flashSaleRepository) UpdateReservationStatusCAS(id string, fromStatus domain.ReservationStatus, toStatus domain.ReservationStatus, orderID *uint) error {
	updates := map[string]interface{}{
		"status":     toStatus,
		"version":    gorm.Expr("version + 1"),
		"updated_at": time.Now(),
	}
	if orderID != nil {
		updates["order_id"] = *orderID
	}

	res := r.db.Model(&domain.FlashSaleReservation{}).
		Where("id = ? AND status = ?", id, fromStatus).
		Updates(updates)

	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("không thể cập nhật trạng thái reservation từ %s sang %s (CAS mismatch)", fromStatus, toStatus)
	}
	return nil
}

func (r *flashSaleRepository) ClaimExpiredReservations(batchSize int) ([]*domain.FlashSaleReservation, error) {
	var reservations []*domain.FlashSaleReservation

	err := r.db.Transaction(func(tx *gorm.DB) error {
		// Claim các reservation hết hạn bằng SELECT ... FOR UPDATE SKIP LOCKED
		err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("status IN (?, ?) AND expires_at <= ?", domain.ReservationStatusReserved, domain.ReservationStatusProcessing, time.Now()).
			Limit(batchSize).
			Find(&reservations).Error

		return err
	})

	return reservations, err
}

func (r *flashSaleRepository) ConfirmReservationDB(tx *gorm.DB, reservationID string, orderID uint) error {
	db := r.db
	if tx != nil {
		db = tx
	}

	var resv domain.FlashSaleReservation
	if err := db.Where("id = ?", reservationID).First(&resv).Error; err != nil {
		return err
	}

	// 1. CAS Reservation status sang CONFIRMED
	res := db.Model(&domain.FlashSaleReservation{}).
		Where("id = ? AND status IN (?, ?) AND expires_at > ?", reservationID, domain.ReservationStatusReserved, domain.ReservationStatusProcessing, time.Now()).
		Updates(map[string]interface{}{
			"status":     domain.ReservationStatusConfirmed,
			"order_id":   orderID,
			"version":    gorm.Expr("version + 1"),
			"updated_at": time.Now(),
		})

	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("không thể confirm reservation: đã hết hạn hoặc không ở trạng thái hợp lệ")
	}

	// 2. Chuyển reserved_stock -> sold_stock trong flash_sale_items
	resItem := db.Model(&domain.FlashSaleItem{}).
		Where("id = ? AND reserved_stock >= ?", resv.FlashSaleItemID, resv.Quantity).
		Updates(map[string]interface{}{
			"reserved_stock": gorm.Expr("reserved_stock - ?", resv.Quantity),
			"sold_stock":     gorm.Expr("sold_stock + ?", resv.Quantity),
			"version":        gorm.Expr("version + 1"),
			"updated_at":     time.Now(),
		})

	if resItem.Error != nil {
		return resItem.Error
	}
	if resItem.RowsAffected == 0 {
		return errors.New("không đủ reserved_stock để chuyển sang sold_stock")
	}

	return nil
}

func (r *flashSaleRepository) ConfirmReservationAndCreateOrder(
	tx *gorm.DB,
	inputEventID string,
	order *domain.Order,
	reservationID string,
	outboxEvents []*domain.OutboxEvent,
) error {
	db := r.db
	if tx != nil {
		db = tx
	}

	// 1. Kiểm tra và ghi nhận processed_events nếu có inputEventID
	if inputEventID != "" {
		processed := domain.ProcessedEvent{
			ConsumerName: "flash-sale-worker",
			EventID:      inputEventID,
			ProcessedAt:  time.Now(),
		}
		res := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&processed)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return errors.New("event đã được xử lý trước đó (duplicate)")
		}
	}

	// 2. Tạo Order và OrderItems
	if err := db.Create(order).Error; err != nil {
		return err
	}

	// 3. Confirm reservation trong DB và chuyển reserved_stock -> sold_stock
	if err := r.ConfirmReservationDB(db, reservationID, order.ID); err != nil {
		return err
	}

	// 4. Lưu toàn bộ các Outbox Events
	for _, outbox := range outboxEvents {
		if err := db.Create(outbox).Error; err != nil {
			return err
		}
	}

	return nil
}

func (r *flashSaleRepository) ReleaseReservationDB(tx *gorm.DB, reservationID string, newStatus domain.ReservationStatus) error {
	db := r.db
	if tx != nil {
		db = tx
	}

	var resv domain.FlashSaleReservation
	if err := db.Where("id = ?", reservationID).First(&resv).Error; err != nil {
		return err
	}

	// 1. CAS Reservation status sang EXPIRED / CANCELLED
	res := db.Model(&domain.FlashSaleReservation{}).
		Where("id = ? AND status IN (?, ?)", reservationID, domain.ReservationStatusReserved, domain.ReservationStatusProcessing).
		Updates(map[string]interface{}{
			"status":     newStatus,
			"version":    gorm.Expr("version + 1"),
			"updated_at": time.Now(),
		})

	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("reservation không ở trạng thái có thể giải phóng (đã CONFIRMED hoặc đã RELEASED)")
	}

	// 2. Hoàn lại reserved_stock trong flash_sale_items
	resItem := db.Model(&domain.FlashSaleItem{}).
		Where("id = ? AND reserved_stock >= ?", resv.FlashSaleItemID, resv.Quantity).
		Updates(map[string]interface{}{
			"reserved_stock": gorm.Expr("reserved_stock - ?", resv.Quantity),
			"version":        gorm.Expr("version + 1"),
			"updated_at":     time.Now(),
		})

	if resItem.Error != nil {
		return resItem.Error
	}
	return nil
}
