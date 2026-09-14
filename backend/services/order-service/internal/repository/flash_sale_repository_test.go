package repository

import (
	"fmt"
	"testing"
	"time"

	"ecomerce-service/services/order-service/internal/domain"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupOrderTestDB(t *testing.T) *gorm.DB {
	dbName := fmt.Sprintf("file:order_memdb_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
	assert.NoError(t, err)

	err = db.AutoMigrate(
		&domain.FlashSaleCampaign{},
		&domain.FlashSaleItem{},
		&domain.FlashSaleReservation{},
		&domain.OutboxEvent{},
		&domain.ProcessedEvent{},
	)
	assert.NoError(t, err)

	return db
}

func TestFlashSaleRepository_CampaignAndItems(t *testing.T) {
	db := setupOrderTestDB(t)
	repo := NewFlashSaleRepository(db)

	now := time.Now()
	camp := &domain.FlashSaleCampaign{
		Name:     "Flash Sale 12h",
		StartsAt: now,
		EndsAt:   now.Add(2 * time.Hour),
		Status:   domain.CampaignStatusDraft,
	}
	assert.NoError(t, repo.CreateCampaign(camp))

	// CAS Status Update
	err := repo.UpdateCampaignStatus(camp.ID, domain.CampaignStatusDraft, domain.CampaignStatusActive)
	assert.NoError(t, err)

	// Lỗi khi CAS không đúng fromStatus
	err = repo.UpdateCampaignStatus(camp.ID, domain.CampaignStatusDraft, domain.CampaignStatusActive)
	assert.Error(t, err)

	// Thêm Item
	item := &domain.FlashSaleItem{
		CampaignID:          camp.ID,
		ProductID:           101,
		SalePrice:           500000,
		OriginalPrice:       1000000,
		AllocatedStock:      10,
		ReservedStock:       0,
		SoldStock:           0,
		MaxQuantityPerUser:  1,
		MaxQuantityPerOrder: 1,
		ReservationSeconds:  120,
	}
	assert.NoError(t, repo.AddItem(item))

	foundItem, err := repo.GetItem(camp.ID, 101)
	assert.NoError(t, err)
	assert.Equal(t, item.ID, foundItem.ID)
	assert.Equal(t, 10, foundItem.AllocatedStock)
}

func TestFlashSaleRepository_ReservationAndOutbox(t *testing.T) {
	db := setupOrderTestDB(t)
	fsRepo := NewFlashSaleRepository(db)
	outboxRepo := NewOutboxRepository(db)

	camp := &domain.FlashSaleCampaign{
		Name:     "Campaign 1",
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(time.Hour),
		Status:   domain.CampaignStatusActive,
	}
	assert.NoError(t, fsRepo.CreateCampaign(camp))

	item := &domain.FlashSaleItem{
		CampaignID:     camp.ID,
		ProductID:      202,
		SalePrice:      100000,
		OriginalPrice:  200000,
		AllocatedStock: 5,
	}
	assert.NoError(t, fsRepo.AddItem(item))

	// 1. Tạo Reservation cùng Outbox trong 1 transaction
	resvID := "FSR-TEST-001"
	resv := &domain.FlashSaleReservation{
		ID:                 resvID,
		RequestID:          "req-uuid-1",
		RequestFingerprint: "fp-hash-1",
		CampaignID:         camp.ID,
		FlashSaleItemID:    item.ID,
		ProductID:          item.ProductID,
		UserID:             "user-1",
		Quantity:           2,
		UnitPrice:          100000,
		TotalAmount:        200000,
		PaymentMethod:      "COD",
		Status:             domain.ReservationStatusReserved,
		ExpiresAt:          time.Now().Add(2 * time.Minute),
	}

	outbox := &domain.OutboxEvent{
		ID:            "evt-001",
		AggregateType: "FlashSaleReservation",
		AggregateID:   resvID,
		EventType:     "FLASH_SALE_RESERVED",
		Topic:         "flashsale.orders",
		PartitionKey:  "user-1",
		Payload:       `{"reservation_id":"FSR-TEST-001"}`,
		Status:        domain.OutboxStatusPending,
		NextAttemptAt: time.Now(),
	}

	err := fsRepo.CreateReservationWithOutbox(resv, outbox)
	assert.NoError(t, err)

	// Kiểm tra item reserved_stock tăng lên 2
	itemCheck, _ := fsRepo.GetItemByID(item.ID)
	assert.Equal(t, 2, itemCheck.ReservedStock)

	// 2. Outbox Worker claim batch
	events, err := outboxRepo.ClaimPendingBatch(10, "worker-1", 30*time.Second)
	assert.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, "evt-001", events[0].ID)
	assert.Equal(t, domain.OutboxStatusProcessing, events[0].Status)

	// Mark Published
	err = outboxRepo.MarkPublished("evt-001")
	assert.NoError(t, err)

	// 3. Confirm Reservation DB
	orderID := uint(999)
	err = fsRepo.ConfirmReservationDB(nil, resvID, orderID)
	assert.NoError(t, err)

	itemAfterConfirm, _ := fsRepo.GetItemByID(item.ID)
	assert.Equal(t, 0, itemAfterConfirm.ReservedStock)
	assert.Equal(t, 2, itemAfterConfirm.SoldStock)

	resvCheck, _ := fsRepo.FindReservationByID(resvID)
	assert.Equal(t, domain.ReservationStatusConfirmed, resvCheck.Status)
	assert.Equal(t, orderID, *resvCheck.OrderID)
}

func TestProcessedEventRepository(t *testing.T) {
	db := setupOrderTestDB(t)
	repo := NewProcessedEventRepository(db)

	consumer := "flash-sale-worker"
	eventID := "evt-123"

	has, err := repo.HasProcessed(consumer, eventID)
	assert.NoError(t, err)
	assert.False(t, has)

	err = repo.MarkProcessed(nil, consumer, eventID)
	assert.NoError(t, err)

	has, err = repo.HasProcessed(consumer, eventID)
	assert.NoError(t, err)
	assert.True(t, has)

	// Idempotent: gọi lại không lỗi
	err = repo.MarkProcessed(nil, consumer, eventID)
	assert.NoError(t, err)
}
