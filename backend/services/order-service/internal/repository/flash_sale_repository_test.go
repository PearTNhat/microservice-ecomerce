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
		&domain.Order{},
		&domain.OrderItem{},
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
	err = outboxRepo.MarkPublished("evt-001", "worker-1")
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

func TestConfirmReservationAndCreateOrder(t *testing.T) {
	db := setupOrderTestDB(t)
	fsRepo := NewFlashSaleRepository(db)

	camp := &domain.FlashSaleCampaign{
		Name:     "Test Camp",
		StartsAt: time.Now().Add(-1 * time.Hour),
		EndsAt:   time.Now().Add(1 * time.Hour),
		Status:   domain.CampaignStatusActive,
	}
	assert.NoError(t, fsRepo.CreateCampaign(camp))

	item := &domain.FlashSaleItem{
		CampaignID:     camp.ID,
		ProductID:      10,
		SalePrice:      500000,
		AllocatedStock: 20,
		ReservedStock:  2,
		SoldStock:      0,
	}
	assert.NoError(t, fsRepo.AddItem(item))

	resvID := "FSR-TEST-CONFIRM"
	resv := &domain.FlashSaleReservation{
		ID:              resvID,
		CampaignID:      camp.ID,
		FlashSaleItemID: item.ID,
		ProductID:       10,
		UserID:          "user-1",
		Quantity:        2,
		Status:          domain.ReservationStatusReserved,
		ExpiresAt:       time.Now().Add(5 * time.Minute),
	}
	assert.NoError(t, db.Create(resv).Error)

	order := &domain.Order{
		OrderCode:     "ORD-FS-TEST-1",
		UserID:        "user-1",
		CustomerName:  "Test Customer",
		CustomerEmail: "test@example.com",
		TotalAmount:   1000000,
		OrderStatus:   domain.OrderStatusPending,
	}

	outboxFS := &domain.OutboxEvent{
		ID:            "outbox-fs-1",
		AggregateType: "FlashSaleOrder",
		EventType:     "FLASH_SALE_ORDER_CONFIRMED",
		Topic:         "flashsale.confirmed",
		Payload:       `{"event_id":"outbox-fs-1"}`,
		Status:        domain.OutboxStatusPending,
	}
	outboxOrder := &domain.OutboxEvent{
		ID:            "outbox-order-1",
		AggregateType: "Order",
		EventType:     "ORDER_CREATED",
		Topic:         "order.events",
		Payload:       `{"event_id":"outbox-order-1"}`,
		Status:        domain.OutboxStatusPending,
	}

	inputEventID := "input-evt-1"

	// 1. First execution -> succeeds
	err := fsRepo.ConfirmReservationAndCreateOrder(db, inputEventID, order, resvID, []*domain.OutboxEvent{outboxFS, outboxOrder})
	assert.NoError(t, err)

	// Verify order created
	var savedOrder domain.Order
	assert.NoError(t, db.Where("order_code = ?", "ORD-FS-TEST-1").First(&savedOrder).Error)
	assert.Equal(t, "user-1", savedOrder.UserID)

	// Verify reservation CONFIRMED
	resvCheck, err := fsRepo.FindReservationByID(resvID)
	assert.NoError(t, err)
	assert.Equal(t, domain.ReservationStatusConfirmed, resvCheck.Status)
	assert.Equal(t, savedOrder.ID, *resvCheck.OrderID)

	// Verify stock shifted
	itemCheck, _ := fsRepo.GetItemByID(item.ID)
	assert.Equal(t, 0, itemCheck.ReservedStock)
	assert.Equal(t, 2, itemCheck.SoldStock)

	// Verify both outbox events inserted
	var outboxCount int64
	db.Model(&domain.OutboxEvent{}).Where("id IN ?", []string{"outbox-fs-1", "outbox-order-1"}).Count(&outboxCount)
	assert.Equal(t, int64(2), outboxCount)

	// 2. Re-execution with same inputEventID -> duplicate error, does not create second order
	order2 := &domain.Order{
		OrderCode: "ORD-FS-TEST-2",
		UserID:    "user-1",
	}
	err = fsRepo.ConfirmReservationAndCreateOrder(db, inputEventID, order2, resvID, nil)
	assert.Error(t, err)

	var order2Count int64
	db.Model(&domain.Order{}).Where("order_code = ?", "ORD-FS-TEST-2").Count(&order2Count)
	assert.Equal(t, int64(0), order2Count)
}

func TestOutboxRepository_LeaseOwnership(t *testing.T) {
	db := setupOrderTestDB(t)
	repo := NewOutboxRepository(db)

	evt := &domain.OutboxEvent{
		ID:            "evt-lease-test",
		AggregateType: "Order",
		EventType:     "ORDER_CREATED",
		Topic:         "order.events",
		Payload:       `{}`,
		Status:        domain.OutboxStatusPending,
		NextAttemptAt: time.Now().Add(-1 * time.Minute),
	}
	assert.NoError(t, db.Create(evt).Error)

	// Worker-1 claims the batch
	events, err := repo.ClaimPendingBatch(10, "worker-1", 30*time.Second)
	assert.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, "worker-1", events[0].LockedBy)

	// Worker-2 attempts to MarkPublished or MarkFailed -> rejected
	err = repo.MarkPublished("evt-lease-test", "worker-2")
	assert.Error(t, err)

	err = repo.MarkFailed("evt-lease-test", "worker-2", 1*time.Minute)
	assert.Error(t, err)

	// Worker-1 MarkPublished -> succeeds
	err = repo.MarkPublished("evt-lease-test", "worker-1")
	assert.NoError(t, err)

	var saved domain.OutboxEvent
	assert.NoError(t, db.Where("id = ?", "evt-lease-test").First(&saved).Error)
	assert.Equal(t, domain.OutboxStatusPublished, saved.Status)
}
