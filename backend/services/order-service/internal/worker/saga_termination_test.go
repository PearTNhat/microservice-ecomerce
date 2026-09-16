package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/repository"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupSagaWorkerTestDB(t *testing.T) (*gorm.DB, *miniredis.Miniredis, *redis.Client) {
	dbName := fmt.Sprintf("file:saga_worker_test_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
	assert.NoError(t, err)

	err = db.AutoMigrate(
		&domain.Order{},
		&domain.OrderItem{},
		&domain.FlashSaleCampaign{},
		&domain.FlashSaleItem{},
		&domain.FlashSaleReservation{},
		&domain.OutboxEvent{},
	)
	assert.NoError(t, err)

	mr, err := miniredis.Run()
	assert.NoError(t, err)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return db, mr, rdb
}

func TestMixedOrderSagaWorker_RegularOrder_ConfirmsAndPublishesNotification(t *testing.T) {
	db, mr, rdb := setupSagaWorkerTestDB(t)
	defer mr.Close()
	defer rdb.Close()
	orderRepo := repository.NewOrderRepository(db)
	w := &MixedOrderSagaWorker{
		db: db, orderRepo: orderRepo,
		fsRepo:      repository.NewFlashSaleRepository(db),
		outboxRepo:  repository.NewOutboxRepository(db),
		redisClient: rdb, producer: pkgKafka.NewNoopOrderKafkaProducer(),
	}
	order := domain.Order{
		OrderCode: "ORD-REGULAR-SAGA", UserID: "regular-user",
		CustomerName: "Regular User", CustomerEmail: "regular@example.com",
		CustomerPhone: "0900000000", ShippingAddress: "Hanoi",
		PaymentMethod: domain.PaymentMethodCOD, OrderStatus: domain.OrderStatusPending,
		TotalAmount: 240000,
		Items:       []domain.OrderItem{{ProductID: 701, ProductName: "Regular item", Quantity: 2, Price: 120000, Subtotal: 240000}},
	}
	assert.NoError(t, db.Create(&order).Error)
	result := pkgKafka.MixedOrderStockResultPayload{
		EventID: "regular-result", EventType: pkgKafka.EventMixedStockDeductResult,
		OrderID: order.ID, OrderCode: order.OrderCode, Success: true,
		DeductedItems: []pkgKafka.OrderItemPayload{{ProductID: 701, Quantity: 2}},
	}
	data, err := json.Marshal(result)
	assert.NoError(t, err)
	assert.NoError(t, w.processMessage(context.Background(), kafka.Message{Value: data}))
	confirmed, err := orderRepo.FindByID(order.ID)
	assert.NoError(t, err)
	assert.Equal(t, domain.OrderStatusConfirmed, confirmed.OrderStatus)
	var notification domain.OutboxEvent
	assert.NoError(t, db.Where("event_type = ? AND aggregate_id = ?", pkgKafka.EventOrderCreated, fmt.Sprintf("%d", order.ID)).First(&notification).Error)
	var payload pkgKafka.OrderCreatedPayload
	assert.NoError(t, json.Unmarshal([]byte(notification.Payload), &payload))
	assert.True(t, payload.StockHandledBySaga)
	assert.False(t, payload.IsFlashSale)
	assert.NoError(t, w.processMessage(context.Background(), kafka.Message{Value: data}))
	var count int64
	assert.NoError(t, db.Model(&domain.OutboxEvent{}).Where("event_type = ?", pkgKafka.EventOrderCreated).Count(&count).Error)
	assert.Equal(t, int64(1), count)
}

// 17.2 Test: Saga 2-Phase Cancellation
// Đơn hàng có món thường khi thất bại phải vào COMPENSATING, chỉ chuyển CANCELLED khi nhận compensate result
func TestMixedOrderSagaWorker_TwoPhaseCompensation(t *testing.T) {
	db, mr, rdb := setupSagaWorkerTestDB(t)
	defer mr.Close()

	orderRepo := repository.NewOrderRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	outboxRepo := repository.NewOutboxRepository(db)
	producer := pkgKafka.NewNoopOrderKafkaProducer()

	w := &MixedOrderSagaWorker{
		db:          db,
		orderRepo:   orderRepo,
		fsRepo:      fsRepo,
		outboxRepo:  outboxRepo,
		redisClient: rdb,
		producer:    producer,
	}

	// Tạo đơn hàng PENDING có 1 món thường và 1 món Flash Sale
	resv := domain.FlashSaleReservation{
		ID:        "resv-17",
		Status:    domain.ReservationStatusReserved,
		ExpiresAt: time.Now().Add(10 * time.Minute),
	}
	assert.NoError(t, db.Create(&resv).Error)

	order := domain.Order{
		OrderCode:       "ORD-SAGA-17",
		UserID:          "user-17",
		CustomerName:    "Tester",
		CustomerEmail:   "test@test.com",
		CustomerPhone:   "0123456789",
		ShippingAddress: "Vietnam",
		PaymentMethod:   "COD",
		OrderStatus:     domain.OrderStatusPending,
		TotalAmount:     500000,
		Items: []domain.OrderItem{
			{ProductID: 1, ProductName: "Regular Item", Quantity: 1, Price: 200000, Subtotal: 200000, IsFlashSale: false},
			{ProductID: 2, ProductName: "FS Item", Quantity: 1, Price: 300000, Subtotal: 300000, IsFlashSale: true, ReservationID: "resv-17"},
		},
	}
	assert.NoError(t, db.Create(&order).Error)

	ctx := context.Background()

	// BƯỚC 1: Xử lý thất bại trừ kho có món thường -> Order phải chuyển sang COMPENSATING, KHÔNG được CANCELLED ngay!
	regItems := []pkgKafka.OrderItemPayload{
		{ProductID: 1, ProductName: "Regular Item", Quantity: 1},
	}
	err := w.handleStockFailure(ctx, &order, "Tồn kho không đủ", regItems)
	assert.NoError(t, err)

	updatedOrder, err := orderRepo.FindByID(order.ID)
	assert.NoError(t, err)
	assert.Equal(t, domain.OrderStatusCompensating, updatedOrder.OrderStatus, "Kỳ vọng trạng thái COMPENSATING trong khi chờ Product Service bồi hoàn")

	// Outbox event bồi hoàn phải được tạo
	var outbox domain.OutboxEvent
	err = db.Where("aggregate_id = ? AND event_type = ?", fmt.Sprintf("%d", order.ID), pkgKafka.EventMixedStockCompensate).First(&outbox).Error
	assert.NoError(t, err)

	// BƯỚC 2: Nhận kết quả bồi hoàn thành công từ Product Service qua Kafka message
	compResult := pkgKafka.MixedOrderStockCompensateResultPayload{
		EventID:   "evt-comp-res-17",
		EventType: pkgKafka.EventMixedStockCompensateResult,
		OrderID:   order.ID,
		OrderCode: order.OrderCode,
		Success:   true,
		Status:    "COMPENSATED",
		Timestamp: time.Now(),
	}
	compResultBytes, _ := json.Marshal(compResult)

	err = w.processCompensateResultMessage(ctx, kafka.Message{Value: compResultBytes})
	assert.NoError(t, err)

	// BƯỚC 3: Đơn hàng chính thức chuyển sang CANCELLED
	finalOrder, err := orderRepo.FindByID(order.ID)
	assert.NoError(t, err)
	assert.Equal(t, domain.OrderStatusCancelled, finalOrder.OrderStatus, "Kỳ vọng chuyển sang CANCELLED sau khi Product Service xác nhận")
}

// 17.2 Test: Khi đơn hàng 100% Flash Sale (không có món thường nào cần bồi hoàn kho Product Service),
// Đơn hàng chuyển thẳng sang CANCELLED ngay lập tức.
func TestMixedOrderSagaWorker_PureFlashSale_DirectCancelled(t *testing.T) {
	db, mr, rdb := setupSagaWorkerTestDB(t)
	defer mr.Close()

	orderRepo := repository.NewOrderRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	outboxRepo := repository.NewOutboxRepository(db)
	producer := pkgKafka.NewNoopOrderKafkaProducer()

	w := &MixedOrderSagaWorker{
		db:          db,
		orderRepo:   orderRepo,
		fsRepo:      fsRepo,
		outboxRepo:  outboxRepo,
		redisClient: rdb,
		producer:    producer,
	}

	resv := domain.FlashSaleReservation{
		ID:        "resv-pure-1",
		Status:    domain.ReservationStatusReserved,
		ExpiresAt: time.Now().Add(10 * time.Minute),
	}
	assert.NoError(t, db.Create(&resv).Error)

	order := domain.Order{
		OrderCode:       "ORD-PURE-FS",
		UserID:          "user-pure",
		CustomerName:    "Tester",
		CustomerEmail:   "test@test.com",
		CustomerPhone:   "0123456789",
		ShippingAddress: "Vietnam",
		PaymentMethod:   "COD",
		OrderStatus:     domain.OrderStatusPending,
		TotalAmount:     300000,
		Items: []domain.OrderItem{
			{ProductID: 2, ProductName: "FS Item", Quantity: 1, Price: 300000, Subtotal: 300000, IsFlashSale: true, ReservationID: "resv-pure-1"},
		},
	}
	assert.NoError(t, db.Create(&order).Error)

	ctx := context.Background()

	// Xử lý failure với regularItemsToCompensate rỗng (nil)
	err := w.handleStockFailure(ctx, &order, "Flash Sale reservation expired", nil)
	assert.NoError(t, err)

	// Vì không có món thường, đơn phải chuyển thẳng sang CANCELLED mà không qua COMPENSATING
	updatedOrder, err := orderRepo.FindByID(order.ID)
	assert.NoError(t, err)
	assert.Equal(t, domain.OrderStatusCancelled, updatedOrder.OrderStatus)
}
