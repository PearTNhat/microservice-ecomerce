package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/redislock"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/dto"
	"ecomerce-service/services/order-service/internal/repository"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const testQuoteSecret = "test-quote-secret-key-for-unit-tests-32b"

func setupMixedTestDB(t *testing.T) (*gorm.DB, *miniredis.Miniredis, *redis.Client) {
	dbName := fmt.Sprintf("file:mixed_test_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
	assert.NoError(t, err)

	err = db.AutoMigrate(
		&domain.Order{},
		&domain.OrderItem{},
		&domain.FlashSaleCampaign{},
		&domain.FlashSaleItem{},
		&domain.FlashSaleReservation{},
		&domain.OutboxEvent{},
		&domain.Cart{},
		&domain.CartItem{},
	)
	assert.NoError(t, err)

	mr, err := miniredis.Run()
	assert.NoError(t, err)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return db, mr, rdb
}

// Đơn không có Flash Sale cũng phải dùng stock-request outbox; không publish
// order.created trước khi Product xác nhận trừ kho.
func TestOrderService_RegularCheckout_UsesDurableStockSaga(t *testing.T) {
	db, mr, rdb := setupMixedTestDB(t)
	defer mr.Close()
	defer rdb.Close()

	productID := uint(701)
	productClient := &mockProductClient{products: map[uint]*dto.ProductDetailResponse{
		productID: {ID: productID, Name: "Regular item", Price: 120000, Stock: 5},
	}}
	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	svc := NewOrderService(orderRepo, cartRepo, productClient, rdb, kafka.NewNoopOrderKafkaProducer(), testQuoteSecret)
	svc.(interface {
		SetFlashSale(*gorm.DB, domain.FlashSaleRepository)
	}).SetFlashSale(db, fsRepo)

	token, err := GenerateQuoteToken([]byte(testQuoteSecret), "regular-user", []QuoteItem{
		{ProductID: productID, Quantity: 2, QuotedPrice: 120000, PurchaseMode: "REGULAR"},
	}, 240000, 10*time.Minute)
	assert.NoError(t, err)
	result, err := svc.CreateOrder(context.Background(), "regular-user", &dto.CreateOrderRequest{
		CustomerName: "Regular User", CustomerEmail: "regular@example.com",
		CustomerPhone: "0900000000", ShippingAddress: "Hanoi",
		PaymentMethod: domain.PaymentMethodCOD, QuoteToken: token,
		Items: []dto.CreateOrderItemRequest{{ProductID: productID, Quantity: 2}},
	})
	assert.NoError(t, err)
	if !assert.NotNil(t, result) {
		return
	}
	assert.Equal(t, domain.OrderStatusPending, result.OrderStatus)
	var stockRequests []domain.OutboxEvent
	assert.NoError(t, db.Where("event_type = ?", kafka.EventMixedStockDeductRequest).Find(&stockRequests).Error)
	assert.Len(t, stockRequests, 1)
	var prematureNotifications int64
	assert.NoError(t, db.Model(&domain.OutboxEvent{}).Where("event_type = ?", kafka.EventOrderCreated).Count(&prematureNotifications).Error)
	assert.Zero(t, prematureNotifications)
}

// Point 1 & Point 2 Test: FlashSaleItemID != CampaignID, reserved_stock incremented, Outbox written
func TestOrderService_MixedCheckout_CorrectFlashSaleItemID_AndOutbox(t *testing.T) {
	db, mr, rdb := setupMixedTestDB(t)
	defer mr.Close()

	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	producer := kafka.NewNoopOrderKafkaProducer()
	mockProd := &mockProductClient{products: make(map[uint]*dto.ProductDetailResponse)}

	// Sản phẩm thường ID 500
	mockProd.products[500] = &dto.ProductDetailResponse{
		ID:    500,
		Name:  "Tai nghe Sony",
		Price: 1000000,
		Stock: 50,
	}
	// Sản phẩm Flash Sale ID 200
	mockProd.products[200] = &dto.ProductDetailResponse{
		ID:    200,
		Name:  "iPhone 16 Flash Sale",
		Price: 20000000,
		Stock: 10,
	}

	// Tạo Campaign với ID = 10, Item ID = 99 (cố ý khác nhau để kiểm tra Point 1)
	now := time.Now()
	camp := domain.FlashSaleCampaign{
		ID:        10,
		Name:      "Siêu Sale 9.9",
		StartsAt:  now.Add(-10 * time.Minute),
		EndsAt:    now.Add(2 * time.Hour),
		Status:    domain.CampaignStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
		Items: []domain.FlashSaleItem{
			{
				ID:                 99, // Item ID = 99 != Campaign ID 10
				CampaignID:         10,
				ProductID:          200,
				SalePrice:          15000000,
				AllocatedStock:     10,
				ReservedStock:      0,
				SoldStock:          0,
				MaxQuantityPerUser: 2,
				CreatedAt:          now,
				UpdatedAt:          now,
			},
		},
	}
	assert.NoError(t, db.Create(&camp).Error)

	ctx := context.Background()
	// Nạp tồn kho và cấu hình lên Redis qua PrewarmCampaignItem
	errInit := redislock.PrewarmCampaignItem(
		ctx, rdb,
		10, 200,
		10, 2, 2,
		now.Add(-10*time.Minute).Unix(),
		now.Add(2*time.Hour).Unix(),
		300,
		"ACTIVE",
	)
	assert.NoError(t, errInit)

	svc := NewOrderService(orderRepo, cartRepo, mockProd, rdb, producer, testQuoteSecret)
	if ordSvcImpl, ok := svc.(interface {
		SetFlashSale(db *gorm.DB, fsRepo domain.FlashSaleRepository)
	}); ok {
		ordSvcImpl.SetFlashSale(db, fsRepo)
	}

	campID := uint(10)
	tok, err := GenerateQuoteToken([]byte(testQuoteSecret), "user-123", []QuoteItem{
		{ProductID: 200, Quantity: 1, QuotedPrice: 15000000, IsFlashSale: true, CampaignID: &campID, PurchaseMode: "FLASH_SALE"},
		{ProductID: 500, Quantity: 2, QuotedPrice: 1000000, IsFlashSale: false, PurchaseMode: "REGULAR"},
	}, 17000000, 10*time.Minute)
	assert.NoError(t, err)

	// Đặt đơn hỗn hợp: 1 món Flash Sale (ID 200) + 1 món thường (ID 500)
	req := &dto.CreateOrderRequest{
		CustomerName:    "Nguyen Van A",
		CustomerEmail:   "a@example.com",
		CustomerPhone:   "0987654321",
		ShippingAddress: "Hanoi",
		PaymentMethod:   "COD",
		QuoteToken:      tok,
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 200, Quantity: 1},
			{ProductID: 500, Quantity: 2},
		},
	}

	orderResp, err := svc.CreateOrder(ctx, "user-123", req)
	assert.NoError(t, err)
	assert.NotNil(t, orderResp)
	assert.Equal(t, "PENDING", orderResp.OrderStatus)

	// 1. Kiểm tra Reservation trong DB: FlashSaleItemID phải là 99 (Item ID), KHÔNG PHẢI 10 (Campaign ID)
	var resv domain.FlashSaleReservation
	err = db.Where("product_id = ? AND user_id = ?", 200, "user-123").First(&resv).Error
	assert.NoError(t, err)
	assert.Equal(t, uint(99), resv.FlashSaleItemID, "FlashSaleItemID phải là ID của FlashSaleItem (99), không phải CampaignID (10)")
	assert.Equal(t, uint(10), resv.CampaignID)
	assert.Equal(t, domain.ReservationStatusReserved, resv.Status)

	// 2. Kiểm tra flash_sale_items: reserved_stock phải được tăng lên 1
	var fsItem domain.FlashSaleItem
	err = db.First(&fsItem, 99).Error
	assert.NoError(t, err)
	assert.Equal(t, 1, fsItem.ReservedStock, "reserved_stock phải được tăng lên 1 trong DB")

	// 3. Kiểm tra Outbox Event MIXED_STOCK_DEDUCT_REQUEST được lưu trong DB
	var outbox domain.OutboxEvent
	err = db.Where("topic = ?", kafka.TopicMixedOrderStockRequest).First(&outbox).Error
	assert.NoError(t, err, "Outbox event MIXED_STOCK_DEDUCT_REQUEST phải được lưu trong DB")
	assert.Contains(t, outbox.Payload, `"order_id"`)
	assert.Contains(t, outbox.Payload, `"regular_items"`)
}

// Point 7 Test: Hết suất Flash Sale lúc Checkout phải trả về lỗi (409 Conflict), không được âm thầm tính giá thường
func TestOrderService_FlashSaleOutOfStock_ReturnsConflictError(t *testing.T) {
	db, mr, rdb := setupMixedTestDB(t)
	defer mr.Close()

	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	producer := kafka.NewNoopOrderKafkaProducer()
	mockProd := &mockProductClient{products: make(map[uint]*dto.ProductDetailResponse)}

	mockProd.products[201] = &dto.ProductDetailResponse{
		ID:    201,
		Name:  "iPad Pro M4",
		Price: 30000000,
		Stock: 10,
	}

	now := time.Now()
	camp := domain.FlashSaleCampaign{
		ID:        20,
		Name:      "Mega Sale",
		StartsAt:  now.Add(-10 * time.Minute),
		EndsAt:    now.Add(2 * time.Hour),
		Status:    domain.CampaignStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
		Items: []domain.FlashSaleItem{
			{
				ID:                 101,
				CampaignID:         20,
				ProductID:          201,
				SalePrice:          25000000,
				AllocatedStock:     5,
				ReservedStock:      5, // Đã hết suất trong DB!
				SoldStock:          0,
				MaxQuantityPerUser: 1,
				CreatedAt:          now,
				UpdatedAt:          now,
			},
		},
	}
	assert.NoError(t, db.Create(&camp).Error)

	ctx := context.Background()
	// Tồn kho trên Redis = 0
	_ = rdb.Set(ctx, redislock.KeyStock(20, 201), 0, 0)
	_ = rdb.Set(ctx, redislock.KeyState(20, 201), "ACTIVE", 0)

	svc := NewOrderService(orderRepo, cartRepo, mockProd, rdb, producer, testQuoteSecret)
	if ordSvcImpl, ok := svc.(interface {
		SetFlashSale(db *gorm.DB, fsRepo domain.FlashSaleRepository)
	}); ok {
		ordSvcImpl.SetFlashSale(db, fsRepo)
	}

	campID2 := uint(20)
	tok2, err := GenerateQuoteToken([]byte(testQuoteSecret), "user-456", []QuoteItem{
		{ProductID: 201, Quantity: 1, QuotedPrice: 25000000, IsFlashSale: true, CampaignID: &campID2, PurchaseMode: "FLASH_SALE"},
	}, 25000000, 10*time.Minute)
	assert.NoError(t, err)

	req := &dto.CreateOrderRequest{
		CustomerName:    "Nguyen Van B",
		CustomerEmail:   "b@example.com",
		CustomerPhone:   "0987654322",
		ShippingAddress: "Danang",
		PaymentMethod:   "COD",
		QuoteToken:      tok2,
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 201, Quantity: 1},
		},
	}

	_, err = svc.CreateOrder(ctx, "user-456", req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "FLASH_SALE_OUT_OF_STOCK", "Phải trả về lỗi FLASH_SALE_OUT_OF_STOCK chứ không được âm thầm tính giá thường")
}

// Point 5 Test: Đơn 100% Flash Sale được xác nhận nguyên tử và ghi outbox xác nhận
func TestOrderService_100PercentFlashSale_AtomicallyConfirmed(t *testing.T) {
	db, mr, rdb := setupMixedTestDB(t)
	defer mr.Close()

	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	producer := kafka.NewNoopOrderKafkaProducer()
	mockProd := &mockProductClient{products: make(map[uint]*dto.ProductDetailResponse)}

	mockProd.products[202] = &dto.ProductDetailResponse{
		ID:    202,
		Name:  "MacBook Air M3",
		Price: 28000000,
		Stock: 5,
	}

	now := time.Now()
	camp := domain.FlashSaleCampaign{
		ID:        30,
		Name:      "Apple Festival",
		StartsAt:  now.Add(-10 * time.Minute),
		EndsAt:    now.Add(2 * time.Hour),
		Status:    domain.CampaignStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
		Items: []domain.FlashSaleItem{
			{
				ID:                 102,
				CampaignID:         30,
				ProductID:          202,
				SalePrice:          24000000,
				AllocatedStock:     5,
				ReservedStock:      0,
				SoldStock:          0,
				MaxQuantityPerUser: 1,
				CreatedAt:          now,
				UpdatedAt:          now,
			},
		},
	}
	assert.NoError(t, db.Create(&camp).Error)

	ctx := context.Background()
	errInit := redislock.PrewarmCampaignItem(
		ctx, rdb,
		30, 202,
		5, 1, 1,
		now.Add(-10*time.Minute).Unix(),
		now.Add(2*time.Hour).Unix(),
		300,
		"ACTIVE",
	)
	assert.NoError(t, errInit)

	svc := NewOrderService(orderRepo, cartRepo, mockProd, rdb, producer, testQuoteSecret)
	if ordSvcImpl, ok := svc.(interface {
		SetFlashSale(db *gorm.DB, fsRepo domain.FlashSaleRepository)
	}); ok {
		ordSvcImpl.SetFlashSale(db, fsRepo)
	}

	campID3 := uint(30)
	tok3, err := GenerateQuoteToken([]byte(testQuoteSecret), "user-789", []QuoteItem{
		{ProductID: 202, Quantity: 1, QuotedPrice: 24000000, IsFlashSale: true, CampaignID: &campID3, PurchaseMode: "FLASH_SALE"},
	}, 24000000, 10*time.Minute)
	assert.NoError(t, err)

	req := &dto.CreateOrderRequest{
		CustomerName:    "Nguyen Van C",
		CustomerEmail:   "c@example.com",
		CustomerPhone:   "0987654323",
		ShippingAddress: "HCM",
		PaymentMethod:   "COD",
		QuoteToken:      tok3,
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 202, Quantity: 1},
		},
	}

	orderResp, err := svc.CreateOrder(ctx, "user-789", req)
	assert.NoError(t, err)
	assert.NotNil(t, orderResp)
	assert.Equal(t, "CONFIRMED", orderResp.OrderStatus, "Đơn 100% Flash Sale phải được CONFIRMED ngay lập tức")

	// Kiểm tra outbox FLASH_SALE_ORDER_CONFIRMED
	var outboxFS domain.OutboxEvent
	err = db.Where("topic = ?", kafka.TopicFlashSaleConfirmed).First(&outboxFS).Error
	assert.NoError(t, err, "Phải ghi Outbox FLASH_SALE_ORDER_CONFIRMED")

	// Kiểm tra outbox ORDER_CREATED
	var outboxOrder domain.OutboxEvent
	err = db.Where("topic = ?", kafka.TopicOrderEvents).First(&outboxOrder).Error
	assert.NoError(t, err, "Phải ghi Outbox ORDER_CREATED")
}

// 16.3 Test: Order DB Invariant bảo vệ trần phân bổ khi Redis bị lệch số
func TestOrderService_AllocationInvariant_OrderDBReject(t *testing.T) {
	db, mr, rdb := setupMixedTestDB(t)
	defer mr.Close()

	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	producer := kafka.NewNoopOrderKafkaProducer()
	mockProd := &mockProductClient{products: make(map[uint]*dto.ProductDetailResponse)}

	mockProd.products[203] = &dto.ProductDetailResponse{
		ID:    203,
		Name:  "iPad Pro Flash Sale",
		Price: 25000000,
		Stock: 10,
	}

	now := time.Now()
	// AllocatedStock = 1, SoldStock = 1 -> Đã hết sạch chỉ tiêu trong DB!
	camp := domain.FlashSaleCampaign{
		ID:        40,
		Name:      "Limited Drop",
		StartsAt:  now.Add(-10 * time.Minute),
		EndsAt:    now.Add(2 * time.Hour),
		Status:    domain.CampaignStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
		Items: []domain.FlashSaleItem{
			{
				ID:                 103,
				CampaignID:         40,
				ProductID:          203,
				SalePrice:          20000000,
				AllocatedStock:     1,
				ReservedStock:      0,
				SoldStock:          1, // Đã bán 1/1
				MaxQuantityPerUser: 5,
				CreatedAt:          now,
				UpdatedAt:          now,
			},
		},
	}
	assert.NoError(t, db.Create(&camp).Error)

	ctx := context.Background()
	// Giả lập tình huống Redis bị lệch (ví dụ cache còn báo 5 suất)
	errInit := redislock.PrewarmCampaignItem(
		ctx, rdb,
		40, 203,
		5, 5, 5,
		now.Add(-10*time.Minute).Unix(),
		now.Add(2*time.Hour).Unix(),
		300,
		"ACTIVE",
	)
	assert.NoError(t, errInit)

	svc := NewOrderService(orderRepo, cartRepo, mockProd, rdb, producer, testQuoteSecret)
	if ordSvcImpl, ok := svc.(interface {
		SetFlashSale(db *gorm.DB, fsRepo domain.FlashSaleRepository)
	}); ok {
		ordSvcImpl.SetFlashSale(db, fsRepo)
	}

	campID4 := uint(40)
	tok4, err := GenerateQuoteToken([]byte(testQuoteSecret), "user-999", []QuoteItem{
		{ProductID: 203, Quantity: 1, QuotedPrice: 20000000, IsFlashSale: true, CampaignID: &campID4, PurchaseMode: "FLASH_SALE"},
	}, 20000000, 10*time.Minute)
	assert.NoError(t, err)

	req := &dto.CreateOrderRequest{
		CustomerName:    "Nguyen Van D",
		CustomerEmail:   "d@example.com",
		CustomerPhone:   "0987654324",
		ShippingAddress: "Da Nang",
		PaymentMethod:   "COD",
		QuoteToken:      tok4,
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 203, Quantity: 1},
		},
	}

	// Đặt hàng phải thất bại do vi phạm invariant trong Order DB!
	_, err = svc.CreateOrder(ctx, "user-999", req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ALLOCATION_EXCEEDED", "Order DB phải chặn đứng vượt trần phân bổ")

	// Không có đơn hàng nào được tạo trong DB
	var orderCount int64
	db.Model(&domain.Order{}).Count(&orderCount)
	assert.Equal(t, int64(0), orderCount, "Không được lưu bất kỳ đơn hàng nào khi DB rollback")
}

// 16.5 Test: Flash Sale hết hạn ngay trước khi ấn checkout -> Chặn tạo đơn giá thường, trả về 409 Re-quote
func TestOrderService_ReQuote_FlashSaleExpiredBeforeCheckout(t *testing.T) {
	db, mr, rdb := setupMixedTestDB(t)
	defer mr.Close()

	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	producer := kafka.NewNoopOrderKafkaProducer()
	mockProd := &mockProductClient{products: make(map[uint]*dto.ProductDetailResponse)}

	mockProd.products[204] = &dto.ProductDetailResponse{
		ID:    204,
		Name:  "Smart Watch Sale",
		Price: 5000000, // Giá thường 5 triệu
		Stock: 20,
	}

	userID := "user-requote-test"

	// Khách hàng nhận được báo giá khi chiến dịch còn hiệu lực (giá sale 2 triệu)
	campID := uint(50)
	quoteItems := []QuoteItem{
		{
			ProductID:   204,
			Quantity:    1,
			QuotedPrice: 2000000, // Khách thấy giá 2 triệu
			IsFlashSale: true,
			CampaignID:  &campID,
		},
	}
	quoteToken, err := GenerateQuoteToken([]byte(testQuoteSecret), userID, quoteItems, 2000000, 10*time.Minute)
	assert.NoError(t, err)

	// Nhưng vào thời điểm submit đơn hàng, chiến dịch đã HẾT HẠN (EndsAt trong quá khứ)
	past := time.Now().Add(-1 * time.Hour)
	camp := domain.FlashSaleCampaign{
		ID:        50,
		Name:      "Expired Campaign",
		StartsAt:  past.Add(-2 * time.Hour),
		EndsAt:    past, // Đã kết thúc!
		Status:    domain.CampaignStatusEnded,
		CreatedAt: past,
		UpdatedAt: past,
		Items: []domain.FlashSaleItem{
			{
				ID:                 104,
				CampaignID:         50,
				ProductID:          204,
				SalePrice:          2000000,
				AllocatedStock:     10,
				ReservedStock:      0,
				SoldStock:          10,
				MaxQuantityPerUser: 1,
			},
		},
	}
	assert.NoError(t, db.Create(&camp).Error)

	svc := NewOrderService(orderRepo, cartRepo, mockProd, rdb, producer, testQuoteSecret)
	if ordSvcImpl, ok := svc.(interface {
		SetFlashSale(db *gorm.DB, fsRepo domain.FlashSaleRepository)
	}); ok {
		ordSvcImpl.SetFlashSale(db, fsRepo)
	}

	req := &dto.CreateOrderRequest{
		CustomerName:    "Nguyen Van E",
		CustomerEmail:   "e@example.com",
		CustomerPhone:   "0987654325",
		ShippingAddress: "Can Tho",
		PaymentMethod:   "COD",
		QuoteToken:      quoteToken,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 204, Quantity: 1},
		},
	}

	// Submit đơn hàng: Hệ thống TUYỆT ĐỐI KHÔNG được âm thầm tạo đơn với giá 5 triệu!
	_, err = svc.CreateOrder(context.Background(), userID, req)
	assert.Error(t, err)

	// Phải trả về lỗi PriceConflictError chứa token mới và thông tin chênh lệch
	conflictErr, ok := err.(*PriceConflictError)
	assert.True(t, ok, "Kỳ vọng trả về PriceConflictError khi Flash Sale đã hết hạn")
	assert.Equal(t, "PRICE_CHANGED", conflictErr.ErrorCode)
	assert.Equal(t, 1, len(conflictErr.Response.AffectedItems))
	assert.Equal(t, "FLASH_SALE_EXPIRED", conflictErr.Response.AffectedItems[0].Reason)
	assert.Equal(t, float64(2000000), conflictErr.Response.AffectedItems[0].QuotedPrice)
	assert.Equal(t, float64(5000000), conflictErr.Response.AffectedItems[0].UpdatedPrice)
	assert.NotEmpty(t, conflictErr.Response.NewQuoteToken, "Phải sinh new_quote_token mới để khách xác nhận")

	// Không có đơn hàng nào được tạo
	var orderCount int64
	db.Model(&domain.Order{}).Count(&orderCount)
	assert.Equal(t, int64(0), orderCount)
}

// 17.1 Test: Phá vỡ vòng lặp 409 vô tận khi người dùng chấp nhận mua với giá thường (PurchaseMode == "REGULAR")
// Kể cả khi Flash Sale campaign vẫn đang ACTIVE nhưng hết suất, gửi Q2 (PurchaseMode: REGULAR) phải thành công!
func TestOrderService_Break409Loop_WhenUserAcceptsRegularPrice(t *testing.T) {
	db, mr, rdb := setupMixedTestDB(t)
	defer mr.Close()

	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	producer := kafka.NewNoopOrderKafkaProducer()
	mockProd := &mockProductClient{products: make(map[uint]*dto.ProductDetailResponse)}

	productID := uint(205)
	regularPrice := float64(6000000)
	salePrice := float64(3000000)

	mockProd.products[productID] = &dto.ProductDetailResponse{
		ID:    productID,
		Name:  "Smart Watch Vòng 3",
		Price: regularPrice,
		Stock: 50,
	}

	// Campaign ACTIVE nhưng hết sạch tồn kho (Allocated: 10, Sold: 10, Remaining: 0)
	now := time.Now()
	camp := domain.FlashSaleCampaign{
		ID:        60,
		Name:      "Campaign Vòng 3",
		StartsAt:  now.Add(-1 * time.Hour),
		EndsAt:    now.Add(2 * time.Hour),
		Status:    domain.CampaignStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
		Items: []domain.FlashSaleItem{
			{
				ID:                 105,
				CampaignID:         60,
				ProductID:          productID,
				SalePrice:          salePrice,
				AllocatedStock:     10,
				ReservedStock:      0,
				SoldStock:          10, // ĐÃ BÁN HẾT!
				MaxQuantityPerUser: 2,
			},
		},
	}
	assert.NoError(t, db.Create(&camp).Error)

	ctx := context.Background()
	_ = redislock.PrewarmCampaignItem(
		ctx, rdb,
		60, productID,
		0, 2, 2,
		now.Add(-1*time.Hour).Unix(),
		now.Add(2*time.Hour).Unix(),
		300,
		"ACTIVE",
	)

	svc := NewOrderService(orderRepo, cartRepo, mockProd, rdb, producer, testQuoteSecret)
	if ordSvcImpl, ok := svc.(interface {
		SetFlashSale(db *gorm.DB, fsRepo domain.FlashSaleRepository)
	}); ok {
		ordSvcImpl.SetFlashSale(db, fsRepo)
	}

	userID := "user-vong-3"

	// BƯỚC 1: Client mang QuoteToken Q1 (chế độ FLASH_SALE với giá sale 3 triệu) lên checkout
	q1Items := []QuoteItem{
		{ProductID: productID, Quantity: 1, QuotedPrice: salePrice, IsFlashSale: true, PurchaseMode: "FLASH_SALE"},
	}
	tokenQ1, err := GenerateQuoteToken([]byte(testQuoteSecret), userID, q1Items, salePrice, 10*time.Minute)
	assert.NoError(t, err)

	reqQ1 := &dto.CreateOrderRequest{
		CustomerName:    "Le Van F",
		CustomerEmail:   "f@example.com",
		CustomerPhone:   "0987654326",
		ShippingAddress: "Da Nang",
		PaymentMethod:   "COD",
		QuoteToken:      tokenQ1,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: productID, Quantity: 1},
		},
	}

	// Submit Q1: Hệ thống phải reject với 409 FLASH_SALE_OUT_OF_STOCK và phát Q2 với PurchaseMode == "REGULAR"
	_, err = svc.CreateOrder(ctx, userID, reqQ1)
	assert.Error(t, err)
	conflictErr, ok := err.(*PriceConflictError)
	assert.True(t, ok)
	assert.Equal(t, "PRICE_CHANGED", conflictErr.ErrorCode)
	assert.NotEmpty(t, conflictErr.Response.NewQuoteToken)
	tokenQ2 := conflictErr.Response.NewQuoteToken

	// Xác minh payload của Q2 đã mang PurchaseMode == "REGULAR"
	verifiedQ2, err := VerifyQuoteToken([]byte(testQuoteSecret), tokenQ2, userID)
	assert.NoError(t, err)
	assert.Equal(t, "REGULAR", verifiedQ2.Items[0].PurchaseMode)
	assert.Equal(t, regularPrice, verifiedQ2.Items[0].QuotedPrice)

	// BƯỚC 2: Người dùng chấp nhận mua giá thường và gửi lại đơn với token Q2!
	reqQ2 := &dto.CreateOrderRequest{
		CustomerName:    "Le Van F",
		CustomerEmail:   "f@example.com",
		CustomerPhone:   "0987654326",
		ShippingAddress: "Da Nang",
		PaymentMethod:   "COD",
		QuoteToken:      tokenQ2,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: productID, Quantity: 1},
		},
	}

	// 17.1: TUYỆT ĐỐI KHÔNG ĐƯỢC BỊ 409 NỮA! Đơn hàng phải tạo thành công ngay lập tức với giá thường!
	orderRes, err := svc.CreateOrder(ctx, userID, reqQ2)
	assert.NoError(t, err, "Kỳ vọng đặt hàng thành công khi gửi Q2 với PurchaseMode REGULAR, không bị vòng lặp 409")
	assert.NotNil(t, orderRes)
	assert.Equal(t, regularPrice, orderRes.TotalAmount)
	assert.False(t, orderRes.Items[0].IsFlashSale)
}

// 17.1 Test: Strict Basket Validation - Client sửa đổi số lượng hoặc tráo sản phẩm sẽ bị từ chối
func TestOrderService_StrictBasketValidation(t *testing.T) {
	db, mr, rdb := setupMixedTestDB(t)
	defer mr.Close()

	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	producer := kafka.NewNoopOrderKafkaProducer()
	mockProd := &mockProductClient{products: make(map[uint]*dto.ProductDetailResponse)}

	mockProd.products[301] = &dto.ProductDetailResponse{ID: 301, Name: "Item A", Price: 100000, Stock: 10}
	mockProd.products[302] = &dto.ProductDetailResponse{ID: 302, Name: "Item B", Price: 200000, Stock: 10}

	svc := NewOrderService(orderRepo, cartRepo, mockProd, rdb, producer, testQuoteSecret)
	userID := "user-basket-tamper"

	// Token báo giá chỉ chứa 1 cái món 301
	qItems := []QuoteItem{
		{ProductID: 301, Quantity: 1, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
	}
	token, err := GenerateQuoteToken([]byte(testQuoteSecret), userID, qItems, 100000, 10*time.Minute)
	assert.NoError(t, err)

	// Case 1: Client gửi tráo sang món 302
	reqTamperProd := &dto.CreateOrderRequest{
		CustomerName:    "Hacker",
		CustomerEmail:   "hacker@example.com",
		CustomerPhone:   "0987654321",
		ShippingAddress: "Hanoi",
		PaymentMethod:   "COD",
		QuoteToken:      token,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 302, Quantity: 1}, // Tráo sản phẩm!
		},
	}
	_, err = svc.CreateOrder(context.Background(), userID, reqTamperProd)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "INVALID_QUOTE_TOKEN")

	// Case 2: Client gửi tăng số lượng lên 2 cái
	reqTamperQty := &dto.CreateOrderRequest{
		CustomerName:    "Hacker",
		CustomerEmail:   "hacker@example.com",
		CustomerPhone:   "0987654321",
		ShippingAddress: "Hanoi",
		PaymentMethod:   "COD",
		QuoteToken:      token,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 301, Quantity: 2}, // Tăng số lượng!
		},
	}
	_, err = svc.CreateOrder(context.Background(), userID, reqTamperQty)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "INVALID_QUOTE_TOKEN")
}

// 17.1 Test: GetBasketQuote báo giá chính xác giỏ hàng kèm token đã ký
func TestOrderService_GetBasketQuote(t *testing.T) {
	db, mr, rdb := setupMixedTestDB(t)
	defer mr.Close()

	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	producer := kafka.NewNoopOrderKafkaProducer()
	mockProd := &mockProductClient{products: make(map[uint]*dto.ProductDetailResponse)}

	mockProd.products[401] = &dto.ProductDetailResponse{ID: 401, Name: "Chuột không dây", Price: 300000, Stock: 20}
	mockProd.products[402] = &dto.ProductDetailResponse{ID: 402, Name: "Bàn phím cơ Sale", Price: 1000000, Stock: 10}

	// Tạo Campaign cho sản phẩm 402
	now := time.Now()
	camp := domain.FlashSaleCampaign{
		ID:        70,
		Name:      "Phụ kiện Sale",
		StartsAt:  now.Add(-10 * time.Minute),
		EndsAt:    now.Add(2 * time.Hour),
		Status:    domain.CampaignStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
		Items: []domain.FlashSaleItem{
			{
				ID:                 106,
				CampaignID:         70,
				ProductID:          402,
				SalePrice:          600000,
				AllocatedStock:     10,
				ReservedStock:      0,
				SoldStock:          0,
				MaxQuantityPerUser: 1,
			},
		},
	}
	assert.NoError(t, db.Create(&camp).Error)

	ctx := context.Background()
	_ = redislock.PrewarmCampaignItem(
		ctx, rdb,
		70, 402,
		10, 2, 1,
		now.Add(-10*time.Minute).Unix(),
		now.Add(2*time.Hour).Unix(),
		300,
		"ACTIVE",
	)

	svc := NewOrderService(orderRepo, cartRepo, mockProd, rdb, producer, testQuoteSecret)
	if ordSvcImpl, ok := svc.(interface {
		SetFlashSale(db *gorm.DB, fsRepo domain.FlashSaleRepository)
	}); ok {
		ordSvcImpl.SetFlashSale(db, fsRepo)
	}

	userID := "user-quote-test"

	quoteReq := &dto.BasketQuoteRequest{
		Items: []dto.BasketQuoteItem{
			{ProductID: 401, Quantity: 2}, // Thường: 300k * 2 = 600k
			{ProductID: 402, Quantity: 1}, // Flash Sale: 600k * 1 = 600k
		},
	}

	quoteRes, err := svc.GetBasketQuote(ctx, userID, quoteReq)
	assert.NoError(t, err)
	assert.NotNil(t, quoteRes)
	assert.Equal(t, float64(1200000), quoteRes.Total)
	assert.Len(t, quoteRes.Items, 2)

	// Item 401: REGULAR
	assert.Equal(t, uint(401), quoteRes.Items[0].ProductID)
	assert.Equal(t, "REGULAR", quoteRes.Items[0].PurchaseMode)
	assert.False(t, quoteRes.Items[0].IsFlashSale)

	// Item 402: FLASH_SALE
	assert.Equal(t, uint(402), quoteRes.Items[1].ProductID)
	assert.Equal(t, "FLASH_SALE", quoteRes.Items[1].PurchaseMode)
	assert.True(t, quoteRes.Items[1].IsFlashSale)
	assert.Equal(t, float64(600000), quoteRes.Items[1].UnitPrice)

	// Token xác thực hợp lệ
	payload, err := VerifyQuoteToken([]byte(testQuoteSecret), quoteRes.QuoteToken, userID)
	assert.NoError(t, err)
	assert.Equal(t, float64(1200000), payload.Total)
}

// 18.1 Test: Giá Flash Sale bị tăng trong khi campaign vẫn ACTIVE -> Trả về 409 Conflict thay vì âm thầm chém giá
func TestOrderService_SalePriceIncrease_ReturnsConflict409(t *testing.T) {
	db, mr, rdb := setupMixedTestDB(t)
	defer mr.Close()

	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	producer := kafka.NewNoopOrderKafkaProducer()
	mockProd := &mockProductClient{products: make(map[uint]*dto.ProductDetailResponse)}

	mockProd.products[700] = &dto.ProductDetailResponse{
		ID:    700,
		Name:  "Áo Hoodie Limited",
		Price: 1000000,
		Stock: 50,
	}

	now := time.Now()
	// Campaign ACTIVE nhưng giá Flash Sale thực tế hiện tại là 700,000 (đã tăng từ 500,000)
	camp := domain.FlashSaleCampaign{
		ID:        80,
		Name:      "Fashion Sale",
		StartsAt:  now.Add(-10 * time.Minute),
		EndsAt:    now.Add(2 * time.Hour),
		Status:    domain.CampaignStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
		Items: []domain.FlashSaleItem{
			{
				ID:                 180,
				CampaignID:         80,
				ProductID:          700,
				SalePrice:          700000, // Giá đã bị tăng!
				AllocatedStock:     50,
				ReservedStock:      0,
				SoldStock:          0,
				MaxQuantityPerUser: 5,
				CreatedAt:          now,
				UpdatedAt:          now,
			},
		},
	}
	assert.NoError(t, db.Create(&camp).Error)

	ctx := context.Background()
	_ = redislock.PrewarmCampaignItem(
		ctx, rdb,
		80, 700,
		50, 5, 5,
		now.Add(-10*time.Minute).Unix(),
		now.Add(2*time.Hour).Unix(),
		300,
		"ACTIVE",
	)

	svc := NewOrderService(orderRepo, cartRepo, mockProd, rdb, producer, testQuoteSecret)
	if ordSvcImpl, ok := svc.(interface {
		SetFlashSale(db *gorm.DB, fsRepo domain.FlashSaleRepository)
	}); ok {
		ordSvcImpl.SetFlashSale(db, fsRepo)
	}

	userID := "user-fashion-lover"
	campID := uint(80)

	// Khách được báo giá cũ là 500,000
	tokenOldPrice, err := GenerateQuoteToken([]byte(testQuoteSecret), userID, []QuoteItem{
		{ProductID: 700, Quantity: 1, QuotedPrice: 500000, IsFlashSale: true, CampaignID: &campID, PurchaseMode: "FLASH_SALE"},
	}, 500000, 10*time.Minute)
	assert.NoError(t, err)

	req := &dto.CreateOrderRequest{
		CustomerName:    "Nguyen Thi Fashion",
		CustomerEmail:   "fashion@example.com",
		CustomerPhone:   "0987654329",
		ShippingAddress: "Da Nang",
		PaymentMethod:   "COD",
		QuoteToken:      tokenOldPrice,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 700, Quantity: 1},
		},
	}

	_, err = svc.CreateOrder(ctx, userID, req)
	assert.Error(t, err)

	conflictErr, ok := err.(*PriceConflictError)
	assert.True(t, ok, "Phải trả về PriceConflictError khi giá sale tăng")
	assert.Equal(t, "PRICE_CHANGED", conflictErr.ErrorCode)
	assert.Len(t, conflictErr.Response.AffectedItems, 1)
	assert.Equal(t, "PRICE_CHANGED", conflictErr.Response.AffectedItems[0].Reason)
	assert.Equal(t, float64(500000), conflictErr.Response.AffectedItems[0].QuotedPrice)
	assert.Equal(t, float64(700000), conflictErr.Response.AffectedItems[0].UpdatedPrice)
	assert.Equal(t, float64(700000), conflictErr.Response.NewTotal)
	assert.NotEmpty(t, conflictErr.Response.NewQuoteToken)
}

// 18.2 Test: Thiếu quote_token khi tạo đơn phải bị chặn ngay lập tức (QUOTE_REQUIRED)
func TestOrderService_MissingQuoteToken_Rejected(t *testing.T) {
	db, mr, rdb := setupMixedTestDB(t)
	defer mr.Close()

	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	producer := kafka.NewNoopOrderKafkaProducer()
	mockProd := &mockProductClient{products: make(map[uint]*dto.ProductDetailResponse)}

	svc := NewOrderService(orderRepo, cartRepo, mockProd, rdb, producer, testQuoteSecret)
	req := &dto.CreateOrderRequest{
		CustomerName:    "Khach Vang Lai",
		CustomerEmail:   "vanglai@example.com",
		CustomerPhone:   "0909090909",
		ShippingAddress: "Ha Noi",
		PaymentMethod:   "COD",
		QuoteToken:      "", // Rỗng!
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 10, Quantity: 1},
		},
	}

	_, err := svc.CreateOrder(context.Background(), "user-vanglai", req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "QUOTE_REQUIRED")
}

// 18.3 Test: UserID không khớp trong quote token bị từ chối và secret ngắn bị chặn
func TestOrderService_TamperedUserID_AndSecretSecurity(t *testing.T) {
	db, mr, rdb := setupMixedTestDB(t)
	defer mr.Close()

	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	producer := kafka.NewNoopOrderKafkaProducer()
	mockProd := &mockProductClient{products: make(map[uint]*dto.ProductDetailResponse)}

	// 1. Secret quá ngắn (< 16 bytes) phải bị từ chối
	_, err := GenerateQuoteToken([]byte("short-sec"), "user-A", nil, 100, 10*time.Minute)
	assert.ErrorIs(t, err, ErrSecretInsecure)

	_, err = VerifyQuoteToken([]byte("short-sec"), "dummy.dummy", "user-A")
	assert.ErrorIs(t, err, ErrSecretInsecure)

	// 2. Token của User-A đem cho User-B dùng phải bị chặn
	tokUserA, err := GenerateQuoteToken([]byte(testQuoteSecret), "user-A", []QuoteItem{
		{ProductID: 10, Quantity: 1, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
	}, 100000, 10*time.Minute)
	assert.NoError(t, err)

	svc := NewOrderService(orderRepo, cartRepo, mockProd, rdb, producer, testQuoteSecret)
	reqUserB := &dto.CreateOrderRequest{
		CustomerName:    "User B",
		CustomerEmail:   "b@example.com",
		CustomerPhone:   "0911223344",
		ShippingAddress: "Hue",
		PaymentMethod:   "COD",
		QuoteToken:      tokUserA, // Dùng lén token của user-A
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 10, Quantity: 1},
		},
	}

	_, err = svc.CreateOrder(context.Background(), "user-B", reqUserB)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "INVALID_QUOTE_TOKEN")
}
