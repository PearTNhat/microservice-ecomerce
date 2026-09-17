package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"ecomerce-service/pkg/kafka"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/dto"
	"ecomerce-service/services/order-service/internal/repository"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type mockProductClient struct {
	products map[uint]*dto.ProductDetailResponse
}

func (m *mockProductClient) GetProduct(ctx context.Context, productID uint) (*dto.ProductDetailResponse, error) {
	p, ok := m.products[productID]
	if !ok {
		return nil, errors.New("product not found")
	}
	return p, nil
}

func (m *mockProductClient) AllocateFlashSaleStock(ctx context.Context, campaignID, productID uint, requestID string, quantity int) error {
	return nil
}

func (m *mockProductClient) ReleaseFlashSaleStock(ctx context.Context, campaignID, productID uint, requestID string) error {
	return nil
}

func (m *mockProductClient) GetStockAllocation(ctx context.Context, campaignID, productID uint) (*dto.StockAllocationResponse, error) {
	return &dto.StockAllocationResponse{}, nil
}

func helperQuoteToken(t *testing.T, userID string, items []QuoteItem, total float64) string {
	tok, err := GenerateQuoteToken([]byte(testQuoteSecret), userID, items, total, 10*time.Minute)
	if err != nil {
		t.Fatalf("GenerateQuoteToken failed: %v", err)
	}
	return tok
}

func setupOrderServiceTestDB(t *testing.T) (*gorm.DB, *miniredis.Miniredis, *redis.Client, OrderService, domain.CartRepository, domain.OrderRepository) {
	dbName := fmt.Sprintf("file:ordersvc_test_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
	if err != nil {
		t.Fatalf("Không thể khởi tạo sqlite: %v", err)
	}
	_ = db.AutoMigrate(
		&domain.Order{},
		&domain.OrderItem{},
		&domain.FlashSaleCampaign{},
		&domain.FlashSaleItem{},
		&domain.FlashSaleReservation{},
		&domain.OutboxEvent{},
		&domain.Cart{},
		&domain.CartItem{},
		&domain.CheckoutAttempt{},
	)
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Không thể khởi chạy miniredis: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	producer := kafka.NewNoopOrderKafkaProducer()

	svc := NewOrderService(orderRepo, cartRepo, nil, rdb, producer, testQuoteSecret)
	svc.(interface {
		SetFlashSale(*gorm.DB, domain.FlashSaleRepository)
	}).SetFlashSale(db, fsRepo)

	return db, mr, rdb, svc, cartRepo, orderRepo
}

func TestOrderService_CreateOrder_DirectAndFromCart(t *testing.T) {
	_, mr, rdb, svc, cartRepo, _ := setupOrderServiceTestDB(t)
	defer mr.Close()
	defer rdb.Close()

	ctx := context.Background()
	userID := "user-abc"

	// 1. Test tạo đơn trực tiếp thành công
	createReq := &dto.CreateOrderRequest{
		CustomerName:    "Lê Tuấn Nhật",
		CustomerEmail:   "nhat@example.com",
		CustomerPhone:   "0987654321",
		ShippingAddress: "123 Đường Công Nghệ, TP.HCM",
		PaymentMethod:   "COD",
		QuoteToken: helperQuoteToken(t, userID, []QuoteItem{
			{ProductID: 100, Quantity: 2, QuotedPrice: 50000, PurchaseMode: "REGULAR"},
		}, 100000),
		FromCart: false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 100, Quantity: 2},
		},
	}

	orderResp, err := svc.CreateOrder(ctx, userID, createReq)
	if err != nil {
		t.Fatalf("Lỗi CreateOrder trực tiếp: %v", err)
	}
	if orderResp.OrderCode == "" || len(orderResp.Items) != 1 {
		t.Errorf("Tạo đơn hàng không hợp lệ: %+v", orderResp)
	}
	if orderResp.OrderStatus != domain.OrderStatusPending {
		t.Errorf("Kỳ vọng order_status PENDING cho đơn regular outbox nhưng nhận %s", orderResp.OrderStatus)
	}

	// 2. Test thiếu / sai QuoteToken
	badQuoteReq := &dto.CreateOrderRequest{
		CustomerName:    "Khách Sai Quote",
		CustomerEmail:   "si@example.com",
		CustomerPhone:   "0987654321",
		ShippingAddress: "Kho Tổng",
		PaymentMethod:   "COD",
		QuoteToken:      "invalid.quote.token",
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 100, Quantity: 1},
		},
	}
	_, err = svc.CreateOrder(ctx, userID, badQuoteReq)
	if err == nil {
		t.Error("Kỳ vọng lỗi khi quote token sai nhưng lại thành công")
	}

	// 3. Test tạo đơn từ Giỏ hàng
	prodClient := &mockProductClient{products: map[uint]*dto.ProductDetailResponse{
		200: {ID: 200, Name: "Sản phẩm giỏ hàng", Price: 30000, Stock: 50},
	}}
	cartSvc := NewCartService(cartRepo, prodClient)
	_, err = cartSvc.AddToCart(ctx, userID, &dto.AddToCartRequest{
		ProductID: 200,
		Quantity:  3,
	})
	if err != nil {
		t.Fatalf("Lỗi AddToCart: %v", err)
	}

	cartOrderReq := &dto.CreateOrderRequest{
		CustomerName:    "Lê Tuấn Nhật",
		CustomerEmail:   "nhat@example.com",
		CustomerPhone:   "0987654321",
		ShippingAddress: "123 Đường Công Nghệ, TP.HCM",
		PaymentMethod:   "COD",
		QuoteToken: helperQuoteToken(t, userID, []QuoteItem{
			{ProductID: 200, Quantity: 3, QuotedPrice: 30000, PurchaseMode: "REGULAR"},
		}, 90000),
		FromCart: true,
	}

	cartOrderResp, err := svc.CreateOrder(ctx, userID, cartOrderReq)
	if err != nil {
		t.Fatalf("Lỗi CreateOrder từ giỏ hàng: %v", err)
	}
	if cartOrderResp.TotalAmount != 90000 {
		t.Errorf("Tổng tiền không hợp lệ: %.2f", cartOrderResp.TotalAmount)
	}

	// Kiểm tra giỏ hàng sau khi đặt xong đã được dọn sạch
	cart, _ := cartSvc.GetCart(ctx, userID)
	if len(cart.Items) != 0 {
		t.Errorf("Kỳ vọng giỏ hàng rỗng sau khi checkout nhưng còn %d món", len(cart.Items))
	}
}

func TestOrderService_GetOrderAndStatus(t *testing.T) {
	_, mr, rdb, svc, _, _ := setupOrderServiceTestDB(t)
	defer mr.Close()
	defer rdb.Close()

	ctx := context.Background()
	userID := "user-xyz"

	// Tạo 1 đơn hàng
	orderReq := &dto.CreateOrderRequest{
		CustomerName:    "Người Mua",
		CustomerEmail:   "buyer@example.com",
		CustomerPhone:   "0123456789",
		ShippingAddress: "Địa chỉ nhận",
		PaymentMethod:   "COD",
		QuoteToken: helperQuoteToken(t, userID, []QuoteItem{
			{ProductID: 1, Quantity: 1, QuotedPrice: 50000, PurchaseMode: "REGULAR"},
		}, 50000),
		FromCart: false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 1},
		},
	}
	created, err := svc.CreateOrder(ctx, userID, orderReq)
	if err != nil {
		t.Fatalf("Lỗi tạo đơn hàng: %v", err)
	}

	// 1. GetOrderByID - Chính chủ xem -> OK
	order, err := svc.GetOrderByID(ctx, created.ID, userID, "CUSTOMER")
	if err != nil {
		t.Fatalf("Lỗi GetOrderByID: %v", err)
	}
	if order.OrderCode != created.OrderCode {
		t.Errorf("Mã đơn hàng không khớp: %s != %s", order.OrderCode, created.OrderCode)
	}

	// 2. GetOrderByID - Người lạ xem -> Lỗi quyền
	_, err = svc.GetOrderByID(ctx, created.ID, "user-stranger", "CUSTOMER")
	if err == nil {
		t.Error("Kỳ vọng lỗi không có quyền xem đơn nhưng lại thành công")
	}

	// 3. GetOrderByID - Admin xem -> OK
	_, err = svc.GetOrderByID(ctx, created.ID, "user-admin", "ADMIN")
	if err != nil {
		t.Errorf("Admin phải được phép xem mọi đơn hàng: %v", err)
	}

	// 4. UpdateOrderStatus
	err = svc.UpdateOrderStatus(ctx, created.ID, &dto.UpdateOrderStatusRequest{
		Status: domain.OrderStatusConfirmed,
	})
	if err != nil {
		t.Fatalf("Lỗi UpdateOrderStatus: %v", err)
	}

	updated, _ := svc.GetOrderByID(ctx, created.ID, userID, "CUSTOMER")
	if updated.OrderStatus != domain.OrderStatusConfirmed {
		t.Errorf("Kỳ vọng trạng thái CONFIRMED nhưng nhận %s", updated.OrderStatus)
	}
}

func TestOrderService_ReplayFirst_EvenIfQuoteExpired(t *testing.T) {
	_, mr, rdb, svc, _, _ := setupOrderServiceTestDB(t)
	defer mr.Close()
	defer rdb.Close()

	ctx := context.Background()
	userID := "user-replay"

	// 1. Tạo đơn lần đầu thành công với quote token có TTL 1 giây
	quoteTok, err := GenerateQuoteToken([]byte(testQuoteSecret), userID, []QuoteItem{
		{ProductID: 1, Quantity: 2, QuotedPrice: 50000, PurchaseMode: "REGULAR"},
	}, 100000, 1*time.Second)
	if err != nil {
		t.Fatalf("GenerateQuoteToken failed: %v", err)
	}

	orderReq := &dto.CreateOrderRequest{
		CustomerName:    "Người Replay",
		CustomerEmail:   "replay@example.com",
		CustomerPhone:   "0988776655",
		ShippingAddress: "123 Đường Test, Hà Nội",
		PaymentMethod:   "COD",
		IdempotencyKey:  "idemp-key-replay-999",
		QuoteToken:      quoteTok,
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 2},
		},
	}

	firstResp, err := svc.CreateOrder(ctx, userID, orderReq)
	if err != nil {
		t.Fatalf("Lỗi tạo đơn ban đầu: %v", err)
	}

	// 2. Chờ clock vượt expiry của CHÍNH token ban đầu đã dùng (T08)
	for {
		if _, verifyErr := VerifyQuoteToken([]byte(testQuoteSecret), quoteTok, userID); verifyErr == ErrQuoteExpired {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Tắt Redis để mô phỏng Redis không sẵn sàng lúc replay (T08)
	mr.Close()

	// Giữ nguyên requestFingerprint khớp đơn ban đầu (chứa chính quoteTok ban đầu đã expired)
	replayReq := &dto.CreateOrderRequest{
		CustomerName:    "Người Replay",
		CustomerEmail:   "replay@example.com",
		CustomerPhone:   "0988776655",
		ShippingAddress: "123 Đường Test, Hà Nội",
		PaymentMethod:   "COD",
		IdempotencyKey:  "idemp-key-replay-999",
		QuoteToken:      orderReq.QuoteToken, // Chính token đã dùng và nay đã hết hạn
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 2},
		},
	}

	replayResp, err := svc.CreateOrder(ctx, userID, replayReq)
	if err != nil {
		t.Fatalf("Kỳ vọng Replay thành công khi token hết hạn và Redis down nhưng bị lỗi: %v", err)
	}
	if replayResp.ID != firstResp.ID || replayResp.OrderCode != firstResp.OrderCode {
		t.Errorf("Kỳ vọng replay ra đúng đơn cũ ID=%d nhưng nhận ID=%d", firstResp.ID, replayResp.ID)
	}

	// 3. Nếu gửi cùng IdempotencyKey nhưng đổi địa chỉ giao hàng -> Phải báo IDEMPOTENCY_CONFLICT
	conflictReq := &dto.CreateOrderRequest{
		CustomerName:    "Người Replay",
		CustomerEmail:   "replay@example.com",
		CustomerPhone:   "0988776655",
		ShippingAddress: "999 Địa Chỉ Khác, TP.HCM", // Thay đổi fingerprint
		PaymentMethod:   "COD",
		IdempotencyKey:  "idemp-key-replay-999",
		QuoteToken:      orderReq.QuoteToken,
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 2},
		},
	}
	_, err = svc.CreateOrder(ctx, userID, conflictReq)
	if err == nil {
		t.Fatal("Kỳ vọng lỗi IDEMPOTENCY_CONFLICT khi thay đổi thông tin nhưng lại thành công")
	}
}

func TestOrderService_FailClosedRedis_WhenFlashSale(t *testing.T) {
	dbName := fmt.Sprintf("file:ordersvc_failclosed_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
	if err != nil {
		t.Fatalf("Không thể khởi tạo sqlite: %v", err)
	}
	_ = db.AutoMigrate(
		&domain.Order{},
		&domain.OrderItem{},
		&domain.FlashSaleCampaign{},
		&domain.FlashSaleItem{},
		&domain.FlashSaleReservation{},
		&domain.OutboxEvent{},
		&domain.Cart{},
		&domain.CartItem{},
		&domain.CheckoutAttempt{},
	)
	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	producer := kafka.NewNoopOrderKafkaProducer()

	// Svc với redisClient = nil (Mô phỏng Redis outage)
	svc := NewOrderService(orderRepo, cartRepo, nil, nil, producer, testQuoteSecret)
	svc.(interface {
		SetFlashSale(*gorm.DB, domain.FlashSaleRepository)
	}).SetFlashSale(db, fsRepo)

	// Tạo active campaign trong DB
	now := time.Now()
	camp := &domain.FlashSaleCampaign{
		Name:      "Flash Sale Test Fail Closed",
		StartsAt:  now.Add(-10 * time.Minute),
		EndsAt:    now.Add(50 * time.Minute),
		Status:    domain.CampaignStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
		Items: []domain.FlashSaleItem{
			{
				ProductID:      99,
				SalePrice:      10000,
				AllocatedStock: 10,
				SoldStock:      0,
			},
		},
	}
	_ = fsRepo.CreateCampaign(camp)

	ctx := context.Background()
	userID := "user-fail-closed"
	campID := camp.ID

	quoteTok, _ := GenerateQuoteToken([]byte(testQuoteSecret), userID, []QuoteItem{
		{ProductID: 99, Quantity: 1, QuotedPrice: 10000, IsFlashSale: true, CampaignID: &campID, PurchaseMode: "FLASH_SALE"},
	}, 10000, 10*time.Minute)

	req := &dto.CreateOrderRequest{
		CustomerName:    "Test User",
		CustomerEmail:   "failclosed@example.com",
		CustomerPhone:   "0911223344",
		ShippingAddress: "Địa chỉ",
		PaymentMethod:   "COD",
		QuoteToken:      quoteTok,
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 99, Quantity: 1},
		},
	}

	_, err = svc.CreateOrder(ctx, userID, req)
	if err == nil {
		t.Fatal("Kỳ vọng lỗi fail-closed khi Redis nil cho Flash Sale item nhưng lại thành công")
	}
	if err.Error() == "" || !errors.Is(err, errors.New(err.Error())) {
		// Just ensure error returned
	}
}

