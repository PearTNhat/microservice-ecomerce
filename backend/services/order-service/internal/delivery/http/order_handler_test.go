package http

import (
	"bytes"
	"ecomerce-service/pkg/config"
	"ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/server"
	"ecomerce-service/pkg/utils"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/dto"
	"ecomerce-service/services/order-service/internal/service"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fmt"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"ecomerce-service/services/order-service/internal/repository"
	"github.com/alicebob/miniredis/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

const testQuoteSecret = "test-quote-secret-key-for-unit-tests-32b"

type mockOrderRepoForHandlerTest struct {
	orders []*domain.Order
}

func (m *mockOrderRepoForHandlerTest) CreateOrder(order *domain.Order) error {
	order.ID = uint(len(m.orders) + 1)
	m.orders = append(m.orders, order)
	return nil
}

func (m *mockOrderRepoForHandlerTest) FindByID(id uint) (*domain.Order, error) {
	for _, o := range m.orders {
		if o.ID == id {
			return o, nil
		}
	}
	return nil, nil
}

func (m *mockOrderRepoForHandlerTest) FindByOrderCode(orderCode string) (*domain.Order, error) {
	return nil, nil
}

func (m *mockOrderRepoForHandlerTest) FindByUserID(userID string, page int, limit int) ([]*domain.Order, int64, error) {
	return m.orders, int64(len(m.orders)), nil
}

func (m *mockOrderRepoForHandlerTest) UpdateStatus(orderID uint, status string) error {
	for _, o := range m.orders {
		if o.ID == orderID {
			o.OrderStatus = status
			return nil
		}
	}
	return nil
}

func (m *mockOrderRepoForHandlerTest) UpdatePaymentStatus(orderID uint, status string) error {
	return nil
}

func (m *mockOrderRepoForHandlerTest) GetCheckoutAttempt(userID, key string) (*domain.CheckoutAttempt, error) {
	return nil, nil
}

func (m *mockOrderRepoForHandlerTest) CreateCheckoutAttempt(attempt *domain.CheckoutAttempt) error {
	return nil
}

func (m *mockOrderRepoForHandlerTest) SaveCheckoutAttempt(attempt *domain.CheckoutAttempt) error {
	return nil
}

func (m *mockOrderRepoForHandlerTest) FindOrderByCheckoutAttemptID(attemptID uint) (*domain.Order, error) {
	return nil, nil
}

func (m *mockOrderRepoForHandlerTest) GetCheckoutAttemptByID(id uint) (*domain.CheckoutAttempt, error) {
	return nil, nil
}

func (m *mockOrderRepoForHandlerTest) CASClaimPending(attempt *domain.CheckoutAttempt) (bool, error) {
	return true, nil
}

func (m *mockOrderRepoForHandlerTest) CASRecoveringTakeover(id uint, expectedVersion uint64, newOwner string, newLease time.Time, recoveryTarget string) (*domain.CheckoutAttempt, bool, error) {
	return nil, false, nil
}

func (m *mockOrderRepoForHandlerTest) CASRenewLease(id uint, expectedVersion uint64, ownerToken string, newLease time.Time) (bool, error) {
	return true, nil
}

func (m *mockOrderRepoForHandlerTest) TransitionAttemptStatus(id uint, expectedVersion uint64, newStatus string, recoveryTarget string) (bool, error) {
	return true, nil
}

func (m *mockOrderRepoForHandlerTest) CompleteAttemptInTx(tx *gorm.DB, attemptID uint, expectedVersion uint64, orderID uint, orderCode string, responsePayload string) error {
	return nil
}

func setupTestOrderApp(t *testing.T) (*fiber.App, string, string) {
	secret := "test-secret-for-orders"
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Không thể khởi chạy miniredis: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	dbName := fmt.Sprintf("file:handler_test_%d?mode=memory&cache=shared", time.Now().UnixNano())
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

	orderSvc := service.NewOrderService(orderRepo, cartRepo, nil, rdb, producer, testQuoteSecret)
	orderSvc.(interface {
		SetFlashSale(*gorm.DB, domain.FlashSaleRepository)
	}).SetFlashSale(db, fsRepo)

	app := fiber.New()
	rh := &server.RestHandler{
		App: app,
		Config: config.AppConfig{
			AppSecret: secret,
		},
	}

	SetupOrderRoutes(rh, orderSvc, rdb)

	token, _ := utils.GenerateTokenWithRole(1, "CUSTOMER", secret)
	return app, token, secret
}

func TestOrderHandler_UnauthorizedWhenNoToken(t *testing.T) {
	app, _, _ := setupTestOrderApp(t)

	req := httptest.NewRequest("GET", "/orders", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Lỗi test request: %v", err)
	}

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Kỳ vọng status 401 Unauthorized khi không có token nhưng nhận %d", resp.StatusCode)
	}
}

func TestOrderHandler_CreateOrder(t *testing.T) {
	app, token, _ := setupTestOrderApp(t)

	// 1. Kiểm tra khi thiếu quote_token: phải trả về 400 Bad Request
	orderBodyMissingQuote, _ := json.Marshal(dto.CreateOrderRequest{
		CustomerName:    "Khách Hàng Mẫu",
		CustomerEmail:   "test@gmail.com",
		CustomerPhone:   "0901234567",
		ShippingAddress: "Hà Nội, Việt Nam",
		PaymentMethod:   "COD",
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 1},
		},
	})
	reqMissing := httptest.NewRequest("POST", "/orders", bytes.NewReader(orderBodyMissingQuote))
	reqMissing.Header.Set("Authorization", "Bearer "+token)
	reqMissing.Header.Set("Content-Type", "application/json")
	respMissing, err := app.Test(reqMissing)
	if err != nil {
		t.Fatalf("Lỗi Create Order missing quote: %v", err)
	}
	if respMissing.StatusCode != http.StatusBadRequest {
		t.Errorf("Kỳ vọng status 400 Bad Request khi thiếu quote_token nhưng nhận %d", respMissing.StatusCode)
	}

	// 2. Kiểm tra khi có quote_token hợp lệ: trả về 201 Created
	quoteToken, err := service.GenerateQuoteToken([]byte(testQuoteSecret), "1", []service.QuoteItem{
		{ProductID: 1, Quantity: 1, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
	}, 100000, 10*time.Minute)
	if err != nil {
		t.Fatalf("Lỗi sinh quote token: %v", err)
	}

	orderBody, _ := json.Marshal(dto.CreateOrderRequest{
		CustomerName:    "Khách Hàng Mẫu",
		CustomerEmail:   "test@gmail.com",
		CustomerPhone:   "0901234567",
		ShippingAddress: "Hà Nội, Việt Nam",
		PaymentMethod:   "COD",
		QuoteToken:      quoteToken,
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 1},
		},
	})

	req := httptest.NewRequest("POST", "/orders", bytes.NewReader(orderBody))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Key", "idemp-key-order-1")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Lỗi Create Order: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("Kỳ vọng status 201 Created nhưng nhận %d", resp.StatusCode)
	}
}
