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

func setupTestOrderApp(t *testing.T) (*fiber.App, string, string) {
	secret := "test-secret-for-orders"
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Không thể khởi chạy miniredis: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	orderRepo := &mockOrderRepoForHandlerTest{}
	producer := kafka.NewNoopOrderKafkaProducer()

	orderSvc := service.NewOrderService(orderRepo, nil, nil, rdb, producer, testQuoteSecret)

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
