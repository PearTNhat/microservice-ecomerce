package handlers

import (
	"bytes"
	"ecomerce-service/config"
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/dto"
	"ecomerce-service/pkg/rabbitmq"
	"ecomerce-service/pkg/utils"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

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
	producer := rabbitmq.NewNoopRabbitMQProducer()

	orderSvc := service.NewOrderService(orderRepo, nil, nil, rdb, producer)

	app := fiber.New()
	rh := &rest.RestHandler{
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

	orderBody, _ := json.Marshal(dto.CreateOrderRequest{
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
