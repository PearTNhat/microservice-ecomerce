package handlers

import (
	"bytes"
	"ecomerce-service/config"
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/dto"
	"ecomerce-service/pkg/utils"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
)

type mockCartRepoForCartHandlerTest struct {
	cart *domain.Cart
}

func (m *mockCartRepoForCartHandlerTest) GetCartByUserID(userID string) (*domain.Cart, error) {
	if m.cart == nil {
		m.cart = &domain.Cart{
			ID:     1,
			UserID: userID,
			Items:  []domain.CartItem{},
		}
	}
	return m.cart, nil
}

func (m *mockCartRepoForCartHandlerTest) AddItem(cartID uint, item *domain.CartItem) error {
	item.ID = uint(len(m.cart.Items) + 1)
	item.CartID = cartID
	m.cart.Items = append(m.cart.Items, *item)
	return nil
}

func (m *mockCartRepoForCartHandlerTest) UpdateItemQuantity(cartID uint, itemID uint, quantity int) error {
	for i, it := range m.cart.Items {
		if it.ID == itemID {
			m.cart.Items[i].Quantity = quantity
			return nil
		}
	}
	return nil
}

func (m *mockCartRepoForCartHandlerTest) RemoveItem(cartID uint, itemID uint) error {
	var newItems []domain.CartItem
	for _, it := range m.cart.Items {
		if it.ID != itemID {
			newItems = append(newItems, it)
		}
	}
	m.cart.Items = newItems
	return nil
}

func (m *mockCartRepoForCartHandlerTest) ClearCart(cartID uint) error {
	m.cart.Items = []domain.CartItem{}
	return nil
}

func setupTestCartApp(t *testing.T) (*fiber.App, string) {
	secret := "test-secret-for-cart"
	cartRepo := &mockCartRepoForCartHandlerTest{}
	cartSvc := service.NewCartService(cartRepo, nil)

	app := fiber.New()
	rh := &rest.RestHandler{
		App: app,
		Config: config.AppConfig{
			AppSecret: secret,
		},
	}

	SetupCartRoutes(rh, cartSvc)

	token, _ := utils.GenerateTokenWithRole(1, "CUSTOMER", secret)
	return app, token
}

func TestCartHandler_UnauthorizedWhenNoToken(t *testing.T) {
	app, _ := setupTestCartApp(t)

	req := httptest.NewRequest("GET", "/cart", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Lỗi test request: %v", err)
	}

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Kỳ vọng status 401 Unauthorized khi không có token nhưng nhận %d", resp.StatusCode)
	}
}

func TestCartHandler_CartFlow(t *testing.T) {
	app, token := setupTestCartApp(t)

	// 1. Get Cart ban đầu
	req := httptest.NewRequest("GET", "/cart", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Lỗi Get Cart: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Kỳ vọng status 200 nhưng nhận %d", resp.StatusCode)
	}

	// 2. Add to Cart
	addBody, _ := json.Marshal(dto.AddToCartRequest{
		ProductID: 10,
		Quantity:  2,
	})
	addReq := httptest.NewRequest("POST", "/cart/add", bytes.NewReader(addBody))
	addReq.Header.Set("Authorization", "Bearer "+token)
	addReq.Header.Set("Content-Type", "application/json")

	addResp, err := app.Test(addReq)
	if err != nil {
		t.Fatalf("Lỗi Add Cart: %v", err)
	}
	if addResp.StatusCode != http.StatusOK {
		t.Errorf("Kỳ vọng status 200 khi thêm giỏ nhưng nhận %d", addResp.StatusCode)
	}
}
