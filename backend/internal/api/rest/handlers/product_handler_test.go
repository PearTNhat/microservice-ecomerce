package handlers

import (
	"ecomerce-service/config"
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/pkg/elasticsearch"
	"ecomerce-service/pkg/kafka"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

type mockProductRepoForHandler struct {
	products   []*domain.Product
	categories []*domain.Category
	brands     []*domain.Brand
}

func (m *mockProductRepoForHandler) FindAll(filter domain.ProductFilter) ([]*domain.Product, int64, error) {
	return m.products, int64(len(m.products)), nil
}

func (m *mockProductRepoForHandler) FindById(id uint) (*domain.Product, error) {
	for _, p := range m.products {
		if p.ID == id {
			return p, nil
		}
	}
	return nil, nil
}

func (m *mockProductRepoForHandler) FindByIds(ids []uint) ([]*domain.Product, error) {
	var result []*domain.Product
	for _, id := range ids {
		for _, p := range m.products {
			if p.ID == id {
				result = append(result, p)
			}
		}
	}
	return result, nil
}

func (m *mockProductRepoForHandler) FindBySlug(slug string) (*domain.Product, error) { return nil, nil }
func (m *mockProductRepoForHandler) FindCategories() ([]*domain.Category, error)       { return m.categories, nil }
func (m *mockProductRepoForHandler) FindBrands() ([]*domain.Brand, error)             { return m.brands, nil }
func (m *mockProductRepoForHandler) CreateProduct(product *domain.Product) error       { return nil }
func (m *mockProductRepoForHandler) CreateCategory(category *domain.Category) error   { return nil }
func (m *mockProductRepoForHandler) CreateBrand(brand *domain.Brand) error           { return nil }
func (m *mockProductRepoForHandler) IncrementViews(id uint) error                      { return nil }
func (m *mockProductRepoForHandler) BatchIncrementViews(viewCounts map[uint]int64) error { return nil }
func (m *mockProductRepoForHandler) Count() (int64, error)                             { return int64(len(m.products)), nil }

func setupTestProductApp(t *testing.T) (*fiber.App, *miniredis.Miniredis) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Không thể khởi động miniredis: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	mockRepo := &mockProductRepoForHandler{
		categories: []*domain.Category{
			{ID: 1, Name: "Máy Lạnh - Điều Hòa", Slug: "may-lanh-dieu-hoa"},
		},
		products: []*domain.Product{
			{
				ID:             1,
				Name:           "Máy Lạnh Daikin Inverter 1.5 HP",
				Slug:           "may-lanh-daikin-inverter-1-5-hp",
				Price:          13500000,
				DiscountPrice:  12190000,
				Stock:          30,
				CategoryID:     1,
				Specifications: `{"btu":11900,"inverter":true}`,
			},
		},
	}

	mockKafka := kafka.NewNoopProducer()
	mockES := elasticsearch.NewNoopSearchClient()
	productService := service.NewProductService(mockRepo, redisClient, mockKafka, mockES)

	app := fiber.New()
	rh := &rest.RestHandler{
		App:    app,
		Config: config.AppConfig{},
	}

	SetupProductRoutes(rh, productService)

	return app, mr
}

func TestProductHandler_GetCategories(t *testing.T) {
	app, mr := setupTestProductApp(t)
	defer mr.Close()

	req := httptest.NewRequest("GET", "/categories", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatalf("Lỗi gửi request: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Kỳ vọng status 200 nhưng nhận %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var responseData map[string]interface{}
	json.Unmarshal(body, &responseData)

	if status, ok := responseData["status"]; !ok || status != "success" {
		t.Errorf("Response không thành công: %s", string(body))
	}
}

func TestProductHandler_GetProducts(t *testing.T) {
	app, mr := setupTestProductApp(t)
	defer mr.Close()

	req := httptest.NewRequest("GET", "/products?page=1&limit=10", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatalf("Lỗi gửi request: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Kỳ vọng status 200 nhưng nhận %d", resp.StatusCode)
	}
}

func TestProductHandler_GetProductDetail(t *testing.T) {
	app, mr := setupTestProductApp(t)
	defer mr.Close()

	req := httptest.NewRequest("GET", "/products/1", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatalf("Lỗi gửi request: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Kỳ vọng status 200 nhưng nhận %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var responseData map[string]interface{}
	json.Unmarshal(body, &responseData)

	data, ok := responseData["data"].(map[string]interface{})
	if !ok || data["name"] != "Máy Lạnh Daikin Inverter 1.5 HP" {
		t.Errorf("Dữ liệu chi tiết sản phẩm không khớp: %s", string(body))
	}
}
