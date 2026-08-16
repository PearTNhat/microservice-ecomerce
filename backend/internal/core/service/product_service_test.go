package service

import (
	"context"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/dto"
	"ecomerce-service/pkg/elasticsearch"
	"ecomerce-service/pkg/kafka"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type mockProductRepository struct {
	products   []*domain.Product
	categories []*domain.Category
	brands     []*domain.Brand
}

func (m *mockProductRepository) FindAll(filter domain.ProductFilter) ([]*domain.Product, int64, error) {
	return m.products, int64(len(m.products)), nil
}

func (m *mockProductRepository) FindById(id uint) (*domain.Product, error) {
	for _, p := range m.products {
		if p.ID == id {
			return p, nil
		}
	}
	return nil, nil
}

func (m *mockProductRepository) FindByIds(ids []uint) ([]*domain.Product, error) {
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

func (m *mockProductRepository) FindBySlug(slug string) (*domain.Product, error) {
	for _, p := range m.products {
		if p.Slug == slug {
			return p, nil
		}
	}
	return nil, nil
}

func (m *mockProductRepository) FindCategories() ([]*domain.Category, error) {
	return m.categories, nil
}

func (m *mockProductRepository) FindBrands() ([]*domain.Brand, error) {
	return m.brands, nil
}

func (m *mockProductRepository) CreateProduct(product *domain.Product) error   { return nil }
func (m *mockProductRepository) CreateCategory(category *domain.Category) error { return nil }
func (m *mockProductRepository) CreateBrand(brand *domain.Brand) error       { return nil }
func (m *mockProductRepository) IncrementViews(id uint) error                  { return nil }
func (m *mockProductRepository) Count() (int64, error)                         { return int64(len(m.products)), nil }

func setupTestProductService(t *testing.T) (*ProductService, *miniredis.Miniredis, *mockProductRepository) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Không thể khởi động miniredis: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	mockRepo := &mockProductRepository{
		categories: []*domain.Category{
			{ID: 1, Name: "Máy Lạnh", Slug: "may-lanh"},
		},
		brands: []*domain.Brand{
			{ID: 1, Name: "Daikin", Slug: "daikin"},
		},
		products: []*domain.Product{
			{
				ID:             1,
				Name:           "Máy Lạnh Daikin Inverter 1.5 HP",
				Slug:           "may-lanh-daikin-inverter-1-5-hp",
				Price:          13500000,
				DiscountPrice:  12190000,
				Stock:          20,
				CategoryID:     1,
				BrandID:        1,
				Specifications: `{"btu":11900,"inverter":true}`,
			},
		},
	}

	mockKafka := kafka.NewNoopProducer()
	mockES := elasticsearch.NewNoopSearchClient()
	svc := NewProductService(mockRepo, redisClient, mockKafka, mockES)

	return svc, mr, mockRepo
}

func TestProductService_GetProducts(t *testing.T) {
	svc, mr, _ := setupTestProductService(t)
	defer mr.Close()

	ctx := context.Background()
	resp, err := svc.GetProducts(ctx, dto.ProductFilterRequest{Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("Lỗi khi lấy danh sách sản phẩm: %v", err)
	}

	if resp.Total != 1 {
		t.Errorf("Kỳ vọng 1 sản phẩm nhưng nhận %d", resp.Total)
	}

	if len(resp.Products) != 1 || resp.Products[0].Name != "Máy Lạnh Daikin Inverter 1.5 HP" {
		t.Errorf("Tên sản phẩm không khớp")
	}
}

func TestProductService_GetProductDetail_CacheHit(t *testing.T) {
	svc, mr, _ := setupTestProductService(t)
	defer mr.Close()

	ctx := context.Background()

	// Lần 1: Cache Miss -> Đọc từ DB và lưu vào Redis Cache
	detail1, err := svc.GetProductDetail(ctx, 1, "user-123")
	if err != nil {
		t.Fatalf("Lỗi lấy chi tiết sản phẩm: %v", err)
	}

	if detail1.Name != "Máy Lạnh Daikin Inverter 1.5 HP" {
		t.Errorf("Tên sản phẩm không khớp")
	}

	// Lần 2: Cache Hit -> Đọc trực tiếp từ Redis Cache
	detail2, err := svc.GetProductDetail(ctx, 1, "user-123")
	if err != nil {
		t.Fatalf("Lỗi lấy chi tiết sản phẩm từ Cache: %v", err)
	}

	if detail2.ID != 1 || detail2.Price != 13500000 {
		t.Errorf("Dữ liệu từ Cache không chính xác")
	}
}

func TestProductService_GetCategories(t *testing.T) {
	svc, mr, _ := setupTestProductService(t)
	defer mr.Close()

	ctx := context.Background()
	categories, err := svc.GetCategories(ctx)
	if err != nil {
		t.Fatalf("Lỗi lấy danh mục: %v", err)
	}

	if len(categories) != 1 || categories[0].Name != "Máy Lạnh" {
		t.Errorf("Danh mục không khớp")
	}
}
