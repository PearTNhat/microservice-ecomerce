package worker

import (
	"context"
	"ecomerce-service/internal/core/domain"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type mockProductRepoForWorker struct {
	mu         sync.Mutex
	viewCounts map[uint]int64
}

func (m *mockProductRepoForWorker) FindAll(filter domain.ProductFilter) ([]*domain.Product, int64, error) {
	return nil, 0, nil
}
func (m *mockProductRepoForWorker) FindById(id uint) (*domain.Product, error)           { return nil, nil }
func (m *mockProductRepoForWorker) FindByIds(ids []uint) ([]*domain.Product, error)     { return nil, nil }
func (m *mockProductRepoForWorker) FindBySlug(slug string) (*domain.Product, error)     { return nil, nil }
func (m *mockProductRepoForWorker) FindCategories() ([]*domain.Category, error)         { return nil, nil }
func (m *mockProductRepoForWorker) FindBrands() ([]*domain.Brand, error)                 { return nil, nil }
func (m *mockProductRepoForWorker) CreateProduct(product *domain.Product) error         { return nil }
func (m *mockProductRepoForWorker) CreateCategory(category *domain.Category) error     { return nil }
func (m *mockProductRepoForWorker) CreateBrand(brand *domain.Brand) error               { return nil }
func (m *mockProductRepoForWorker) IncrementViews(id uint) error                        { return nil }
func (m *mockProductRepoForWorker) Count() (int64, error)                               { return 0, nil }

func (m *mockProductRepoForWorker) BatchIncrementViews(viewCounts map[uint]int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.viewCounts == nil {
		m.viewCounts = make(map[uint]int64)
	}
	for k, v := range viewCounts {
		m.viewCounts[k] += v
	}
	return nil
}

func TestProductViewWorker_Flush(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Không thể khởi động miniredis: %v", err)
	}
	defer mr.Close()

	rClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	mockRepo := &mockProductRepoForWorker{}

	worker := &ProductViewWorker{
		repo:        mockRepo,
		redisClient: rClient,
		buffer:      make(map[uint]int64),
		flushPeriod: 100 * time.Millisecond,
		batchSize:   5,
	}

	// Nạp dữ liệu vào buffer
	worker.buffer[1] = 3
	worker.buffer[2] = 5

	// Set cache Redis mẫu
	_ = rClient.Set(context.Background(), "product:detail:1", "cached_data", time.Hour).Err()

	// Kích hoạt flush
	worker.flush(context.Background())

	// 1. Kiểm tra Database nhận đúng tổng lượt xem
	mockRepo.mu.Lock()
	if mockRepo.viewCounts[1] != 3 || mockRepo.viewCounts[2] != 5 {
		t.Errorf("Kỳ vọng viewCounts[1]=3, viewCounts[2]=5; Thực tế: %v", mockRepo.viewCounts)
	}
	mockRepo.mu.Unlock()

	// 2. Kiểm tra Buffer đã được làm rỗng
	worker.mu.Lock()
	if len(worker.buffer) != 0 {
		t.Errorf("Kỳ vọng buffer rỗng sau khi flush, nhưng còn %d phần tử", len(worker.buffer))
	}
	worker.mu.Unlock()

	// 3. Kiểm tra Cache Redis đã bị xóa để reload dữ liệu mới
	val, err := rClient.Get(context.Background(), "product:detail:1").Result()
	if err == nil && val != "" {
		t.Errorf("Kỳ vọng cache 'product:detail:1' bị xóa, nhưng vẫn còn: %s", val)
	}
}
