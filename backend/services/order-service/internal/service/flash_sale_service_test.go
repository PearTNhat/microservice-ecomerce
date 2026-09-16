package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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

type mockProductClientForFlashSale struct {
	failProductID uint
	allocated     map[uint]int
}

func (m *mockProductClientForFlashSale) GetProduct(ctx context.Context, productID uint) (*dto.ProductDetailResponse, error) {
	return &dto.ProductDetailResponse{
		ID:    productID,
		Name:  fmt.Sprintf("Product #%d", productID),
		Price: 1000000,
	}, nil
}

func (m *mockProductClientForFlashSale) AllocateFlashSaleStock(ctx context.Context, campaignID, productID uint, requestID string, quantity int) error {
	if productID == m.failProductID {
		return errors.New("mock allocate error")
	}
	m.allocated[productID] = quantity
	return nil
}

func (m *mockProductClientForFlashSale) ReleaseFlashSaleStock(ctx context.Context, campaignID, productID uint, requestID string) error {
	delete(m.allocated, productID)
	return nil
}

func (m *mockProductClientForFlashSale) GetStockAllocation(ctx context.Context, campaignID, productID uint) (*dto.StockAllocationResponse, error) {
	qty := m.allocated[productID]
	return &dto.StockAllocationResponse{
		CampaignID:        campaignID,
		ProductID:         productID,
		AllocatedQuantity: qty,
		SoldQuantity:      0,
		ReleasedQuantity:  0,
	}, nil
}

func setupFlashSaleTestEnv(t *testing.T) (*gorm.DB, *miniredis.Miniredis, *redis.Client) {
	dbName := fmt.Sprintf("file:fs_svc_mem_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
	assert.NoError(t, err)

	err = db.AutoMigrate(
		&domain.FlashSaleCampaign{},
		&domain.FlashSaleItem{},
		&domain.FlashSaleReservation{},
		&domain.OutboxEvent{},
		&domain.ProcessedEvent{},
	)
	assert.NoError(t, err)

	mr, err := miniredis.Run()
	assert.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return db, mr, client
}

func TestFlashSaleService_CreateAndActivateCampaign(t *testing.T) {
	db, mr, rdb := setupFlashSaleTestEnv(t)
	defer mr.Close()
	defer rdb.Close()

	ctx := context.Background()
	fsRepo := repository.NewFlashSaleRepository(db)
	prodClient := &mockProductClientForFlashSale{allocated: make(map[uint]int)}

	svc := NewFlashSaleService(fsRepo, prodClient, rdb)

	now := time.Now()
	// 1. Tạo campaign
	camp, err := svc.CreateCampaign(ctx, &dto.CreateCampaignRequest{
		Name:     "Campaign 9.9",
		StartsAt: now.Add(-10 * time.Second),
		EndsAt:   now.Add(2 * time.Hour),
	})
	assert.NoError(t, err)
	assert.Equal(t, "DRAFT", camp.Status)

	// 2. Thêm item
	item, err := svc.AddItem(ctx, camp.ID, &dto.AddFlashSaleItemRequest{
		ProductID:           101,
		SalePrice:           500000,
		OriginalPrice:       1000000,
		AllocatedStock:      10,
		MaxQuantityPerUser:  1,
		MaxQuantityPerOrder: 1,
		ReservationSeconds:  120,
	})
	assert.NoError(t, err)
	assert.Equal(t, uint(101), item.ProductID)

	// 3. Kích hoạt campaign -> Phải gọi sang ProductClient allocate và prewarm Redis
	err = svc.ActivateCampaign(ctx, camp.ID)
	assert.NoError(t, err)

	// Kiểm tra trạng thái đã sang ACTIVE
	campActive, err := svc.GetCampaign(ctx, camp.ID)
	assert.NoError(t, err)
	assert.Equal(t, "ACTIVE", campActive.Status)
	assert.Equal(t, 10, prodClient.allocated[101])

	// Kiểm tra Redis state là ACTIVE
	state, err := rdb.Get(ctx, redislock.KeyState(camp.ID, 101)).Result()
	assert.NoError(t, err)
	assert.Equal(t, "ACTIVE", state)

	// 4. Reserve Order từ khách hàng
	orderResp, err := svc.ReserveOrder(ctx, camp.ID, 101, "user-abc", "req-cust-1", &dto.FlashSaleCustomerOrderRequest{
		Quantity:        1,
		PaymentMethod:   "COD",
		CustomerName:    "Nguyen Van A",
		CustomerEmail:   "a@test.com",
		CustomerPhone:   "0987654321",
		ShippingAddress: "123 Street",
	})
	assert.NoError(t, err)
	assert.NotNil(t, orderResp)
	assert.Equal(t, "RESERVED", orderResp.Status)
	assert.NotEmpty(t, orderResp.ReservationID)

	// Kiểm tra DB có reservation và outbox_event
	var countResv int64
	db.Model(&domain.FlashSaleReservation{}).Where("id = ?", orderResp.ReservationID).Count(&countResv)
	assert.Equal(t, int64(1), countResv)

	var countOutbox int64
	db.Model(&domain.OutboxEvent{}).Where("aggregate_id = ?", orderResp.ReservationID).Count(&countOutbox)
	assert.Equal(t, int64(1), countOutbox)

	// 5. Thử mua lần 2 cùng user -> Bị chặn giới hạn mua tối đa
	_, err = svc.ReserveOrder(ctx, camp.ID, 101, "user-abc", "req-cust-2", &dto.FlashSaleCustomerOrderRequest{
		Quantity:        1,
		PaymentMethod:   "COD",
		CustomerName:    "Nguyen Van A",
		CustomerEmail:   "a@test.com",
		CustomerPhone:   "0987654321",
		ShippingAddress: "123 Street",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "giới hạn mua tối đa")
}

func TestFlashSaleService_ActivateCompensatingSaga(t *testing.T) {
	db, mr, rdb := setupFlashSaleTestEnv(t)
	defer mr.Close()
	defer rdb.Close()

	ctx := context.Background()
	fsRepo := repository.NewFlashSaleRepository(db)

	// Product #102 sẽ cố tình bị lỗi khi allocate
	prodClient := &mockProductClientForFlashSale{
		failProductID: 102,
		allocated:     make(map[uint]int),
	}

	svc := NewFlashSaleService(fsRepo, prodClient, rdb)

	now := time.Now()
	camp, err := svc.CreateCampaign(ctx, &dto.CreateCampaignRequest{
		Name:     "Failed Campaign",
		StartsAt: now,
		EndsAt:   now.Add(time.Hour),
	})
	assert.NoError(t, err)

	_, _ = svc.AddItem(ctx, camp.ID, &dto.AddFlashSaleItemRequest{
		ProductID:      101,
		AllocatedStock: 5,
		SalePrice:      1000,
	})
	_, _ = svc.AddItem(ctx, camp.ID, &dto.AddFlashSaleItemRequest{
		ProductID:      102, // Sẽ lỗi
		AllocatedStock: 5,
		SalePrice:      1000,
	})

	err = svc.ActivateCampaign(ctx, camp.ID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock allocate error")

	// Đảm bảo bồi hoàn: Product #101 đã được giải phóng
	assert.Empty(t, prodClient.allocated)

	// Trạng thái campaign chuyển sang ACTIVATION_FAILED
	campCheck, _ := svc.GetCampaign(ctx, camp.ID)
	assert.Equal(t, "ACTIVATION_FAILED", campCheck.Status)
}

func TestFlashSaleService_GetActiveCampaign(t *testing.T) {
	db, mr, rdb := setupFlashSaleTestEnv(t)
	defer mr.Close()
	defer rdb.Close()

	ctx := context.Background()
	fsRepo := repository.NewFlashSaleRepository(db)
	prodClient := &mockProductClientForFlashSale{allocated: make(map[uint]int)}
	svc := NewFlashSaleService(fsRepo, prodClient, rdb)

	// 1. Khi chưa có campaign nào active
	active, err := svc.GetActiveCampaign(ctx)
	assert.NoError(t, err)
	assert.Nil(t, active)

	// 2. Tạo và kích hoạt 1 campaign
	now := time.Now()
	camp, err := svc.CreateCampaign(ctx, &dto.CreateCampaignRequest{
		Name:        "Super Sale",
		Description: "Mô tả sale",
		StartsAt:    now.Add(-10 * time.Minute),
		EndsAt:      now.Add(2 * time.Hour),
	})
	assert.NoError(t, err)

	_, err = svc.AddItem(ctx, camp.ID, &dto.AddFlashSaleItemRequest{
		ProductID:           101,
		SalePrice:           500000,
		OriginalPrice:       1000000,
		AllocatedStock:      20,
		MaxQuantityPerUser:  2,
		MaxQuantityPerOrder: 1,
		ReservationSeconds:  120,
	})
	assert.NoError(t, err)

	err = svc.ActivateCampaign(ctx, camp.ID)
	assert.NoError(t, err)

	// 3. Query active campaign
	active, err = svc.GetActiveCampaign(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, active)
	assert.Equal(t, camp.ID, active.ID)
	assert.Equal(t, "Super Sale", active.Name)
	assert.Equal(t, "ACTIVE", active.Status)
	assert.True(t, active.RemainingSeconds > 0)
	assert.Len(t, active.Items, 1)
	assert.Equal(t, uint(101), active.Items[0].ProductID)
	assert.Equal(t, 50, active.Items[0].DiscountPercentage)
	assert.Equal(t, 20, active.Items[0].RemainingStock)
	assert.Equal(t, 2, active.Items[0].MaxQuantityPerUser)
}

func TestFlashSaleService_ProductOffers(t *testing.T) {
	_, mr, rdb := setupFlashSaleTestEnv(t)
	defer mr.Close()
	defer rdb.Close()

	dbName := fmt.Sprintf("file:fs_offers_test_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
	assert.NoError(t, err)
	_ = db.AutoMigrate(&domain.FlashSaleCampaign{}, &domain.FlashSaleItem{}, &domain.FlashSaleReservation{})

	fsRepo := repository.NewFlashSaleRepository(db)
	prodClient := &mockProductClientForFlashSale{allocated: make(map[uint]int)}
	svc := NewFlashSaleService(fsRepo, prodClient, rdb)
	ctx := context.Background()

	// 1. Kiểm tra sản phẩm khi chưa có chiến dịch nào
	offer, err := svc.GetProductOffer(ctx, 101, "user-1")
	assert.NoError(t, err)
	assert.NotNil(t, offer)
	assert.False(t, offer.HasFlashSale)

	// 2. Tạo & kích hoạt chiến dịch
	now := time.Now()
	camp, err := svc.CreateCampaign(ctx, &dto.CreateCampaignRequest{
		Name:        "Holiday Sale",
		Description: "Holiday Sale Desc",
		StartsAt:    now.Add(-5 * time.Minute),
		EndsAt:      now.Add(2 * time.Hour),
	})
	assert.NoError(t, err)

	_, err = svc.AddItem(ctx, camp.ID, &dto.AddFlashSaleItemRequest{
		ProductID:           101,
		SalePrice:           400000,
		OriginalPrice:       1000000,
		AllocatedStock:      10,
		MaxQuantityPerUser:  2,
		MaxQuantityPerOrder: 1,
		ReservationSeconds:  120,
	})
	assert.NoError(t, err)

	err = svc.ActivateCampaign(ctx, camp.ID)
	assert.NoError(t, err)

	// 3. Lấy ưu đãi đơn lẻ cho sản phẩm 101
	offer, err = svc.GetProductOffer(ctx, 101, "user-1")
	assert.NoError(t, err)
	assert.NotNil(t, offer)
	assert.True(t, offer.HasFlashSale)
	assert.Equal(t, float64(400000), *offer.SalePrice)
	assert.Equal(t, float64(400000), offer.EffectivePrice)
	assert.Equal(t, float64(1000000), offer.OriginalPrice)
	assert.Equal(t, 60, offer.DiscountPercent)
	assert.Equal(t, 10, offer.RemainingStock)
	assert.Equal(t, 2, offer.MaxQuantityPerUser)

	// 4. Lấy ưu đãi hàng loạt (Batch offers)
	batchResp, err := svc.GetBatchProductOffers(ctx, []uint{101, 999}, "user-1")
	assert.NoError(t, err)
	assert.NotNil(t, batchResp)
	assert.Len(t, batchResp.Offers, 2)

	assert.True(t, batchResp.Offers[101].HasFlashSale)
	assert.Equal(t, float64(400000), *batchResp.Offers[101].SalePrice)

	assert.False(t, batchResp.Offers[999].HasFlashSale)
}

