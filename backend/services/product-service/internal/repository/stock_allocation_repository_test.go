package repository

import (
	"fmt"
	"testing"
	"time"

	"ecomerce-service/services/product-service/internal/domain"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupTestDB(t *testing.T) *gorm.DB {
	dbName := fmt.Sprintf("file:memdb_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
	assert.NoError(t, err)

	err = db.AutoMigrate(&domain.Product{}, &domain.ProductStockAllocation{})
	assert.NoError(t, err)

	return db
}

func TestStockAllocation_AllocateAndRelease(t *testing.T) {
	db := setupTestDB(t)
	repo := NewStockAllocationRepository(db)

	// Tạo sản phẩm mẫu có tồn kho 100
	prod := &domain.Product{
		Name:  "Tủ lạnh Inverter",
		Slug:  "tu-lanh-inverter",
		Price: 10000000,
		Stock: 100,
	}
	assert.NoError(t, db.Create(prod).Error)

	campaignID := uint(1)
	requestID := "req-alloc-1"

	// 1. Phân bổ 20 sản phẩm cho Flash Sale
	alloc, err := repo.AllocateStock(campaignID, prod.ID, requestID, 20)
	assert.NoError(t, err)
	assert.NotNil(t, alloc)
	assert.Equal(t, 20, alloc.AllocatedQuantity)
	assert.Equal(t, domain.StockAllocationStatusAllocated, alloc.Status)

	// Kiểm tra stock thường bị giảm còn 80
	var checkProd domain.Product
	assert.NoError(t, db.First(&checkProd, prod.ID).Error)
	assert.Equal(t, 80, checkProd.Stock)

	// 2. Idempotency test: Gửi lại cùng requestID phải trả về kết quả cũ, không trừ stock lần 2
	allocDup, err := repo.AllocateStock(campaignID, prod.ID, requestID, 20)
	assert.NoError(t, err)
	assert.Equal(t, alloc.ID, allocDup.ID)

	assert.NoError(t, db.First(&checkProd, prod.ID).Error)
	assert.Equal(t, 80, checkProd.Stock)

	// 3. Bán 5 sản phẩm
	err = repo.IncrementSoldQuantity(campaignID, prod.ID, 5)
	assert.NoError(t, err)

	allocAfterSold, err := repo.FindByCampaignAndProduct(campaignID, prod.ID)
	assert.NoError(t, err)
	assert.Equal(t, 5, allocAfterSold.SoldQuantity)

	// 4. Release tồn kho thừa khi campaign kết thúc (còn thừa 20 - 5 = 15)
	released, err := repo.ReleaseStock(campaignID, prod.ID, "req-release-1")
	assert.NoError(t, err)
	assert.Equal(t, 15, released)

	// Stock thường phải được hoàn trả 15 -> 80 + 15 = 95
	assert.NoError(t, db.First(&checkProd, prod.ID).Error)
	assert.Equal(t, 95, checkProd.Stock)

	// Gọi release lần 2 không được hoàn lại nữa (đã release hết)
	released2, err := repo.ReleaseStock(campaignID, prod.ID, "req-release-2")
	assert.NoError(t, err)
	assert.Equal(t, 0, released2)
}

func TestStockAllocation_InsufficientStock(t *testing.T) {
	db := setupTestDB(t)
	repo := NewStockAllocationRepository(db)

	prod := &domain.Product{
		Name:  "Smart TV 55",
		Slug:  "smart-tv-55",
		Price: 15000000,
		Stock: 10,
	}
	assert.NoError(t, db.Create(prod).Error)

	// Cố tình phân bổ 50 sản phẩm trong khi chỉ có 10
	_, err := repo.AllocateStock(1, prod.ID, "req-over", 50)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "không đủ tồn kho")

	// Stock thường giữ nguyên 10
	var checkProd domain.Product
	assert.NoError(t, db.First(&checkProd, prod.ID).Error)
	assert.Equal(t, 10, checkProd.Stock)
}
