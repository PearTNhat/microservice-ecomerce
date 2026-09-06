package redislock

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return mr, client
}

func TestFlashSaleStockLock(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	productID := uint(101)

	// 1. Test Prewarm Stock
	err := PrewarmStock(ctx, client, productID, 2)
	assert.NoError(t, err)

	stock, err := GetStock(ctx, client, productID)
	assert.NoError(t, err)
	assert.Equal(t, 2, stock)

	// 2. User 1 buys 1 item -> SUCCESS
	res1, err := DeductFlashSaleStockAtomic(ctx, client, productID, "user-1", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(StockResultSuccess), res1)

	// 3. User 1 tries to buy again -> ALREADY PURCHASED (1 item / 1 user rule)
	resDup, err := DeductFlashSaleStockAtomic(ctx, client, productID, "user-1", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(StockResultAlreadyPurchased), resDup)

	// 4. User 2 buys 1 item -> SUCCESS (Stock becomes 0)
	res2, err := DeductFlashSaleStockAtomic(ctx, client, productID, "user-2", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(StockResultSuccess), res2)

	// 5. User 3 tries to buy -> INSUFFICIENT (Sold Out)
	res3, err := DeductFlashSaleStockAtomic(ctx, client, productID, "user-3", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(StockResultInsufficient), res3)

	// 6. Test Revert Flash Sale Stock for User 2
	err = RevertFlashSaleStockAtomic(ctx, client, productID, "user-2", 1)
	assert.NoError(t, err)

	// Stock should be 1 again, User 2 can buy again or User 3 can buy
	res3Retry, err := DeductFlashSaleStockAtomic(ctx, client, productID, "user-3", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(StockResultSuccess), res3Retry)
}
