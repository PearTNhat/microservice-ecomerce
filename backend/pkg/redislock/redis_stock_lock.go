package redislock

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const (
	StockResultNotFound     = -1 // Chưa khởi tạo stock trên Redis
	StockResultInsufficient = 0  // Không đủ hàng tồn kho
	StockResultSuccess      = 1  // Trừ kho thành công
)

var deductStockScript = redis.NewScript(`
	local stock = tonumber(redis.call('GET', KEYS[1]))
	if not stock then
		return -1
	end
	local qty = tonumber(ARGV[1])
	if stock < qty then
		return 0
	end
	redis.call('DECRBY', KEYS[1], qty)
	return 1
`)

var revertStockScript = redis.NewScript(`
	local stock = tonumber(redis.call('GET', KEYS[1]))
	if stock then
		local qty = tonumber(ARGV[1])
		redis.call('INCRBY', KEYS[1], qty)
		return 1
	end
	return 0
`)

func StockKey(productID uint) string {
	return fmt.Sprintf("product:stock:%d", productID)
}

// DeductStockAtomic trừ tồn kho an toàn tuyệt đối chống bán âm bằng Redis Lua Script
func DeductStockAtomic(ctx context.Context, rdb *redis.Client, productID uint, quantity int) (int64, error) {
	if rdb == nil {
		return StockResultNotFound, nil
	}

	key := StockKey(productID)
	res, err := deductStockScript.Run(ctx, rdb, []string{key}, quantity).Result()
	if err != nil {
		return StockResultNotFound, err
	}

	result, ok := res.(int64)
	if !ok {
		return StockResultNotFound, fmt.Errorf("không thể ép kiểu kết quả Redis Lua")
	}

	return result, nil
}

// RevertStockAtomic hoàn lại số lượng tồn kho nếu xảy ra lỗi hủy đơn
func RevertStockAtomic(ctx context.Context, rdb *redis.Client, productID uint, quantity int) error {
	if rdb == nil {
		return nil
	}

	key := StockKey(productID)
	return revertStockScript.Run(ctx, rdb, []string{key}, quantity).Err()
}

// SetStock nạp số lượng tồn kho của sản phẩm vào Redis
func SetStock(ctx context.Context, rdb *redis.Client, productID uint, stock int) error {
	if rdb == nil {
		return nil
	}

	key := StockKey(productID)
	return rdb.Set(ctx, key, stock, 0).Err() // No TTL
}

// GetStock lấy số lượng tồn kho hiện tại từ Redis
func GetStock(ctx context.Context, rdb *redis.Client, productID uint) (int, error) {
	if rdb == nil {
		return 0, fmt.Errorf("redis client nil")
	}

	key := StockKey(productID)
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(val)
}
