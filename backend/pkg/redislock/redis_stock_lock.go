package redislock

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	StockResultNotFound         = -1 // Chưa khởi tạo stock trên Redis
	StockResultInsufficient     = 0  // Không đủ hàng tồn kho
	StockResultSuccess          = 1  // Trừ kho thành công
	StockResultAlreadyPurchased = -2 // Người dùng đã mua trong đợt Flash Sale này
)

// Script trừ kho thông thường
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

// Script hoàn kho thông thường
var revertStockScript = redis.NewScript(`
	local stock = tonumber(redis.call('GET', KEYS[1]))
	if stock then
		local qty = tonumber(ARGV[1])
		redis.call('INCRBY', KEYS[1], qty)
		return 1
	end
	return 0
`)

// Script trừ kho Flash Sale: Kiểm tra 1 User/1 Món + Trừ kho Atomic
var deductFlashSaleStockScript = redis.NewScript(`
	local user_set_key = KEYS[2]
	local user_id = ARGV[2]
	
	-- 1. Kiểm tra User đã mua trong đợt Flash Sale chưa (Chống Bot/Đầu cơ gom hàng)
	if redis.call('SISMEMBER', user_set_key, user_id) == 1 then
		return -2
	end

	-- 2. Kiểm tra tồn kho
	local stock = tonumber(redis.call('GET', KEYS[1]))
	if not stock then
		return -1
	end
	local qty = tonumber(ARGV[1])
	if stock < qty then
		return 0
	end

	-- 3. Trừ tồn kho và ghi nhận User đã mua
	redis.call('DECRBY', KEYS[1], qty)
	redis.call('SADD', user_set_key, user_id)
	return 1
`)

// Script hoàn kho Flash Sale khi đơn hàng bị lỗi
var revertFlashSaleStockScript = redis.NewScript(`
	local stock = tonumber(redis.call('GET', KEYS[1]))
	if stock then
		local qty = tonumber(ARGV[1])
		redis.call('INCRBY', KEYS[1], qty)
	end
	redis.call('SREM', KEYS[2], ARGV[2])
	return 1
`)

func StockKey(productID uint) string {
	return fmt.Sprintf("product:stock:%d", productID)
}

func FlashSaleUserSetKey(productID uint) string {
	return fmt.Sprintf("flash_sale:users:%d", productID)
}

func FlashSaleOrderTokenKey(token string) string {
	return fmt.Sprintf("flash_sale:order:%s", token)
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

// DeductFlashSaleStockAtomic trừ tồn kho Flash Sale kèm giới hạn 1 User / 1 Món
func DeductFlashSaleStockAtomic(ctx context.Context, rdb *redis.Client, productID uint, userID string, quantity int) (int64, error) {
	if rdb == nil {
		return StockResultNotFound, nil
	}

	stockKey := StockKey(productID)
	userSetKey := FlashSaleUserSetKey(productID)

	res, err := deductFlashSaleStockScript.Run(ctx, rdb, []string{stockKey, userSetKey}, quantity, userID).Result()
	if err != nil {
		return StockResultNotFound, err
	}

	result, ok := res.(int64)
	if !ok {
		return StockResultNotFound, fmt.Errorf("không thể ép kiểu kết quả Redis Lua")
	}

	return result, nil
}

// RevertFlashSaleStockAtomic hoàn lại kho và xóa user khỏi danh sách đã mua nếu tạo đơn thất bại
func RevertFlashSaleStockAtomic(ctx context.Context, rdb *redis.Client, productID uint, userID string, quantity int) error {
	if rdb == nil {
		return nil
	}

	stockKey := StockKey(productID)
	userSetKey := FlashSaleUserSetKey(productID)

	return revertFlashSaleStockScript.Run(ctx, rdb, []string{stockKey, userSetKey}, quantity, userID).Err()
}

// SetStock nạp số lượng tồn kho của sản phẩm vào Redis
func SetStock(ctx context.Context, rdb *redis.Client, productID uint, stock int) error {
	if rdb == nil {
		return nil
	}

	key := StockKey(productID)
	return rdb.Set(ctx, key, stock, 0).Err() // No TTL
}

// PrewarmStock nạp trước tồn kho và xóa lịch sử user cũ của đợt Flash Sale
func PrewarmStock(ctx context.Context, rdb *redis.Client, productID uint, stock int) error {
	if rdb == nil {
		return fmt.Errorf("redis client nil")
	}

	pipe := rdb.Pipeline()
	pipe.Set(ctx, StockKey(productID), stock, 0)
	pipe.Del(ctx, FlashSaleUserSetKey(productID))
	_, err := pipe.Exec(ctx)
	return err
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

// SetFlashSaleOrderStatus lưu trạng thái đơn hàng bất đồng bộ vào Redis
func SetFlashSaleOrderStatus(ctx context.Context, rdb *redis.Client, token string, statusData string, ttl time.Duration) error {
	if rdb == nil {
		return fmt.Errorf("redis client nil")
	}

	key := FlashSaleOrderTokenKey(token)
	return rdb.Set(ctx, key, statusData, ttl).Err()
}

// GetFlashSaleOrderStatus đọc trạng thái đơn hàng từ Redis RAM
func GetFlashSaleOrderStatus(ctx context.Context, rdb *redis.Client, token string) (string, error) {
	if rdb == nil {
		return "", fmt.Errorf("redis client nil")
	}

	key := FlashSaleOrderTokenKey(token)
	return rdb.Get(ctx, key).Result()
}
