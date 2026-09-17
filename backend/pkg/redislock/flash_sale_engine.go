package redislock

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type FlashSaleResultCode string

const (
	ResultReserved             FlashSaleResultCode = "RESERVED"
	ResultSoldOut              FlashSaleResultCode = "SOLD_OUT"
	ResultLimitExceeded        FlashSaleResultCode = "LIMIT_EXCEEDED"
	ResultCampaignNotActive    FlashSaleResultCode = "CAMPAIGN_NOT_ACTIVE"
	ResultIdempotencyKeyReused FlashSaleResultCode = "IDEMPOTENCY_KEY_REUSED"
	ResultInvalidOrderQuantity FlashSaleResultCode = "INVALID_ORDER_QUANTITY"
	ResultConfirmed            FlashSaleResultCode = "CONFIRMED"
	ResultAlreadyConfirmed     FlashSaleResultCode = "ALREADY_CONFIRMED"
	ResultReleased             FlashSaleResultCode = "RELEASED"
	ResultAlreadyReleased      FlashSaleResultCode = "ALREADY_RELEASED"
	ResultInvalidState         FlashSaleResultCode = "INVALID_STATE"
	ResultInvariantViolation   FlashSaleResultCode = "INVARIANT_VIOLATION"
	ResultReservationClosed    FlashSaleResultCode = "RESERVATION_CLOSED"
)

type FlashSaleResponse struct {
	Code          FlashSaleResultCode `json:"code"`
	ReservationID string              `json:"reservation_id,omitempty"`
	ExpiresAt     int64               `json:"expires_at,omitempty"`
	Quantity      int                 `json:"quantity,omitempty"`
}

// -----------------------------------------------------------------------------
// Cluster-Safe Redis Keys với Hash Tag {c:C:p:P}
// -----------------------------------------------------------------------------

func KeyStock(campaignID, productID uint) string {
	return fmt.Sprintf("fs:{c:%d:p:%d}:stock", campaignID, productID)
}

func KeyReserved(campaignID, productID uint) string {
	return fmt.Sprintf("fs:{c:%d:p:%d}:reserved", campaignID, productID)
}

func KeyPurchased(campaignID, productID uint) string {
	return fmt.Sprintf("fs:{c:%d:p:%d}:purchased", campaignID, productID)
}

func KeyState(campaignID, productID uint) string {
	return fmt.Sprintf("fs:{c:%d:p:%d}:state", campaignID, productID)
}

func KeyConfig(campaignID, productID uint) string {
	return fmt.Sprintf("fs:{c:%d:p:%d}:config", campaignID, productID)
}

func KeyRequest(campaignID, productID uint, userID, requestID string) string {
	return fmt.Sprintf("fs:{c:%d:p:%d}:req:%s:%s", campaignID, productID, userID, requestID)
}

func KeyReservation(campaignID, productID uint, reservationID string) string {
	return fmt.Sprintf("fs:{c:%d:p:%d}:resv:%s", campaignID, productID, reservationID)
}

func KeyExpiry(campaignID, productID uint) string {
	return fmt.Sprintf("fs:{c:%d:p:%d}:expiry", campaignID, productID)
}

func KeyOrderStatus(reservationID string) string {
	return fmt.Sprintf("fs:order-status:%s", reservationID)
}

// -----------------------------------------------------------------------------
// Lua Scripts
// -----------------------------------------------------------------------------

// ReserveFlashSaleStockScript: Cluster-safe Atomic Reservation
var reserveFlashSaleStockScript = redis.NewScript(`
	local stock_key    = KEYS[1]
	local reserved_key = KEYS[2]
	local purchased_key= KEYS[3]
	local state_key    = KEYS[4]
	local config_key   = KEYS[5]
	local req_key      = KEYS[6]
	local resv_key     = KEYS[7]
	local expiry_key   = KEYS[8]

	local request_id   = ARGV[1]
	local fingerprint  = ARGV[2]
	local resv_id      = ARGV[3]
	local user_id      = ARGV[4]
	local qty          = tonumber(ARGV[5])
	local resv_ttl_arg = tonumber(ARGV[6])

	-- Helper trả về JSON format chuẩn
	local function result(code, extra)
		local t = { code = code }
		if extra then
			for k, v in pairs(extra) do t[k] = v end
		end
		return cjson.encode(t)
	end

	-- 0. Kiểm tra Marker CLOSED: Nếu reservation_id đã bị recovery đóng thì từ chối (Mục 3.3 - R1/R2/T05)
	local resv_status = redis.call('HGET', resv_key, 'status')
	if resv_status == 'CLOSED' then
		return result('RESERVATION_CLOSED')
	end

	-- 1. Kiểm tra Idempotency
	local cached_fp = redis.call('HGET', req_key, 'fingerprint')
	if cached_fp then
		if cached_fp == fingerprint then
			local cached_res = redis.call('HGET', req_key, 'result')
			return cached_res
		else
			return result('IDEMPOTENCY_KEY_REUSED')
		end
	end

	-- 2. Kiểm tra Thời gian và State bằng TIME của Redis Server (chống clock drift)
	local redis_time = redis.call('TIME')
	local now = tonumber(redis_time[1])

	local state = redis.call('GET', state_key)
	if state ~= 'ACTIVE' then
		return result('CAMPAIGN_NOT_ACTIVE')
	end

	local config = redis.call('HMGET', config_key, 'max_per_user', 'max_per_order', 'starts_at', 'ends_at', 'resv_sec')
	local max_per_user  = tonumber(config[1]) or 1
	local max_per_order = tonumber(config[2]) or 1
	local starts_at     = tonumber(config[3]) or 0
	local ends_at       = tonumber(config[4]) or 0
	local resv_sec      = tonumber(config[5]) or resv_ttl_arg or 120

	if now < starts_at or now >= ends_at then
		return result('CAMPAIGN_NOT_ACTIVE')
	end

	if not qty or qty <= 0 or qty > max_per_order then
		return result('INVALID_ORDER_QUANTITY')
	end

	-- 3. Kiểm tra Quota User: (Loại 1: max=1, Loại 2: max=N, hoặc max=0 là Unlimited)
	local current_reserved = tonumber(redis.call('HGET', reserved_key, user_id) or '0')
	local current_purchased= tonumber(redis.call('HGET', purchased_key, user_id) or '0')

	if max_per_user > 0 then
		if (current_reserved + current_purchased + qty) > max_per_user then
			return result('LIMIT_EXCEEDED')
		end
	end

	-- 4. Kiểm tra Tồn kho khả dụng
	local stock = tonumber(redis.call('GET', stock_key) or '0')
	if stock < qty then
		return result('SOLD_OUT')
	end

	-- 5. Thực thi Trừ Stock & Ghi nhận Reserved
	redis.call('DECRBY', stock_key, qty)
	redis.call('HINCRBY', reserved_key, user_id, qty)

	local expires_at = now + resv_sec

	-- Ghi chi tiết Reservation
	redis.call('HMSET', resv_key,
		'id', resv_id,
		'user_id', user_id,
		'quantity', qty,
		'status', 'RESERVED',
		'fingerprint', fingerprint,
		'created_at', now,
		'expires_at', expires_at
	)

	-- Thêm vào Expiry Sorted Set cục bộ
	redis.call('ZADD', expiry_key, expires_at, resv_id)

	-- Lưu Idempotency Result (TTL 24h)
	local resJSON = result('RESERVED', {
		reservation_id = resv_id,
		quantity = qty,
		expires_at = expires_at
	})

	redis.call('HMSET', req_key, 'fingerprint', fingerprint, 'result', resJSON)
	redis.call('EXPIRE', req_key, 86400)

	return resJSON
`)

// ConfirmFlashSaleReservationScript
var confirmFlashSaleReservationScript = redis.NewScript(`
	local reserved_key  = KEYS[1]
	local purchased_key = KEYS[2]
	local resv_key      = KEYS[3]
	local expiry_key    = KEYS[4]

	local resv_id = ARGV[1]

	local function result(code)
		return cjson.encode({ code = code })
	end

	local status = redis.call('HGET', resv_key, 'status')
	if status == 'CONFIRMED' then
		return result('ALREADY_CONFIRMED')
	end

	if status ~= 'RESERVED' and status ~= 'PROCESSING' then
		return result('INVALID_STATE')
	end

	local user_id = redis.call('HGET', resv_key, 'user_id')
	local qty = tonumber(redis.call('HGET', resv_key, 'quantity'))
	if not user_id or not qty or qty <= 0 then
		return result('INVARIANT_VIOLATION')
	end

	local current = tonumber(redis.call('HGET', reserved_key, user_id) or '0')
	if current < qty then
		return result('INVARIANT_VIOLATION')
	end

	local remaining = redis.call('HINCRBY', reserved_key, user_id, -qty)
	if remaining <= 0 then
		redis.call('HDEL', reserved_key, user_id)
	end

	redis.call('HINCRBY', purchased_key, user_id, qty)
	redis.call('HSET', resv_key, 'status', 'CONFIRMED')
	redis.call('ZREM', expiry_key, resv_id)

	return result('CONFIRMED')
`)

// ReleaseFlashSaleReservationScript
var releaseFlashSaleReservationScript = redis.NewScript(`
	local stock_key    = KEYS[1]
	local reserved_key = KEYS[2]
	local resv_key     = KEYS[3]
	local expiry_key   = KEYS[4]

	local resv_id    = ARGV[1]
	local new_status = ARGV[2] -- CANCELLED hoặc EXPIRED

	local function result(code)
		return cjson.encode({ code = code })
	end

	local status = redis.call('HGET', resv_key, 'status')
	if status == 'EXPIRED' or status == 'CANCELLED' then
		return result('ALREADY_RELEASED')
	end

	if status ~= 'RESERVED' and status ~= 'PROCESSING' then
		return result('INVALID_STATE')
	end

	local user_id = redis.call('HGET', resv_key, 'user_id')
	local qty = tonumber(redis.call('HGET', resv_key, 'quantity'))
	if not user_id or not qty or qty <= 0 then
		return result('INVARIANT_VIOLATION')
	end

	local current = tonumber(redis.call('HGET', reserved_key, user_id) or '0')
	if current < qty then
		return result('INVARIANT_VIOLATION')
	end

	redis.call('INCRBY', stock_key, qty)
	local remaining = redis.call('HINCRBY', reserved_key, user_id, -qty)
	if remaining <= 0 then
		redis.call('HDEL', reserved_key, user_id)
	end

	redis.call('HSET', resv_key, 'status', new_status)
	redis.call('ZREM', expiry_key, resv_id)

	return result('RELEASED')
`)

// CloseOrReleaseReservationScript đóng reservationID của generation cũ một cách nguyên tử (Mục 3.3 - R1/R2/T05):
// Nếu reservation đã tồn tại và RESERVED: giải phóng quota kho và đánh dấu CLOSED.
// Nếu reservation chưa tồn tại (request đến muộn): ghi marker status = 'CLOSED' để reserve muộn bị từ chối.
var closeOrReleaseReservationScript = redis.NewScript(`
	local stock_key    = KEYS[1]
	local reserved_key = KEYS[2]
	local resv_key     = KEYS[3]
	local expiry_key   = KEYS[4]

	local resv_id = ARGV[1]

	local function result(code)
		return cjson.encode({ code = code })
	end

	local status = redis.call('HGET', resv_key, 'status')
	if status == 'CLOSED' or status == 'CANCELLED' or status == 'EXPIRED' then
		return result('ALREADY_RELEASED')
	end

	if status == 'CONFIRMED' then
		return result('ALREADY_CONFIRMED')
	end

	if status == 'RESERVED' or status == 'PROCESSING' then
		local user_id = redis.call('HGET', resv_key, 'user_id')
		local qty = tonumber(redis.call('HGET', resv_key, 'quantity'))
		if user_id and qty and qty > 0 then
			local current = tonumber(redis.call('HGET', reserved_key, user_id) or '0')
			if current >= qty then
				redis.call('INCRBY', stock_key, qty)
				local remaining = redis.call('HINCRBY', reserved_key, user_id, -qty)
				if remaining <= 0 then
					redis.call('HDEL', reserved_key, user_id)
				end
			end
		end
		redis.call('ZREM', expiry_key, resv_id)
		redis.call('HSET', resv_key, 'status', 'CLOSED')
		return result('RELEASED')
	end

	-- Nếu chưa tồn tại reservation (late-arriving request): ghi marker CLOSED
	redis.call('HSET', resv_key, 'status', 'CLOSED')
	return result('RELEASED')
`)

// -----------------------------------------------------------------------------
// Go Typed Execution Helpers
// -----------------------------------------------------------------------------

func ReserveFlashSaleStock(
	ctx context.Context,
	rdb *redis.Client,
	campaignID, productID uint,
	userID, requestID, fingerprint, reservationID string,
	quantity int,
	resvSeconds int,
) (*FlashSaleResponse, error) {
	if rdb == nil {
		return nil, fmt.Errorf("redis client nil")
	}

	keys := []string{
		KeyStock(campaignID, productID),
		KeyReserved(campaignID, productID),
		KeyPurchased(campaignID, productID),
		KeyState(campaignID, productID),
		KeyConfig(campaignID, productID),
		KeyRequest(campaignID, productID, userID, requestID),
		KeyReservation(campaignID, productID, reservationID),
		KeyExpiry(campaignID, productID),
	}

	args := []interface{}{
		requestID,
		fingerprint,
		reservationID,
		userID,
		quantity,
		resvSeconds,
	}

	raw, err := reserveFlashSaleStockScript.Run(ctx, rdb, keys, args...).Result()
	if err != nil {
		return nil, err
	}

	jsonStr, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("kết quả trả về từ Lua không phải string: %v", raw)
	}

	var resp FlashSaleResponse
	if err := json.Unmarshal([]byte(jsonStr), &resp); err != nil {
		return nil, fmt.Errorf("lỗi unmarshal response Lua: %w, raw: %s", err, jsonStr)
	}

	return &resp, nil
}

func ConfirmFlashSaleReservation(
	ctx context.Context,
	rdb *redis.Client,
	campaignID, productID uint,
	reservationID string,
) (*FlashSaleResponse, error) {
	if rdb == nil {
		return nil, fmt.Errorf("redis client nil")
	}

	keys := []string{
		KeyReserved(campaignID, productID),
		KeyPurchased(campaignID, productID),
		KeyReservation(campaignID, productID, reservationID),
		KeyExpiry(campaignID, productID),
	}

	raw, err := confirmFlashSaleReservationScript.Run(ctx, rdb, keys, reservationID).Result()
	if err != nil {
		return nil, err
	}

	jsonStr, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("kết quả trả về từ Lua không phải string: %v", raw)
	}

	var resp FlashSaleResponse
	if err := json.Unmarshal([]byte(jsonStr), &resp); err != nil {
		return nil, fmt.Errorf("lỗi unmarshal response Lua: %w", err)
	}

	return &resp, nil
}

func ReleaseFlashSaleReservation(
	ctx context.Context,
	rdb *redis.Client,
	campaignID, productID uint,
	reservationID string,
	newStatus string,
) (*FlashSaleResponse, error) {
	if rdb == nil {
		return nil, fmt.Errorf("redis client nil")
	}

	keys := []string{
		KeyStock(campaignID, productID),
		KeyReserved(campaignID, productID),
		KeyReservation(campaignID, productID, reservationID),
		KeyExpiry(campaignID, productID),
	}

	raw, err := releaseFlashSaleReservationScript.Run(ctx, rdb, keys, reservationID, newStatus).Result()
	if err != nil {
		return nil, err
	}

	jsonStr, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("kết quả trả về từ Lua không phải string: %v", raw)
	}

	var resp FlashSaleResponse
	if err := json.Unmarshal([]byte(jsonStr), &resp); err != nil {
		return nil, fmt.Errorf("lỗi unmarshal response Lua: %w", err)
	}

	return &resp, nil
}

// CloseOrReleaseReservation đóng reservation nguyên tử: giải phóng nếu đã giữ, hoặc ghi marker CLOSED nếu chưa có (Mục 3.3)
func CloseOrReleaseReservation(
	ctx context.Context,
	rdb *redis.Client,
	campaignID, productID uint,
	reservationID string,
) (*FlashSaleResponse, error) {
	if rdb == nil {
		return nil, fmt.Errorf("redis client nil")
	}

	keys := []string{
		KeyStock(campaignID, productID),
		KeyReserved(campaignID, productID),
		KeyReservation(campaignID, productID, reservationID),
		KeyExpiry(campaignID, productID),
	}

	raw, err := closeOrReleaseReservationScript.Run(ctx, rdb, keys, reservationID).Result()
	if err != nil {
		return nil, err
	}

	jsonStr, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("kết quả trả về từ Lua không phải string: %v", raw)
	}

	var resp FlashSaleResponse
	if err := json.Unmarshal([]byte(jsonStr), &resp); err != nil {
		return nil, fmt.Errorf("lỗi unmarshal response Lua: %w", err)
	}

	return &resp, nil
}

// PrewarmCampaignItem khởi tạo dữ liệu item trên Redis trước khi kích hoạt Campaign
func PrewarmCampaignItem(
	ctx context.Context,
	rdb *redis.Client,
	campaignID, productID uint,
	stock int,
	maxPerUser int,
	maxPerOrder int,
	startsAt, endsAt int64,
	resvSec int,
	initialState string,
) error {
	if rdb == nil {
		return fmt.Errorf("redis client nil")
	}

	pipe := rdb.Pipeline()

	pipe.Set(ctx, KeyStock(campaignID, productID), stock, 0)
	pipe.Set(ctx, KeyState(campaignID, productID), initialState, 0)
	pipe.HSet(ctx, KeyConfig(campaignID, productID), map[string]interface{}{
		"max_per_user":  maxPerUser,
		"max_per_order": maxPerOrder,
		"starts_at":     startsAt,
		"ends_at":       endsAt,
		"resv_sec":      resvSec,
	})

	// TTL cho các key cấu hình và stock: kết thúc campaign + 7 ngày
	now := time.Now().Unix()
	ttlSec := (endsAt - now) + 7*86400
	if ttlSec > 0 {
		ttl := time.Duration(ttlSec) * time.Second
		pipe.Expire(ctx, KeyStock(campaignID, productID), ttl)
		pipe.Expire(ctx, KeyState(campaignID, productID), ttl)
		pipe.Expire(ctx, KeyConfig(campaignID, productID), ttl)
		pipe.Expire(ctx, KeyReserved(campaignID, productID), ttl)
		pipe.Expire(ctx, KeyPurchased(campaignID, productID), ttl)
		pipe.Expire(ctx, KeyExpiry(campaignID, productID), ttl)
	}

	_, err := pipe.Exec(ctx)
	return err
}
