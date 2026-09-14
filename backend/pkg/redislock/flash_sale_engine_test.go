package redislock

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFlashSaleEngine_BasicReserveConfirmRelease(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	campaignID := uint(1)
	productID := uint(100)
	now := time.Now().Unix()

	// 1. Prewarm item: 5 stock, max_per_user = 1, max_per_order = 1
	err := PrewarmCampaignItem(ctx, client, campaignID, productID, 5, 1, 1, now-10, now+3600, 120, "ACTIVE")
	assert.NoError(t, err)

	// 2. User 1 đặt mua 1 món -> Phải thành công (RESERVED)
	resp, err := ReserveFlashSaleStock(
		ctx, client,
		campaignID, productID,
		"user-1", "req-1", "fp-1", "FSR-1",
		1, 120,
	)
	assert.NoError(t, err)
	assert.Equal(t, ResultReserved, resp.Code)
	assert.Equal(t, "FSR-1", resp.ReservationID)

	// 3. User 1 đặt mua tiếp -> Phải bị chặn LIMIT_EXCEEDED (vì max_per_user = 1)
	respLimit, err := ReserveFlashSaleStock(
		ctx, client,
		campaignID, productID,
		"user-1", "req-2", "fp-2", "FSR-2",
		1, 120,
	)
	assert.NoError(t, err)
	assert.Equal(t, ResultLimitExceeded, respLimit.Code)

	// 4. Idempotency: Gửi lại cùng req-1 và fp-1 -> Trả lại kết quả RESERVED cũ
	respDup, err := ReserveFlashSaleStock(
		ctx, client,
		campaignID, productID,
		"user-1", "req-1", "fp-1", "FSR-1",
		1, 120,
	)
	assert.NoError(t, err)
	assert.Equal(t, ResultReserved, respDup.Code)
	assert.Equal(t, "FSR-1", respDup.ReservationID)

	// 5. Cùng req-1 nhưng đổi fingerprint (đổi body) -> IDEMPOTENCY_KEY_REUSED
	respReused, err := ReserveFlashSaleStock(
		ctx, client,
		campaignID, productID,
		"user-1", "req-1", "fp-DIFFERENT", "FSR-1",
		1, 120,
	)
	assert.NoError(t, err)
	assert.Equal(t, ResultIdempotencyKeyReused, respReused.Code)

	// 6. Confirm Reservation
	confirmResp, err := ConfirmFlashSaleReservation(ctx, client, campaignID, productID, "FSR-1")
	assert.NoError(t, err)
	assert.Equal(t, ResultConfirmed, confirmResp.Code)

	// Confirm lại lần 2 -> ALREADY_CONFIRMED
	confirmAgain, err := ConfirmFlashSaleReservation(ctx, client, campaignID, productID, "FSR-1")
	assert.NoError(t, err)
	assert.Equal(t, ResultAlreadyConfirmed, confirmAgain.Code)

	// 7. User 2 đặt mua và hủy (Release)
	resp2, err := ReserveFlashSaleStock(
		ctx, client,
		campaignID, productID,
		"user-2", "req-3", "fp-3", "FSR-3",
		1, 120,
	)
	assert.NoError(t, err)
	assert.Equal(t, ResultReserved, resp2.Code)

	// Release
	relResp, err := ReleaseFlashSaleReservation(ctx, client, campaignID, productID, "FSR-3", "CANCELLED")
	assert.NoError(t, err)
	assert.Equal(t, ResultReleased, relResp.Code)

	// Sau khi release, User 2 có thể đặt lại vì quota đã được hoàn lại
	resp2Retry, err := ReserveFlashSaleStock(
		ctx, client,
		campaignID, productID,
		"user-2", "req-4", "fp-4", "FSR-4",
		1, 120,
	)
	assert.NoError(t, err)
	assert.Equal(t, ResultReserved, resp2Retry.Code)
}

func TestFlashSaleEngine_MultiplePurchasesQuota(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	campaignID := uint(2)
	productID := uint(200)
	now := time.Now().Unix()

	// Cấu hình Loại 2: max_per_user = 3 (cho phép mua nhiều lần miễn tổng <= 3), max_per_order = 2
	err := PrewarmCampaignItem(ctx, client, campaignID, productID, 20, 3, 2, now-10, now+3600, 120, "ACTIVE")
	assert.NoError(t, err)

	// Lần 1: Mua 2 cái -> Thành công
	resp1, err := ReserveFlashSaleStock(ctx, client, campaignID, productID, "user-multi", "req-m1", "fp-m1", "FSR-M1", 2, 120)
	assert.NoError(t, err)
	assert.Equal(t, ResultReserved, resp1.Code)

	// Lần 2: Mua tiếp 1 cái -> Thành công (tổng 3)
	resp2, err := ReserveFlashSaleStock(ctx, client, campaignID, productID, "user-multi", "req-m2", "fp-m2", "FSR-M2", 1, 120)
	assert.NoError(t, err)
	assert.Equal(t, ResultReserved, resp2.Code)

	// Lần 3: Mua thêm 1 cái nữa -> LIMIT_EXCEEDED (tổng 4 > 3)
	resp3, err := ReserveFlashSaleStock(ctx, client, campaignID, productID, "user-multi", "req-m3", "fp-m3", "FSR-M3", 1, 120)
	assert.NoError(t, err)
	assert.Equal(t, ResultLimitExceeded, resp3.Code)
}

func TestFlashSaleEngine_Concurrency_ZeroOversell(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	campaignID := uint(3)
	productID := uint(300)
	now := time.Now().Unix()

	stock := 10
	totalUsers := 50

	err := PrewarmCampaignItem(ctx, client, campaignID, productID, stock, 1, 1, now-10, now+3600, 120, "ACTIVE")
	assert.NoError(t, err)

	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0
	soldOutCount := 0

	for i := 0; i < totalUsers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			userID := fmt.Sprintf("concurrent-user-%d", idx)
			reqID := fmt.Sprintf("req-con-%d", idx)
			fp := fmt.Sprintf("fp-con-%d", idx)
			resvID := fmt.Sprintf("FSR-CON-%d", idx)

			resp, err := ReserveFlashSaleStock(ctx, client, campaignID, productID, userID, reqID, fp, resvID, 1, 120)
			if err == nil {
				mu.Lock()
				if resp.Code == ResultReserved {
					successCount++
				} else if resp.Code == ResultSoldOut {
					soldOutCount++
				}
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	assert.Equal(t, stock, successCount, "Chính xác 10 suất được reserve, không bán âm")
	assert.Equal(t, totalUsers-stock, soldOutCount, "40 request còn lại bị báo SOLD_OUT")
}
