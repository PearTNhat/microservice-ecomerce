package middlewares

import (
	"context"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/response"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

const (
	IdempotencyStatusProcessing = "PROCESSING"
	IdempotencyStatusDone       = "DONE"
	IdempotencyTTL              = 2 * time.Minute
)

// IdempotencyMiddleware chống việc gửi lặp lại request (Double Click / Network Retry)
func IdempotencyMiddleware(redisClient *redis.Client) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if redisClient == nil {
			return c.Next()
		}

		// 1. Lấy Idempotency Key từ Headers
		key := c.Get("X-Idempotency-Key")
		if key == "" {
			key = c.Get("Idempotency-Key")
		}

		// Nếu client không truyền key, cho phép tiếp tục
		if key == "" {
			return c.Next()
		}

		ctx := c.UserContext()
		redisKey := fmt.Sprintf("idempotency:%s", key)

		// 2. Thử thiết lập key với cờ NX (Not Exists)
		success, err := redisClient.SetNX(ctx, redisKey, IdempotencyStatusProcessing, IdempotencyTTL).Result()
		if err != nil {
			logger.WarnContext(ctx, "⚠️ Không thể kiểm tra Idempotency trên Redis", "error", err.Error())
			return c.Next()
		}

		if !success {
			// Key đã tồn tại -> Kiểm tra trạng thái hiện tại
			status, _ := redisClient.Get(ctx, redisKey).Result()
			if status == IdempotencyStatusProcessing {
				return response.Error(c, fiber.StatusConflict, "Yêu cầu của bạn đang được xử lý, vui lòng không bấm gửi lại liên tục!")
			}
			return response.Error(c, fiber.StatusConflict, "Yêu cầu này đã được thực hiện thành công trước đó (Trùng lặp Idempotency-Key).")
		}

		// 3. Thực thi handler tiếp theo
		execErr := c.Next()

		// 4. Kiểm tra mã trạng thái trả về để cập nhật hoặc giải phóng key
		statusCode := c.Response().StatusCode()
		if statusCode >= 200 && statusCode < 300 {
			// Thành công -> Lưu trạng thái DONE trong 10 phút
			_ = redisClient.Set(context.Background(), redisKey, IdempotencyStatusDone, 10*time.Minute).Err()
		} else {
			// Lỗi -> Xóa key để client có thể thử lại
			_ = redisClient.Del(context.Background(), redisKey).Err()
		}

		return execErr
	}
}
