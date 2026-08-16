package middlewares

import (
	"ecomerce-service/pkg/logger"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

const HeaderXRequestID = "X-Request-ID"

// RequestIDMiddleware trích xuất hoặc sinh mới X-Request-ID cho mỗi request và ghi log
func RequestIDMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()

		// 1. Lấy Request-ID từ Client header hoặc tự tạo UUID v4
		reqID := c.Get(HeaderXRequestID)
		if reqID == "" {
			reqID = uuid.New().String()
		}

		// 2. Gán Request-ID vào context của Fiber và context chuẩn của Go
		c.Locals("request_id", reqID)
		ctx := logger.SetTraceID(c.UserContext(), reqID)
		c.SetUserContext(ctx)

		// 3. Đính kèm X-Request-ID vào Response header gửi về cho Client
		c.Set(HeaderXRequestID, reqID)

		// 4. Cho request đi tiếp tới Handler
		err := c.Next()

		// 5. Ghi Structured Log sau khi hoàn tất request
		latency := time.Since(start)
		status := c.Response().StatusCode()

		if err != nil {
			logger.ErrorContext(ctx, "HTTP Request Error",
				"method", c.Method(),
				"path", c.Path(),
				"status", status,
				"latency_ms", latency.Milliseconds(),
				"ip", c.IP(),
				"error", err.Error(),
			)
			return err
		}

		logger.InfoContext(ctx, "HTTP Request Completed",
			"method", c.Method(),
			"path", c.Path(),
			"status", status,
			"latency_ms", latency.Milliseconds(),
			"ip", c.IP(),
		)

		return nil
	}
}
