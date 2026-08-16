package middlewares

import (
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/utils"
	"strings"

	"github.com/gofiber/fiber/v2"
)

// RequireAuth là middleware kiểm tra JWT token hợp lệ
func RequireAuth(secret string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// 1. Lấy token từ header Authorization
		authHeader := c.Get("Authorization")
		if authHeader == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status":  "error",
				"message": "Không tìm thấy token xác thực (Missing Authorization Header)",
			})
		}

		// 2. Tách chữ "Bearer " ra khỏi token (định dạng: Bearer <token>)
		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status":  "error",
				"message": "Định dạng token không hợp lệ (Phải là 'Bearer <token>')",
			})
		}
		tokenString := parts[1]

		// 3. Giải mã và xác thực token
		claims, err := utils.VerifyToken(tokenString, secret)
		if err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status":  "error",
				"message": "Token không hợp lệ hoặc đã hết hạn",
			})
		}

		// 4. Trích xuất userID từ claims ("sub") và role từ claims ("role")
		userIDStr, ok := claims["sub"].(string)
		if !ok {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status":  "error",
				"message": "Token không chứa thông tin User ID",
			})
		}

		role, ok := claims["role"].(string)
		if !ok || role == "" {
			role = "CUSTOMER"
		}

		// Lưu userID & role vào context của Fiber (c.Locals)
		c.Locals("userID", userIDStr)
		c.Locals("role", role)

		// Cập nhật UserID vào logger context
		ctx := c.UserContext()
		ctx = logger.SetUserID(ctx, userIDStr)
		c.SetUserContext(ctx)

		// 5. Mọi thứ hợp lệ, cho phép đi tiếp tới Handler
		return c.Next()
	}
}
