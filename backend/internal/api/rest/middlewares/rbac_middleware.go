package middlewares

import (
	"ecomerce-service/pkg/response"

	"github.com/gofiber/fiber/v2"
)

// RequireRole là middleware kiểm tra quyền hạn (Role-Based Access Control)
func RequireRole(allowedRoles ...string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Lấy role từ c.Locals (đã được lưu ở RequireAuth)
		userRole, ok := c.Locals("role").(string)
		if !ok || userRole == "" {
			return response.Forbidden(c, "Truy cập bị từ chối: Không xác định được vai trò người dùng")
		}

		// Kiểm tra xem role của user có nằm trong danh sách cho phép không
		hasRole := false
		for _, role := range allowedRoles {
			if userRole == role {
				hasRole = true
				break
			}
		}

		if !hasRole {
			return response.Forbidden(c, "Truy cập bị từ chối: Bạn không có quyền thực hiện thao tác này")
		}

		return c.Next()
	}
}
