package response

import (
	"net/http"

	"github.com/gofiber/fiber/v2"
)

// Response cấu trúc JSON trả về chuẩn mực cho toàn bộ hệ thống Microservices
type Response struct {
	Status    string      `json:"status"`               // "success" hoặc "error"
	Message   string      `json:"message,omitempty"`     // Thông báo dễ đọc cho người dùng
	Data      interface{} `json:"data,omitempty"`        // Dữ liệu payload
	ErrorCode string      `json:"error_code,omitempty"`  // Mã lỗi kỹ thuật (VD: "USER_NOT_FOUND", "INVALID_OTP")
	Meta      interface{} `json:"meta,omitempty"`        // Thông tin phân trang, tổng số bản ghi
}

// Success trả về phản hồi thành công kèm dữ liệu (Mặc định 200 OK nếu statusCode = 0)
func Success(c *fiber.Ctx, statusCode int, message string, data interface{}) error {
	if statusCode == 0 {
		statusCode = http.StatusOK
	}

	return c.Status(statusCode).JSON(Response{
		Status:  "success",
		Message: message,
		Data:    data,
	})
}

// SuccessWithMeta trả về phản hồi thành công kèm dữ liệu và thông tin phân trang (Meta)
func SuccessWithMeta(c *fiber.Ctx, statusCode int, message string, data interface{}, meta interface{}) error {
	if statusCode == 0 {
		statusCode = http.StatusOK
	}

	return c.Status(statusCode).JSON(Response{
		Status:  "success",
		Message: message,
		Data:    data,
		Meta:    meta,
	})
}

// Error trả về phản hồi lỗi với mã trạng thái HTTP tùy chỉnh
func Error(c *fiber.Ctx, statusCode int, message string, errorCode ...string) error {
	if statusCode == 0 {
		statusCode = http.StatusInternalServerError
	}

	code := ""
	if len(errorCode) > 0 {
		code = errorCode[0]
	}

	return c.Status(statusCode).JSON(Response{
		Status:    "error",
		Message:   message,
		ErrorCode: code,
	})
}

// BadRequest trả về lỗi 400 (Dữ liệu đầu vào không hợp lệ)
func BadRequest(c *fiber.Ctx, message string, errorCode ...string) error {
	return Error(c, http.StatusBadRequest, message, errorCode...)
}

// Unauthorized trả về lỗi 401 (Chưa đăng nhập / Token không hợp lệ hoặc hết hạn)
func Unauthorized(c *fiber.Ctx, message string) error {
	return Error(c, http.StatusUnauthorized, message, "UNAUTHORIZED")
}

// Forbidden trả về lỗi 403 (Không có quyền truy cập chức năng này)
func Forbidden(c *fiber.Ctx, message string) error {
	return Error(c, http.StatusForbidden, message, "FORBIDDEN")
}

// NotFound trả về lỗi 404 (Không tìm thấy tài nguyên)
func NotFound(c *fiber.Ctx, message string) error {
	return Error(c, http.StatusNotFound, message, "NOT_FOUND")
}

// InternalError trả về lỗi 500 (Lỗi máy chủ nội bộ)
func InternalError(c *fiber.Ctx, message string) error {
	return Error(c, http.StatusInternalServerError, message, "INTERNAL_SERVER_ERROR")
}
