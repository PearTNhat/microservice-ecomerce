package http

import (
	"ecomerce-service/pkg/middlewares"
	"ecomerce-service/pkg/response"
	"ecomerce-service/pkg/server"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/dto"
	"ecomerce-service/services/order-service/internal/service"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

type OrderHandler struct {
	svc service.OrderService
}

func SetupOrderRoutes(rh *server.RestHandler, svc service.OrderService, redisClient *redis.Client) {
	app := rh.App
	handler := OrderHandler{svc: svc}

	authMiddleware := middlewares.RequireAuth(rh.Config.AppSecret)

	orderGroup := app.Group("/orders", authMiddleware)
	orderGroup.Post("/", middlewares.IdempotencyMiddleware(redisClient), handler.CreateOrder)
	orderGroup.Post("/checkout", middlewares.IdempotencyMiddleware(redisClient), handler.CreateOrder)
	orderGroup.Post("/checkout/quote", handler.GetBasketQuote) // 17.1: Báo giá trước khi đặt hàng giỏ hàng
	orderGroup.Post("/direct", middlewares.IdempotencyMiddleware(redisClient), handler.CreateOrder)

	// Flash Sale Routes
	orderGroup.Post("/flash-sale", handler.CreateFlashSaleOrder)
	orderGroup.Get("/flash-sale/status/:token", handler.GetFlashSaleStatus)
	orderGroup.Post("/flash-sale/prewarm", middlewares.RequireRole(domain.RoleAdmin), handler.PrewarmStock)

	orderGroup.Get("/", handler.GetUserOrders)
	orderGroup.Get("/:id", handler.GetOrderByID)
	orderGroup.Put("/:id/status", middlewares.RequireRole(domain.RoleAdmin, domain.RoleTechnician), handler.UpdateOrderStatus)
}

func (h *OrderHandler) CreateOrder(c *fiber.Ctx) error {
	userID, _ := c.Locals("userID").(string)
	if userID == "" {
		return response.Unauthorized(c, "Bạn chưa đăng nhập")
	}

	var req dto.CreateOrderRequest
	if err := c.BodyParser(&req); err != nil {
		return response.BadRequest(c, "Thông tin đặt hàng không hợp lệ", "INVALID_BODY")
	}

	order, err := h.svc.CreateOrder(c.UserContext(), userID, &req)
	if err != nil {
		if conflictErr, ok := err.(*service.PriceConflictError); ok {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"status":     "error",
				"message":    conflictErr.Message,
				"error_code": conflictErr.ErrorCode,
				"data":       conflictErr.Response,
			})
		}
		errStr := err.Error()
		if strings.Contains(errStr, "FLASH_SALE_OUT_OF_STOCK") ||
			strings.Contains(errStr, "PRICE_CHANGED") ||
			strings.Contains(errStr, "FLASH_SALE_QUOTA_EXCEEDED") ||
			strings.Contains(errStr, "QUOTE_EXPIRED") {
			return response.Error(c, fiber.StatusConflict, errStr, "CONFLICT_REQUOTE_REQUIRED")
		}
		return response.BadRequest(c, errStr, "CREATE_ORDER_FAILED")
	}

	return response.Success(c, http.StatusCreated, "Đặt hàng thành công", order)
}

// GetBasketQuote lấy báo giá chính xác cho giỏ hàng kèm QuoteToken (17.1)
func (h *OrderHandler) GetBasketQuote(c *fiber.Ctx) error {
	userID, _ := c.Locals("userID").(string)
	if userID == "" {
		return response.Unauthorized(c, "Bạn chưa đăng nhập")
	}

	var req dto.BasketQuoteRequest
	if err := c.BodyParser(&req); err != nil {
		return response.BadRequest(c, "Thông tin yêu cầu báo giá không hợp lệ", "INVALID_BODY")
	}

	quote, err := h.svc.GetBasketQuote(c.UserContext(), userID, &req)
	if err != nil {
		return response.BadRequest(c, err.Error(), "GET_QUOTE_FAILED")
	}

	return response.Success(c, http.StatusOK, "Báo giá giỏ hàng thành công", quote)
}

func (h *OrderHandler) CreateFlashSaleOrder(c *fiber.Ctx) error {
	userID, _ := c.Locals("userID").(string)
	if userID == "" {
		return response.Unauthorized(c, "Bạn chưa đăng nhập")
	}

	var req dto.FlashSaleOrderRequest
	if err := c.BodyParser(&req); err != nil {
		return response.BadRequest(c, "Thông tin đặt hàng Flash Sale không hợp lệ", "INVALID_BODY")
	}

	res, err := h.svc.CreateFlashSaleOrderAsync(c.UserContext(), userID, &req)
	if err != nil {
		return response.BadRequest(c, err.Error(), "FLASH_SALE_FAILED")
	}

	return response.Success(c, http.StatusAccepted, "Đã tiếp nhận đơn hàng Flash Sale vào hàng đợi", res)
}

func (h *OrderHandler) GetFlashSaleStatus(c *fiber.Ctx) error {
	token := c.Params("token")
	if token == "" {
		return response.BadRequest(c, "Mã token không hợp lệ", "INVALID_TOKEN")
	}

	status, err := h.svc.GetFlashSaleOrderStatus(c.UserContext(), token)
	if err != nil {
		return response.InternalError(c, "Lỗi kiểm tra trạng thái: "+err.Error())
	}

	return response.Success(c, http.StatusOK, "Lấy trạng thái đơn hàng thành công", status)
}

func (h *OrderHandler) PrewarmStock(c *fiber.Ctx) error {
	var req dto.PrewarmStockRequest
	if err := c.BodyParser(&req); err != nil {
		return response.BadRequest(c, "Dữ liệu nạp tồn kho không hợp lệ", "INVALID_BODY")
	}

	if err := h.svc.PrewarmStock(c.UserContext(), req.ProductID, req.Stock); err != nil {
		return response.BadRequest(c, err.Error(), "PREWARM_FAILED")
	}

	return response.Success(c, http.StatusOK, fmt.Sprintf("Nạp thành công %d sản phẩm vào Redis Flash Sale", req.Stock), nil)
}

func (h *OrderHandler) GetUserOrders(c *fiber.Ctx) error {
	userID, _ := c.Locals("userID").(string)
	if userID == "" {
		return response.Unauthorized(c, "Bạn chưa đăng nhập")
	}

	page, _ := strconv.Atoi(c.Query("page", "1"))
	limit, _ := strconv.Atoi(c.Query("limit", "10"))

	orders, err := h.svc.GetUserOrders(c.UserContext(), userID, page, limit)
	if err != nil {
		return response.InternalError(c, "Lỗi lấy danh sách đơn hàng: "+err.Error())
	}

	return response.Success(c, http.StatusOK, "Lấy danh sách đơn hàng thành công", orders)
}

func (h *OrderHandler) GetOrderByID(c *fiber.Ctx) error {
	userID, _ := c.Locals("userID").(string)
	userRole, _ := c.Locals("userRole").(string)
	if userID == "" {
		return response.Unauthorized(c, "Bạn chưa đăng nhập")
	}

	idParam := c.Params("id")
	id, err := strconv.ParseUint(idParam, 10, 32)
	if err != nil {
		return response.BadRequest(c, "ID đơn hàng không hợp lệ", "INVALID_ID")
	}

	order, err := h.svc.GetOrderByID(c.UserContext(), uint(id), userID, userRole)
	if err != nil {
		return response.NotFound(c, err.Error())
	}

	return response.Success(c, http.StatusOK, "Lấy thông tin đơn hàng thành công", order)
}

func (h *OrderHandler) UpdateOrderStatus(c *fiber.Ctx) error {
	idParam := c.Params("id")
	id, err := strconv.ParseUint(idParam, 10, 32)
	if err != nil {
		return response.BadRequest(c, "ID đơn hàng không hợp lệ", "INVALID_ID")
	}

	var req dto.UpdateOrderStatusRequest
	if err := c.BodyParser(&req); err != nil {
		return response.BadRequest(c, "Dữ liệu cập nhật không hợp lệ", "INVALID_BODY")
	}

	if err := h.svc.UpdateOrderStatus(c.UserContext(), uint(id), &req); err != nil {
		return response.BadRequest(c, err.Error(), "UPDATE_STATUS_FAILED")
	}

	return response.Success(c, http.StatusOK, "Cập nhật trạng thái đơn hàng thành công", nil)
}
