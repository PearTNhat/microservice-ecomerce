package handlers

import (
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/api/rest/middlewares"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/dto"
	"ecomerce-service/pkg/response"
	"net/http"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

type OrderHandler struct {
	svc service.OrderService
}

func SetupOrderRoutes(rh *rest.RestHandler, svc service.OrderService, redisClient *redis.Client) {
	app := rh.App
	handler := OrderHandler{svc: svc}

	authMiddleware := middlewares.RequireAuth(rh.Config.AppSecret)

	orderGroup := app.Group("/orders", authMiddleware)
	orderGroup.Post("/", middlewares.IdempotencyMiddleware(redisClient), handler.CreateOrder)
	orderGroup.Post("/checkout", middlewares.IdempotencyMiddleware(redisClient), handler.CreateOrder)
	orderGroup.Post("/direct", middlewares.IdempotencyMiddleware(redisClient), handler.CreateOrder)
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
		return response.BadRequest(c, err.Error(), "CREATE_ORDER_FAILED")
	}

	return response.Success(c, http.StatusCreated, "Đặt hàng thành công", order)
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
		return response.InternalError(c, err.Error())
	}

	return response.Success(c, http.StatusOK, "Lấy danh sách đơn hàng thành công", orders)
}

func (h *OrderHandler) GetOrderByID(c *fiber.Ctx) error {
	userID, _ := c.Locals("userID").(string)
	role, _ := c.Locals("role").(string)
	if userID == "" {
		return response.Unauthorized(c, "Bạn chưa đăng nhập")
	}

	orderID, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "ID đơn hàng không hợp lệ", "INVALID_ORDER_ID")
	}

	order, err := h.svc.GetOrderByID(c.UserContext(), uint(orderID), userID, role)
	if err != nil {
		return response.BadRequest(c, err.Error(), "ORDER_NOT_FOUND")
	}

	return response.Success(c, http.StatusOK, "Lấy chi tiết đơn hàng thành công", order)
}

func (h *OrderHandler) UpdateOrderStatus(c *fiber.Ctx) error {
	orderID, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "ID đơn hàng không hợp lệ", "INVALID_ORDER_ID")
	}

	var req dto.UpdateOrderStatusRequest
	if err := c.BodyParser(&req); err != nil {
		return response.BadRequest(c, "Dữ liệu yêu cầu không hợp lệ", "INVALID_BODY")
	}

	if err := h.svc.UpdateOrderStatus(c.UserContext(), uint(orderID), &req); err != nil {
		return response.BadRequest(c, err.Error(), "UPDATE_STATUS_FAILED")
	}

	return response.Success(c, http.StatusOK, "Cập nhật trạng thái đơn hàng thành công", nil)
}
