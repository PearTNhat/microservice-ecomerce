package handlers

import (
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/api/rest/middlewares"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/dto"
	"ecomerce-service/pkg/response"
	"net/http"
	"strconv"

	"github.com/gofiber/fiber/v2"
)

type CartHandler struct {
	svc service.CartService
}

func SetupCartRoutes(rh *rest.RestHandler, svc service.CartService) {
	app := rh.App
	handler := CartHandler{svc: svc}

	authMiddleware := middlewares.RequireAuth(rh.Config.AppSecret)

	cartGroup := app.Group("/cart", authMiddleware)
	cartGroup.Get("/", handler.GetCart)
	cartGroup.Post("/add", handler.AddToCart)
	cartGroup.Put("/items/:id", handler.UpdateCartItem)
	cartGroup.Delete("/items/:id", handler.RemoveCartItem)
	cartGroup.Delete("/", handler.ClearCart)
}

func (h *CartHandler) GetCart(c *fiber.Ctx) error {
	userID, _ := c.Locals("userID").(string)
	if userID == "" {
		return response.Unauthorized(c, "Bạn chưa đăng nhập")
	}

	cart, err := h.svc.GetCart(c.UserContext(), userID)
	if err != nil {
		return response.InternalError(c, err.Error())
	}

	return response.Success(c, http.StatusOK, "Lấy giỏ hàng thành công", cart)
}

func (h *CartHandler) AddToCart(c *fiber.Ctx) error {
	userID, _ := c.Locals("userID").(string)
	if userID == "" {
		return response.Unauthorized(c, "Bạn chưa đăng nhập")
	}

	var req dto.AddToCartRequest
	if err := c.BodyParser(&req); err != nil {
		return response.BadRequest(c, "Dữ liệu yêu cầu không hợp lệ", "INVALID_BODY")
	}

	cart, err := h.svc.AddToCart(c.UserContext(), userID, &req)
	if err != nil {
		return response.BadRequest(c, err.Error(), "ADD_TO_CART_FAILED")
	}

	return response.Success(c, http.StatusOK, "Đã thêm sản phẩm vào giỏ hàng", cart)
}

func (h *CartHandler) UpdateCartItem(c *fiber.Ctx) error {
	userID, _ := c.Locals("userID").(string)
	if userID == "" {
		return response.Unauthorized(c, "Bạn chưa đăng nhập")
	}

	itemID, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "ID món hàng không hợp lệ", "INVALID_ITEM_ID")
	}

	var req dto.UpdateCartItemRequest
	if err := c.BodyParser(&req); err != nil {
		return response.BadRequest(c, "Dữ liệu yêu cầu không hợp lệ", "INVALID_BODY")
	}

	cart, err := h.svc.UpdateCartItem(c.UserContext(), userID, uint(itemID), req.Quantity)
	if err != nil {
		return response.BadRequest(c, err.Error(), "UPDATE_CART_FAILED")
	}

	return response.Success(c, http.StatusOK, "Đã cập nhật số lượng", cart)
}

func (h *CartHandler) RemoveCartItem(c *fiber.Ctx) error {
	userID, _ := c.Locals("userID").(string)
	if userID == "" {
		return response.Unauthorized(c, "Bạn chưa đăng nhập")
	}

	itemID, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "ID món hàng không hợp lệ", "INVALID_ITEM_ID")
	}

	cart, err := h.svc.RemoveCartItem(c.UserContext(), userID, uint(itemID))
	if err != nil {
		return response.BadRequest(c, err.Error(), "REMOVE_ITEM_FAILED")
	}

	return response.Success(c, http.StatusOK, "Đã xóa sản phẩm khỏi giỏ hàng", cart)
}

func (h *CartHandler) ClearCart(c *fiber.Ctx) error {
	userID, _ := c.Locals("userID").(string)
	if userID == "" {
		return response.Unauthorized(c, "Bạn chưa đăng nhập")
	}

	if err := h.svc.ClearCart(c.UserContext(), userID); err != nil {
		return response.InternalError(c, err.Error())
	}

	return response.Success(c, http.StatusOK, "Đã dọn sạch giỏ hàng", nil)
}
