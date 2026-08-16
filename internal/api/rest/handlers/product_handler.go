package handlers

import (
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/dto"
	"net/http"
	"strconv"

	"github.com/gofiber/fiber/v2"
)

type ProductHandler struct {
	svc *service.ProductService
}

func SetupProductRoutes(rh *rest.RestHandler, svc *service.ProductService) {
	app := rh.App
	handler := ProductHandler{
		svc: svc,
	}

	// Public Product Endpoints (Dành cho người mua hàng)
	app.Get("/categories", handler.GetCategories)
	app.Get("/brands", handler.GetBrands)
	app.Get("/products", handler.GetProducts)
	app.Get("/products/:id", handler.GetProductDetail)
}

// GetCategories lấy danh mục sản phẩm (Điện lạnh, Điện gia dụng, Bếp...)
func (h *ProductHandler) GetCategories(ctx *fiber.Ctx) error {
	categories, err := h.svc.GetCategories(ctx.UserContext())
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": err.Error(),
		})
	}

	return ctx.Status(http.StatusOK).JSON(fiber.Map{
		"status": "success",
		"data":   categories,
	})
}

// GetBrands lấy danh sách thương hiệu (Daikin, Panasonic, Samsung, Bosch...)
func (h *ProductHandler) GetBrands(ctx *fiber.Ctx) error {
	brands, err := h.svc.GetBrands(ctx.UserContext())
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": err.Error(),
		})
	}

	return ctx.Status(http.StatusOK).JSON(fiber.Map{
		"status": "success",
		"data":   brands,
	})
}

// GetProducts lấy danh sách sản phẩm với bộ lọc động và phân trang
func (h *ProductHandler) GetProducts(ctx *fiber.Ctx) error {
	var filterReq dto.ProductFilterRequest

	if err := ctx.QueryParser(&filterReq); err != nil {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{
			"status":  "error",
			"message": "Tham số lọc không hợp lệ",
		})
	}

	resp, err := h.svc.GetProducts(filterReq)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": err.Error(),
		})
	}

	return ctx.Status(http.StatusOK).JSON(fiber.Map{
		"status": "success",
		"data":   resp,
	})
}

// GetProductDetail xem chi tiết sản phẩm và bảng thông số kỹ thuật điện máy
func (h *ProductHandler) GetProductDetail(ctx *fiber.Ctx) error {
	idParam := ctx.Params("id")
	id, err := strconv.ParseUint(idParam, 10, 32)
	if err != nil {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{
			"status":  "error",
			"message": "ID sản phẩm không hợp lệ",
		})
	}

	// Lấy userID nếu khách đã đăng nhập (không bắt buộc)
	userID := ""
	if uid, ok := ctx.Locals("userID").(string); ok {
		userID = uid
	}

	product, err := h.svc.GetProductDetail(ctx.UserContext(), uint(id), userID)
	if err != nil {
		return ctx.Status(http.StatusNotFound).JSON(fiber.Map{
			"status":  "error",
			"message": err.Error(),
		})
	}

	return ctx.Status(http.StatusOK).JSON(fiber.Map{
		"status": "success",
		"data":   product,
	})
}
