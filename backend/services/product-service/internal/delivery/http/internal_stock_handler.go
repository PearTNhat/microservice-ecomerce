package http

import (
	"net/http"
	"strconv"

	"ecomerce-service/pkg/response"
	"ecomerce-service/pkg/server"
	"ecomerce-service/services/product-service/internal/domain"

	"github.com/gofiber/fiber/v2"
)

type InternalStockHandler struct {
	repo domain.StockAllocationRepository
}

type AllocateStockRequest struct {
	CampaignID uint `json:"campaign_id"`
	ProductID  uint `json:"product_id"`
	Quantity   int  `json:"quantity"`
}

func SetupInternalStockRoutes(rh *server.RestHandler, repo domain.StockAllocationRepository) {
	handler := &InternalStockHandler{repo: repo}

	internalGroup := rh.App.Group("/internal/stock-allocations")
	internalGroup.Post("/", handler.AllocateStock)
	internalGroup.Get("/:campaignId/:productId", handler.GetAllocation)
	internalGroup.Post("/:campaignId/:productId/release", handler.ReleaseStock)
}

func (h *InternalStockHandler) AllocateStock(c *fiber.Ctx) error {
	requestID := c.Get("Idempotency-Key")
	if requestID == "" {
		requestID = c.Get("X-Request-ID")
	}
	if requestID == "" {
		return response.BadRequest(c, "Thiếu header Idempotency-Key hoặc X-Request-ID", "MISSING_IDEMPOTENCY_KEY")
	}

	var req AllocateStockRequest
	if err := c.BodyParser(&req); err != nil {
		return response.BadRequest(c, "Payload không hợp lệ", "INVALID_BODY")
	}

	if req.CampaignID == 0 || req.ProductID == 0 || req.Quantity <= 0 {
		return response.BadRequest(c, "Thông tin phân bổ không hợp lệ", "INVALID_INPUT")
	}

	allocation, err := h.repo.AllocateStock(req.CampaignID, req.ProductID, requestID, req.Quantity)
	if err != nil {
		return response.BadRequest(c, "Lỗi phân bổ tồn kho: "+err.Error(), "ALLOCATE_FAILED")
	}

	return response.Success(c, http.StatusOK, "Phân bổ tồn kho thành công", allocation)
}

func (h *InternalStockHandler) ReleaseStock(c *fiber.Ctx) error {
	campaignIDStr := c.Params("campaignId")
	productIDStr := c.Params("productId")

	campaignID, err := strconv.ParseUint(campaignIDStr, 10, 32)
	if err != nil {
		return response.BadRequest(c, "campaignId không hợp lệ", "INVALID_CAMPAIGN_ID")
	}
	productID, err := strconv.ParseUint(productIDStr, 10, 32)
	if err != nil {
		return response.BadRequest(c, "productId không hợp lệ", "INVALID_PRODUCT_ID")
	}

	requestID := c.Get("Idempotency-Key")
	if requestID == "" {
		requestID = c.Get("X-Request-ID", "release-default")
	}

	released, err := h.repo.ReleaseStock(uint(campaignID), uint(productID), requestID)
	if err != nil {
		return response.BadRequest(c, "Lỗi hoàn trả tồn kho: "+err.Error(), "RELEASE_FAILED")
	}

	return response.Success(c, http.StatusOK, "Hoàn trả tồn kho thành công", fiber.Map{
		"released_quantity": released,
	})
}

func (h *InternalStockHandler) GetAllocation(c *fiber.Ctx) error {
	campaignIDStr := c.Params("campaignId")
	productIDStr := c.Params("productId")

	campaignID, err := strconv.ParseUint(campaignIDStr, 10, 32)
	if err != nil {
		return response.BadRequest(c, "campaignId không hợp lệ", "INVALID_CAMPAIGN_ID")
	}
	productID, err := strconv.ParseUint(productIDStr, 10, 32)
	if err != nil {
		return response.BadRequest(c, "productId không hợp lệ", "INVALID_PRODUCT_ID")
	}

	allocation, err := h.repo.FindByCampaignAndProduct(uint(campaignID), uint(productID))
	if err != nil {
		return response.NotFound(c, "Không tìm thấy thông tin phân bổ")
	}

	return response.Success(c, http.StatusOK, "Lấy thông tin phân bổ thành công", allocation)
}
