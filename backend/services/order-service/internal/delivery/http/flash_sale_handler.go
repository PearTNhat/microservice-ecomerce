package http

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"ecomerce-service/pkg/middlewares"
	"ecomerce-service/pkg/redislock"
	"ecomerce-service/pkg/response"
	"ecomerce-service/pkg/server"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/dto"
	"ecomerce-service/services/order-service/internal/service"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
)

type FlashSaleHandler struct {
	svc         service.FlashSaleService
	redisClient *redis.Client
}

func SetupFlashSaleRoutes(rh *server.RestHandler, svc service.FlashSaleService, redisClient *redis.Client) {
	app := rh.App
	handler := &FlashSaleHandler{svc: svc, redisClient: redisClient}

	authMiddleware := middlewares.RequireAuth(rh.Config.AppSecret)
	adminMiddleware := middlewares.RequireRole(domain.RoleAdmin)

	// Admin API
	adminGroup := app.Group("/admin/flash-sales", authMiddleware, adminMiddleware)
	adminGroup.Post("/", handler.CreateCampaign)
	adminGroup.Get("/", handler.ListCampaigns)
	adminGroup.Get("/:campaignId", handler.GetCampaign)
	adminGroup.Put("/:campaignId", handler.UpdateCampaign)
	adminGroup.Post("/:campaignId/items", handler.AddItem)
	adminGroup.Put("/:campaignId/items/:itemId", handler.UpdateItem)
	adminGroup.Delete("/:campaignId/items/:itemId", handler.DeleteItem)
	adminGroup.Post("/:campaignId/activate", handler.ActivateCampaign)
	adminGroup.Post("/:campaignId/end", handler.EndCampaign)
	adminGroup.Post("/:campaignId/clone", handler.CloneCampaign)

	// Customer & Public API
	fsGroup := app.Group("/flash-sales")
	fsGroup.Get("/active", handler.GetActiveCampaign)
	fsGroup.Get("/offers/batch", handler.GetBatchOffers)
	fsGroup.Post("/offers/batch", handler.GetBatchOffers)
	fsGroup.Get("/offers/:productId", handler.GetProductOffer)
	fsGroup.Post("/:campaignId/items/:productId/orders", authMiddleware, handler.ReserveOrder)
	fsGroup.Get("/orders/:reservationId", authMiddleware, handler.GetOrderStatus)
	fsGroup.Get("/orders/:reservationId/stream", handler.StreamOrderStatus)
}

func (h *FlashSaleHandler) GetActiveCampaign(c *fiber.Ctx) error {
	resp, err := h.svc.GetActiveCampaign(c.UserContext())
	if err != nil {
		return response.Error(c, http.StatusInternalServerError, err.Error(), "GET_ACTIVE_CAMPAIGN_FAILED")
	}
	if resp == nil {
		return response.Success(c, http.StatusOK, "Hiện tại không có chiến dịch Flash Sale nào đang diễn ra", nil)
	}
	return response.Success(c, http.StatusOK, "Lấy thông tin chiến dịch Flash Sale thành công", resp)
}

func (h *FlashSaleHandler) CreateCampaign(c *fiber.Ctx) error {
	var req dto.CreateCampaignRequest
	if err := c.BodyParser(&req); err != nil {
		return response.BadRequest(c, "Dữ liệu campaign không hợp lệ", "INVALID_BODY")
	}

	camp, err := h.svc.CreateCampaign(c.UserContext(), &req)
	if err != nil {
		return response.BadRequest(c, err.Error(), "CREATE_CAMPAIGN_FAILED")
	}

	return response.Success(c, http.StatusCreated, "Tạo chiến dịch Flash Sale thành công", camp)
}

func (h *FlashSaleHandler) UpdateCampaign(c *fiber.Ctx) error {
	campaignID, err := strconv.ParseUint(c.Params("campaignId"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "campaignId không hợp lệ", "INVALID_ID")
	}

	var req dto.UpdateCampaignRequest
	if err := c.BodyParser(&req); err != nil {
		return response.BadRequest(c, "Dữ liệu cập nhật không hợp lệ", "INVALID_BODY")
	}

	camp, err := h.svc.UpdateCampaign(c.UserContext(), uint(campaignID), &req)
	if err != nil {
		return response.BadRequest(c, err.Error(), "UPDATE_CAMPAIGN_FAILED")
	}

	return response.Success(c, http.StatusOK, "Cập nhật chiến dịch Flash Sale thành công", camp)
}

func (h *FlashSaleHandler) AddItem(c *fiber.Ctx) error {
	campaignID, err := strconv.ParseUint(c.Params("campaignId"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "campaignId không hợp lệ", "INVALID_ID")
	}

	var req dto.AddFlashSaleItemRequest
	if err := c.BodyParser(&req); err != nil {
		return response.BadRequest(c, "Dữ liệu item không hợp lệ", "INVALID_BODY")
	}

	item, err := h.svc.AddItem(c.UserContext(), uint(campaignID), &req)
	if err != nil {
		return response.BadRequest(c, err.Error(), "ADD_ITEM_FAILED")
	}

	return response.Success(c, http.StatusCreated, "Thêm sản phẩm vào Flash Sale thành công", item)
}

func (h *FlashSaleHandler) UpdateItem(c *fiber.Ctx) error {
	campaignID, err := strconv.ParseUint(c.Params("campaignId"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "campaignId không hợp lệ", "INVALID_ID")
	}
	itemID, err := strconv.ParseUint(c.Params("itemId"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "itemId không hợp lệ", "INVALID_ID")
	}

	var req dto.UpdateFlashSaleItemRequest
	if err := c.BodyParser(&req); err != nil {
		return response.BadRequest(c, "Dữ liệu item không hợp lệ", "INVALID_BODY")
	}

	item, err := h.svc.UpdateItem(c.UserContext(), uint(campaignID), uint(itemID), &req)
	if err != nil {
		return response.BadRequest(c, err.Error(), "UPDATE_ITEM_FAILED")
	}

	return response.Success(c, http.StatusOK, "Cập nhật sản phẩm Flash Sale thành công", item)
}

func (h *FlashSaleHandler) DeleteItem(c *fiber.Ctx) error {
	campaignID, err := strconv.ParseUint(c.Params("campaignId"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "campaignId không hợp lệ", "INVALID_ID")
	}
	itemID, err := strconv.ParseUint(c.Params("itemId"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "itemId không hợp lệ", "INVALID_ID")
	}

	if err := h.svc.DeleteItem(c.UserContext(), uint(campaignID), uint(itemID)); err != nil {
		return response.BadRequest(c, err.Error(), "DELETE_ITEM_FAILED")
	}

	return response.Success(c, http.StatusOK, "Xóa sản phẩm khỏi chiến dịch thành công", nil)
}

func (h *FlashSaleHandler) ActivateCampaign(c *fiber.Ctx) error {
	campaignID, err := strconv.ParseUint(c.Params("campaignId"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "campaignId không hợp lệ", "INVALID_ID")
	}

	if err := h.svc.ActivateCampaign(c.UserContext(), uint(campaignID)); err != nil {
		return response.BadRequest(c, err.Error(), "ACTIVATE_FAILED")
	}

	return response.Success(c, http.StatusOK, "Kích hoạt chiến dịch Flash Sale thành công", nil)
}

func (h *FlashSaleHandler) EndCampaign(c *fiber.Ctx) error {
	campaignID, err := strconv.ParseUint(c.Params("campaignId"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "campaignId không hợp lệ", "INVALID_ID")
	}

	if err := h.svc.EndCampaign(c.UserContext(), uint(campaignID)); err != nil {
		return response.BadRequest(c, err.Error(), "END_FAILED")
	}

	return response.Success(c, http.StatusOK, "Kết thúc chiến dịch Flash Sale thành công", nil)
}

func (h *FlashSaleHandler) CloneCampaign(c *fiber.Ctx) error {
	campaignID, err := strconv.ParseUint(c.Params("campaignId"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "campaignId không hợp lệ", "INVALID_ID")
	}

	cloned, err := h.svc.CloneCampaign(c.UserContext(), uint(campaignID))
	if err != nil {
		return response.BadRequest(c, err.Error(), "CLONE_FAILED")
	}

	return response.Success(c, http.StatusCreated, "Sao chép chiến dịch Flash Sale thành công", cloned)
}

func (h *FlashSaleHandler) GetCampaign(c *fiber.Ctx) error {
	campaignID, err := strconv.ParseUint(c.Params("campaignId"), 10, 32)
	if err != nil {
		return response.BadRequest(c, "campaignId không hợp lệ", "INVALID_ID")
	}

	camp, err := h.svc.GetCampaign(c.UserContext(), uint(campaignID))
	if err != nil {
		return response.NotFound(c, err.Error())
	}

	return response.Success(c, http.StatusOK, "Lấy thông tin chiến dịch thành công", camp)
}

func (h *FlashSaleHandler) ListCampaigns(c *fiber.Ctx) error {
	status := c.Query("status")
	page, _ := strconv.Atoi(c.Query("page", "1"))
	limit, _ := strconv.Atoi(c.Query("limit", "10"))

	camps, total, err := h.svc.ListCampaigns(c.UserContext(), status, page, limit)
	if err != nil {
		return response.InternalError(c, err.Error())
	}

	return response.Success(c, http.StatusOK, "Lấy danh sách chiến dịch thành công", fiber.Map{
		"campaigns": camps,
		"total":     total,
		"page":      page,
		"limit":     limit,
	})
}

func (h *FlashSaleHandler) ReserveOrder(c *fiber.Ctx) error {
	return response.Error(c, fiber.StatusGone, "Luồng đặt hàng Flash Sale cũ đã ngừng hoạt động. Vui lòng thêm sản phẩm vào giỏ hàng và tiến hành thanh toán (/orders/checkout)", "FLASH_SALE_CHECKOUT_RETIRED")
}

func (h *FlashSaleHandler) GetOrderStatus(c *fiber.Ctx) error {
	reservationID := c.Params("reservationId")
	if reservationID == "" {
		return response.BadRequest(c, "Mã reservationId không hợp lệ", "INVALID_ID")
	}

	userID, _ := c.Locals("userID").(string)
	userRole, _ := c.Locals("userRole").(string)
	isAdmin := userRole == domain.RoleAdmin

	status, err := h.svc.GetOrderStatus(c.UserContext(), reservationID, userID, isAdmin)
	if err != nil {
		return response.NotFound(c, err.Error())
	}

	return response.Success(c, http.StatusOK, "Lấy trạng thái đơn hàng thành công", status)
}

func (h *FlashSaleHandler) StreamOrderStatus(c *fiber.Ctx) error {
	reservationID := c.Params("reservationId")
	if reservationID == "" {
		return response.BadRequest(c, "Mã reservationId không hợp lệ", "INVALID_ID")
	}

	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")
	c.Set("Transfer-Encoding", "chunked")
	c.Set("X-Accel-Buffering", "no")

	c.Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		// 1. Gửi ngay Snapshot hiện tại
		var initialStatus string
		if h.redisClient != nil {
			initialStatus, _ = h.redisClient.Get(ctx, redislock.KeyOrderStatus(reservationID)).Result()
		}
		if initialStatus != "" {
			_, _ = fmt.Fprintf(w, "event: order-status\ndata: %s\n\n", initialStatus)
			_ = w.Flush()

			var parsed dto.FlashSaleOrderStatusResponse
			if err := json.Unmarshal([]byte(initialStatus), &parsed); err == nil {
				if parsed.Status == "CONFIRMED" || parsed.Status == "CANCELLED" || parsed.Status == "EXPIRED" {
					return
				}
			}
		}

		// 2. Subscribe Redis Pub/Sub để đón kết quả realtime
		var pubsub *redis.PubSub
		if h.redisClient != nil {
			pubsub = h.redisClient.Subscribe(ctx, fmt.Sprintf("pubsub:order-status:%s", reservationID))
			defer pubsub.Close()
		}

		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		ch := pubsub.Channel()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				_, _ = fmt.Fprintf(w, "event: order-status\ndata: %s\n\n", msg.Payload)
				_ = w.Flush()

				var parsed dto.FlashSaleOrderStatusResponse
				if err := json.Unmarshal([]byte(msg.Payload), &parsed); err == nil {
					if parsed.Status == "CONFIRMED" || parsed.Status == "CANCELLED" || parsed.Status == "EXPIRED" {
						return
					}
				}
			case <-ticker.C:
				// Heartbeat comment
				_, _ = fmt.Fprintf(w, ": ping\n\n")
				_ = w.Flush()
			}
		}
	}))

	return nil
}

func (h *FlashSaleHandler) GetProductOffer(c *fiber.Ctx) error {
	productID, err := c.ParamsInt("productId")
	if err != nil || productID <= 0 {
		return response.BadRequest(c, "Mã sản phẩm không hợp lệ", "INVALID_PRODUCT_ID")
	}

	userID, _ := c.Locals("userID").(string)
	if userID == "" {
		userID, _ = c.Locals("userId").(string)
	}
	resp, err := h.svc.GetProductOffer(c.UserContext(), uint(productID), userID)
	if err != nil {
		return response.Error(c, http.StatusInternalServerError, err.Error(), "GET_OFFER_FAILED")
	}

	return response.Success(c, http.StatusOK, "Lấy thông tin ưu đãi thành công", resp)
}

func (h *FlashSaleHandler) GetBatchOffers(c *fiber.Ctx) error {
	var req dto.BatchOfferRequest
	if c.Method() == fiber.MethodPost {
		if err := c.BodyParser(&req); err != nil {
			return response.BadRequest(c, "Dữ liệu yêu cầu không hợp lệ", "INVALID_REQUEST")
		}
	} else {
		// GET query params (e.g. ?ids=1,2,3)
		idsParam := c.Query("ids")
		if idsParam != "" {
			var ids []uint
			for _, part := range strings.Split(idsParam, ",") {
				if id, err := strconv.Atoi(strings.TrimSpace(part)); err == nil && id > 0 {
					ids = append(ids, uint(id))
				}
			}
			req.ProductIDs = ids
		}
	}

	userID, _ := c.Locals("userID").(string)
	if userID == "" {
		userID, _ = c.Locals("userId").(string)
	}
	resp, err := h.svc.GetBatchProductOffers(c.UserContext(), req.ProductIDs, userID)
	if err != nil {
		return response.Error(c, http.StatusInternalServerError, err.Error(), "GET_BATCH_OFFERS_FAILED")
	}

	return response.Success(c, http.StatusOK, "Lấy danh sách ưu đãi thành công", resp)
}
