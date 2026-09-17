package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/redislock"
	"ecomerce-service/services/order-service/internal/client"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/dto"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type FlashSaleService interface {
	CreateCampaign(ctx context.Context, req *dto.CreateCampaignRequest) (*dto.FlashSaleCampaignResponse, error)
	UpdateCampaign(ctx context.Context, campaignID uint, req *dto.UpdateCampaignRequest) (*dto.FlashSaleCampaignResponse, error)
	AddItem(ctx context.Context, campaignID uint, req *dto.AddFlashSaleItemRequest) (*dto.FlashSaleItemResponse, error)
	UpdateItem(ctx context.Context, campaignID, itemID uint, req *dto.UpdateFlashSaleItemRequest) (*dto.FlashSaleItemResponse, error)
	DeleteItem(ctx context.Context, campaignID, itemID uint) error
	ActivateCampaign(ctx context.Context, campaignID uint) error
	EndCampaign(ctx context.Context, campaignID uint) error
	CloneCampaign(ctx context.Context, campaignID uint) (*dto.FlashSaleCampaignResponse, error)
	GetCampaign(ctx context.Context, campaignID uint) (*dto.FlashSaleCampaignResponse, error)
	ListCampaigns(ctx context.Context, status string, page, limit int) ([]*dto.FlashSaleCampaignResponse, int64, error)
	GetActiveCampaign(ctx context.Context) (*dto.ActiveCampaignResponse, error)

	GetProductOffer(ctx context.Context, productID uint, userID string) (*dto.ProductOfferResponse, error)
	GetBatchProductOffers(ctx context.Context, productIDs []uint, userID string) (*dto.BatchOfferResponse, error)

	ReserveOrder(ctx context.Context, campaignID, productID uint, userID, requestID string, req *dto.FlashSaleCustomerOrderRequest) (*dto.FlashSaleOrderResponse, error)
	GetOrderStatus(ctx context.Context, reservationID string, userID string, isAdmin bool) (*dto.FlashSaleOrderStatusResponse, error)
}

type flashSaleService struct {
	repo          domain.FlashSaleRepository
	productClient client.ProductClient
	redisClient   *redis.Client
}

func NewFlashSaleService(
	repo domain.FlashSaleRepository,
	productClient client.ProductClient,
	redisClient *redis.Client,
) FlashSaleService {
	return &flashSaleService{
		repo:          repo,
		productClient: productClient,
		redisClient:   redisClient,
	}
}

func (s *flashSaleService) CreateCampaign(ctx context.Context, req *dto.CreateCampaignRequest) (*dto.FlashSaleCampaignResponse, error) {
	if req.Name == "" {
		return nil, errors.New("tên chiến dịch không được để trống")
	}
	if req.EndsAt.Before(req.StartsAt) || req.EndsAt.Equal(req.StartsAt) {
		return nil, errors.New("thời gian kết thúc phải sau thời gian bắt đầu")
	}

	campaign := &domain.FlashSaleCampaign{
		Name:        req.Name,
		Description: req.Description,
		StartsAt:    req.StartsAt,
		EndsAt:      req.EndsAt,
		Status:      domain.CampaignStatusDraft,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := s.repo.CreateCampaign(campaign); err != nil {
		return nil, err
	}

	return s.toCampaignResponse(campaign), nil
}

func (s *flashSaleService) UpdateCampaign(ctx context.Context, campaignID uint, req *dto.UpdateCampaignRequest) (*dto.FlashSaleCampaignResponse, error) {
	camp, err := s.repo.GetCampaignByID(campaignID)
	if err != nil {
		return nil, fmt.Errorf("không tìm thấy campaign #%d", campaignID)
	}

	if camp.Status != domain.CampaignStatusDraft && camp.Status != domain.CampaignStatusActivationFailed {
		return nil, fmt.Errorf("chỉ có thể chỉnh sửa chiến dịch ở trạng thái DRAFT hoặc ACTIVATION_FAILED (hiện tại: %s). Vui lòng Nhân bản để tạo đợt mới nếu muốn thay đổi", camp.Status)
	}

	if req.Name == "" {
		return nil, errors.New("tên chiến dịch không được để trống")
	}
	if req.EndsAt.Before(req.StartsAt) || req.EndsAt.Equal(req.StartsAt) {
		return nil, errors.New("thời gian kết thúc phải sau thời gian bắt đầu")
	}

	camp.Name = req.Name
	camp.Description = req.Description
	camp.StartsAt = req.StartsAt
	camp.EndsAt = req.EndsAt
	camp.UpdatedAt = time.Now()

	if err := s.repo.UpdateCampaign(camp); err != nil {
		return nil, fmt.Errorf("lỗi cập nhật chiến dịch: %w", err)
	}

	return s.toCampaignResponse(camp), nil
}

func (s *flashSaleService) AddItem(ctx context.Context, campaignID uint, req *dto.AddFlashSaleItemRequest) (*dto.FlashSaleItemResponse, error) {
	camp, err := s.repo.GetCampaignByID(campaignID)
	if err != nil {
		return nil, fmt.Errorf("không tìm thấy campaign #%d", campaignID)
	}

	if camp.Status != domain.CampaignStatusDraft && camp.Status != domain.CampaignStatusActivationFailed {
		return nil, errors.New("chỉ có thể thêm sản phẩm khi Campaign ở trạng thái DRAFT hoặc ACTIVATION_FAILED")
	}

	if req.ProductID == 0 || req.AllocatedStock <= 0 || req.SalePrice < 0 {
		return nil, errors.New("thông tin sản phẩm phân bổ không hợp lệ")
	}

	maxPerUser := req.MaxQuantityPerUser
	if maxPerUser < 0 {
		maxPerUser = 1
	}
	maxPerOrder := req.MaxQuantityPerOrder
	if maxPerOrder <= 0 {
		maxPerOrder = 1
	}
	resvSec := req.ReservationSeconds
	if resvSec <= 0 {
		resvSec = 120
	}

	item := &domain.FlashSaleItem{
		CampaignID:          campaignID,
		ProductID:           req.ProductID,
		SalePrice:           req.SalePrice,
		OriginalPrice:       req.OriginalPrice,
		AllocatedStock:      req.AllocatedStock,
		ReservedStock:       0,
		SoldStock:           0,
		MaxQuantityPerUser:  maxPerUser,
		MaxQuantityPerOrder: maxPerOrder,
		ReservationSeconds:  resvSec,
		CreatedAt:           time.Now(),
		UpdatedAt:           time.Now(),
	}

	if err := s.repo.AddItem(item); err != nil {
		return nil, err
	}

	return s.toItemResponse(item), nil
}

func (s *flashSaleService) UpdateItem(ctx context.Context, campaignID, itemID uint, req *dto.UpdateFlashSaleItemRequest) (*dto.FlashSaleItemResponse, error) {
	camp, err := s.repo.GetCampaignByID(campaignID)
	if err != nil {
		return nil, fmt.Errorf("không tìm thấy campaign #%d", campaignID)
	}

	if camp.Status != domain.CampaignStatusDraft && camp.Status != domain.CampaignStatusActivationFailed {
		return nil, fmt.Errorf("chỉ có thể chỉnh sửa sản phẩm khi Campaign ở trạng thái DRAFT hoặc ACTIVATION_FAILED (hiện tại: %s)", camp.Status)
	}

	item, err := s.repo.GetItemByID(itemID)
	if err != nil || item == nil || item.CampaignID != campaignID {
		return nil, fmt.Errorf("không tìm thấy sản phẩm #%d trong chiến dịch #%d", itemID, campaignID)
	}

	if req.AllocatedStock <= 0 || req.SalePrice < 0 {
		return nil, errors.New("giá sale và số lượng phân bổ phải lớn hơn 0")
	}

	maxPerUser := req.MaxQuantityPerUser
	if maxPerUser < 0 {
		maxPerUser = 1
	}
	maxPerOrder := req.MaxQuantityPerOrder
	if maxPerOrder <= 0 {
		maxPerOrder = 1
	}
	resvSec := req.ReservationSeconds
	if resvSec <= 0 {
		resvSec = 120
	}

	item.SalePrice = req.SalePrice
	item.OriginalPrice = req.OriginalPrice
	item.AllocatedStock = req.AllocatedStock
	item.MaxQuantityPerUser = maxPerUser
	item.MaxQuantityPerOrder = maxPerOrder
	item.ReservationSeconds = resvSec
	item.UpdatedAt = time.Now()

	if err := s.repo.UpdateItem(item); err != nil {
		return nil, fmt.Errorf("lỗi cập nhật sản phẩm: %w", err)
	}

	return s.toItemResponse(item), nil
}

func (s *flashSaleService) DeleteItem(ctx context.Context, campaignID, itemID uint) error {
	camp, err := s.repo.GetCampaignByID(campaignID)
	if err != nil {
		return fmt.Errorf("không tìm thấy campaign #%d", campaignID)
	}

	if camp.Status != domain.CampaignStatusDraft && camp.Status != domain.CampaignStatusActivationFailed {
		return fmt.Errorf("chỉ có thể xóa sản phẩm khi Campaign ở trạng thái DRAFT hoặc ACTIVATION_FAILED (hiện tại: %s)", camp.Status)
	}

	item, err := s.repo.GetItemByID(itemID)
	if err != nil || item == nil || item.CampaignID != campaignID {
		return fmt.Errorf("không tìm thấy sản phẩm #%d trong chiến dịch #%d", itemID, campaignID)
	}

	if err := s.repo.DeleteItem(campaignID, itemID); err != nil {
		return fmt.Errorf("lỗi xóa sản phẩm khỏi chiến dịch: %w", err)
	}

	return nil
}

func (s *flashSaleService) ActivateCampaign(ctx context.Context, campaignID uint) error {
	camp, err := s.repo.GetCampaignByID(campaignID)
	if err != nil {
		return fmt.Errorf("không tìm thấy campaign #%d", campaignID)
	}

	// Hỗ trợ kích hoạt mới hoặc resume khi crash ở ALLOCATING / PREWARMING / ACTIVATION_FAILED
	if camp.Status != domain.CampaignStatusDraft &&
		camp.Status != domain.CampaignStatusActivationFailed &&
		camp.Status != domain.CampaignStatusAllocating &&
		camp.Status != domain.CampaignStatusPrewarming {
		return fmt.Errorf("chỉ có thể kích hoạt campaign ở trạng thái DRAFT, ACTIVATION_FAILED, ALLOCATING hoặc PREWARMING (hiện tại: %s)", camp.Status)
	}

	if len(camp.Items) == 0 {
		return errors.New("campaign không có sản phẩm nào để kích hoạt")
	}

	// 1. Chuyển trạng thái sang ALLOCATING nếu chưa ở ALLOCATING / PREWARMING
	if camp.Status != domain.CampaignStatusAllocating && camp.Status != domain.CampaignStatusPrewarming {
		if err := s.repo.UpdateCampaignStatus(campaignID, camp.Status, domain.CampaignStatusAllocating); err != nil {
			return err
		}
	}

	// 2. Gọi Product Service phân bổ tồn kho (Idempotent theo requestID)
	var allocatedProductIDs []uint
	var allocateErr error

	for _, item := range camp.Items {
		reqID := fmt.Sprintf("alloc-camp-%d-prod-%d", campaignID, item.ProductID)
		err := s.productClient.AllocateFlashSaleStock(ctx, campaignID, item.ProductID, reqID, item.AllocatedStock)
		if err != nil {
			allocateErr = fmt.Errorf("lỗi phân bổ sản phẩm #%d: %w", item.ProductID, err)
			break
		}
		allocatedProductIDs = append(allocatedProductIDs, item.ProductID)
	}

	// Nếu phân bổ thất bại -> Bồi hoàn (Compensating Transaction)
	if allocateErr != nil {
		logger.ErrorContext(ctx, "❌ Phân bổ tồn kho thất bại, tiến hành bồi hoàn", "campaign_id", campaignID, "error", allocateErr.Error())
		_ = s.repo.UpdateCampaignStatus(campaignID, domain.CampaignStatusAllocating, domain.CampaignStatusCompensating)

		for _, prodID := range allocatedProductIDs {
			compReqID := fmt.Sprintf("compensate-camp-%d-prod-%d", campaignID, prodID)
			_ = s.productClient.ReleaseFlashSaleStock(ctx, campaignID, prodID, compReqID)
		}

		_ = s.repo.UpdateCampaignStatus(campaignID, domain.CampaignStatusCompensating, domain.CampaignStatusActivationFailed)
		return allocateErr
	}

	// 3. Chuyển sang PREWARMING
	if camp.Status != domain.CampaignStatusPrewarming {
		if err := s.repo.UpdateCampaignStatus(campaignID, domain.CampaignStatusAllocating, domain.CampaignStatusPrewarming); err != nil {
			logger.WarnContext(ctx, "Không thể chuyển ALLOCATING -> PREWARMING, có thể đã ở PREWARMING", "error", err.Error())
		}
	}

	// 4. Prewarm toàn bộ Items lên Redis với trạng thái ban đầu là PAUSED
	var prewarmErr error
	for _, item := range camp.Items {
		err := redislock.PrewarmCampaignItem(
			ctx, s.redisClient,
			campaignID, item.ProductID,
			item.AllocatedStock,
			item.MaxQuantityPerUser,
			item.MaxQuantityPerOrder,
			camp.StartsAt.Unix(),
			camp.EndsAt.Unix(),
			item.ReservationSeconds,
			"PAUSED",
		)
		if err != nil {
			prewarmErr = fmt.Errorf("lỗi prewarm Redis sản phẩm #%d: %w", item.ProductID, err)
			logger.ErrorContext(ctx, "Lỗi prewarm Redis", "product_id", item.ProductID, "error", err.Error())
			break
		}
	}

	// Nếu prewarm Redis thất bại -> Không được active dở dang! Báo lỗi để retry
	if prewarmErr != nil {
		logger.ErrorContext(ctx, "❌ Prewarm Redis thất bại, không kích hoạt campaign", "campaign_id", campaignID, "error", prewarmErr.Error())
		return prewarmErr
	}

	// 5. Cập nhật Campaign thành ACTIVE trong DB
	if err := s.repo.UpdateCampaignStatus(campaignID, domain.CampaignStatusPrewarming, domain.CampaignStatusActive); err != nil {
		return err
	}

	// 6. Mở cổng trên Redis: chuyển state từng item từ PAUSED sang ACTIVE
	for _, item := range camp.Items {
		if s.redisClient != nil {
			_ = s.redisClient.Set(ctx, redislock.KeyState(campaignID, item.ProductID), "ACTIVE", 0)
		}
	}

	logger.InfoContext(ctx, "✅ Kích hoạt Flash Sale Campaign thành công", "campaign_id", campaignID, "items_count", len(camp.Items))
	return nil
}

func (s *flashSaleService) EndCampaign(ctx context.Context, campaignID uint) error {
	// 1. Chuyển sang ENDING với FOR UPDATE lock (chờ các checkout FOR SHARE in-flight commit xong)
	camp, err := s.repo.TransitionToEnding(campaignID)
	if err != nil {
		return err
	}

	// 2. Đóng cổng Redis ngay lập tức (state = ENDED) để chặn giữ chỗ mới
	for _, item := range camp.Items {
		if s.redisClient != nil {
			_ = s.redisClient.Set(ctx, redislock.KeyState(campaignID, item.ProductID), "ENDED", 0)
		}
	}

	// 2.1. Lấy lại snapshot tươi mới của Campaign/Items sau khi các in-flight checkouts đã commit
	freshCamp, err := s.repo.GetCampaignByID(campaignID)
	if err != nil {
		return err
	}

	// 2.2. DRAIN BARRIER (Point 9 fix): Không được release khi còn đơn hàng/suất giữ chỗ chưa giải quyết (reserved_stock > 0)
	for _, item := range freshCamp.Items {
		if item.ReservedStock > 0 {
			return fmt.Errorf("chưa thể kết thúc campaign: sản phẩm #%d còn %d suất đang giữ chỗ (reserved_stock > 0). Vui lòng đợi các đơn hàng hỗn hợp hoàn tất hoặc hết hạn giữ chỗ",
				item.ProductID, item.ReservedStock)
		}
	}

	// 3. SETTLEMENT BARRIER (Hàng rào quyết toán tồn kho - Point 9 fix):
	// Đối với từng item, kiểm tra Product DB sold_quantity đã khớp chính xác với Order DB sold_stock chưa (==).
	// Nếu Product consumer còn đang lag phía sau (<), chờ retry. Nếu lớn hơn (>), báo lỗi bất thường sổ cái.
	for _, item := range freshCamp.Items {
		settled := false
		var lastProductSold int
		for attempt := 0; attempt < 5; attempt++ {
			alloc, err := s.productClient.GetStockAllocation(ctx, campaignID, item.ProductID)
			if err == nil && alloc != nil {
				lastProductSold = alloc.SoldQuantity
				if alloc.SoldQuantity == item.SoldStock {
					settled = true
					break
				}
				if alloc.SoldQuantity > item.SoldStock {
					return fmt.Errorf("phát hiện bất thường dữ liệu sổ cái (Ledger Discrepancy): Product DB sold=%d vượt quá Order DB sold=%d cho sản phẩm #%d. Campaign được giữ ở trạng thái ENDING để rà soát",
						alloc.SoldQuantity, item.SoldStock, item.ProductID)
				}
			}
			logger.WarnContext(ctx, "⏳ [SETTLEMENT BARRIER] Chờ Product DB tiêu thụ xong sự kiện bán hàng",
				"campaign_id", campaignID,
				"product_id", item.ProductID,
				"order_db_sold", item.SoldStock,
				"product_db_sold", lastProductSold,
				"attempt", attempt+1,
			)
			time.Sleep(500 * time.Millisecond)
		}

		if !settled {
			return fmt.Errorf("chưa thể kết thúc campaign: sổ cái Product DB (sold=%d) chưa quyết toán kịp với Order DB (sold=%d). Campaign được giữ ở trạng thái ENDING để retry",
				lastProductSold, item.SoldStock)
		}
	}

	// 4. Khi toàn bộ các sản phẩm đã quyết toán đồng bộ -> Gọi ReleaseFlashSaleStock
	for _, item := range freshCamp.Items {
		reqID := fmt.Sprintf("end-release-%d-%d", campaignID, item.ProductID)
		if err := s.productClient.ReleaseFlashSaleStock(ctx, campaignID, item.ProductID, reqID); err != nil {
			logger.ErrorContext(ctx, "❌ Lỗi release tồn kho cho sản phẩm",
				"campaign_id", campaignID,
				"product_id", item.ProductID,
				"error", err.Error(),
			)
			// Không nuốt lỗi! Giữ nguyên ENDING để lần sau retry
			return fmt.Errorf("lỗi release tồn kho sản phẩm #%d: %w", item.ProductID, err)
		}
	}

	// 5. Cập nhật sang ENDED sau khi toàn bộ items đã được hoàn kho thành công
	return s.repo.UpdateCampaignStatus(campaignID, domain.CampaignStatusEnding, domain.CampaignStatusEnded)
}

func (s *flashSaleService) CloneCampaign(ctx context.Context, campaignID uint) (*dto.FlashSaleCampaignResponse, error) {
	orig, err := s.repo.GetCampaignByID(campaignID)
	if err != nil {
		return nil, fmt.Errorf("không tìm thấy campaign nguồn #%d", campaignID)
	}

	now := time.Now()
	newCamp := &domain.FlashSaleCampaign{
		Name:        fmt.Sprintf("%s (Bản sao)", orig.Name),
		Description: orig.Description,
		StartsAt:    now.Add(1 * time.Hour),
		EndsAt:      now.Add(3 * time.Hour),
		Status:      domain.CampaignStatusDraft,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if err := s.repo.CreateCampaign(newCamp); err != nil {
		return nil, err
	}

	for _, item := range orig.Items {
		newItem := &domain.FlashSaleItem{
			CampaignID:          newCamp.ID,
			ProductID:           item.ProductID,
			SalePrice:           item.SalePrice,
			OriginalPrice:       item.OriginalPrice,
			AllocatedStock:      item.AllocatedStock,
			MaxQuantityPerUser:  item.MaxQuantityPerUser,
			MaxQuantityPerOrder: item.MaxQuantityPerOrder,
			ReservationSeconds:  item.ReservationSeconds,
			CreatedAt:           now,
			UpdatedAt:           now,
		}
		_ = s.repo.AddItem(newItem)
	}

	return s.GetCampaign(ctx, newCamp.ID)
}

func (s *flashSaleService) GetCampaign(ctx context.Context, campaignID uint) (*dto.FlashSaleCampaignResponse, error) {
	camp, err := s.repo.GetCampaignByID(campaignID)
	if err != nil {
		return nil, err
	}
	return s.toCampaignResponse(camp), nil
}

func (s *flashSaleService) ListCampaigns(ctx context.Context, status string, page, limit int) ([]*dto.FlashSaleCampaignResponse, int64, error) {
	camps, total, err := s.repo.ListCampaigns(status, page, limit)
	if err != nil {
		return nil, 0, err
	}

	resp := make([]*dto.FlashSaleCampaignResponse, 0, len(camps))
	for _, c := range camps {
		resp = append(resp, s.toCampaignResponse(c))
	}
	return resp, total, nil
}

func (s *flashSaleService) GetActiveCampaign(ctx context.Context) (*dto.ActiveCampaignResponse, error) {
	camp, err := s.repo.GetActiveCampaign()
	if err != nil {
		return nil, fmt.Errorf("lỗi truy vấn chiến dịch active: %w", err)
	}
	if camp == nil {
		return nil, nil
	}

	now := time.Now()
	remainingSec := int64(0)
	if camp.EndsAt.After(now) {
		remainingSec = int64(camp.EndsAt.Sub(now).Seconds())
	}

	items := make([]*dto.ActiveCampaignItemDTO, 0, len(camp.Items))
	for _, it := range camp.Items {
		itemDTO := &dto.ActiveCampaignItemDTO{
			ID:                  it.ID,
			CampaignID:          it.CampaignID,
			ProductID:           it.ProductID,
			SalePrice:           it.SalePrice,
			OriginalPrice:       it.OriginalPrice,
			AllocatedStock:      it.AllocatedStock,
			ReservedStock:       it.ReservedStock,
			SoldStock:           it.SoldStock,
			MaxQuantityPerUser:  it.MaxQuantityPerUser,
			MaxQuantityPerOrder: it.MaxQuantityPerOrder,
			ReservationSeconds:  it.ReservationSeconds,
		}

		if it.OriginalPrice > 0 && it.SalePrice < it.OriginalPrice {
			itemDTO.DiscountPercentage = int(((it.OriginalPrice - it.SalePrice) / it.OriginalPrice) * 100)
		}

		// Đọc tồn kho thực tế từ RAM Redis nếu có
		remaining := it.AllocatedStock - it.SoldStock - it.ReservedStock
		if s.redisClient != nil {
			stockKey := redislock.KeyStock(it.CampaignID, it.ProductID)
			if stockVal, err := s.redisClient.Get(ctx, stockKey).Int(); err == nil {
				remaining = stockVal
			}
		}
		if remaining < 0 {
			remaining = 0
		}
		itemDTO.RemainingStock = remaining

		// Lấy chi tiết sản phẩm từ Product Service (tên, thumbnail)
		if s.productClient != nil {
			prod, err := s.productClient.GetProduct(ctx, it.ProductID)
			if err == nil && prod != nil {
				itemDTO.ProductName = prod.Name
				itemDTO.ProductThumbnail = prod.Thumbnail
				if itemDTO.OriginalPrice == 0 {
					itemDTO.OriginalPrice = prod.Price
				}
			}
		}
		if itemDTO.ProductName == "" {
			itemDTO.ProductName = fmt.Sprintf("Sản phẩm #%d", it.ProductID)
		}

		items = append(items, itemDTO)
	}

	return &dto.ActiveCampaignResponse{
		ID:               camp.ID,
		Name:             camp.Name,
		Description:      camp.Description,
		StartsAt:         camp.StartsAt,
		EndsAt:           camp.EndsAt,
		Status:           string(camp.Status),
		RemainingSeconds: remainingSec,
		Items:            items,
	}, nil
}

func (s *flashSaleService) ReserveOrder(
	ctx context.Context,
	campaignID, productID uint,
	userID, requestID string,
	req *dto.FlashSaleCustomerOrderRequest,
) (*dto.FlashSaleOrderResponse, error) {
	if req.Quantity <= 0 {
		return nil, errors.New("số lượng đặt mua phải lớn hơn 0")
	}
	if req.CustomerName == "" || req.CustomerPhone == "" || req.ShippingAddress == "" {
		return nil, errors.New("vui lòng điền đầy đủ thông tin giao hàng")
	}

	paymentMethod := strings.ToUpper(req.PaymentMethod)
	if paymentMethod == "" {
		paymentMethod = "COD"
	}
	if paymentMethod != "COD" {
		return nil, errors.New("chiến dịch Flash Sale hiện chỉ hỗ trợ hình thức thanh toán khi nhận hàng (COD)")
	}

	// 1. Tính Fingerprint của request (SHA-256) để chống tái sử dụng Idempotency-Key với body khác
	fingerprint := s.computeFingerprint(req)

	// 2. Lấy cấu hình Item từ Database
	item, err := s.repo.GetItem(campaignID, productID)
	if err != nil {
		return nil, errors.New("sản phẩm không nằm trong chiến dịch Flash Sale")
	}

	resvSeconds := item.ReservationSeconds
	reservationID := fmt.Sprintf("FSR-%s", strings.ToUpper(uuid.New().String()[:12]))

	// 3. THÀNH TRÌ REDIS: Giữ chỗ tồn kho & Quota bằng Atomic Lua Script
	luaResp, err := redislock.ReserveFlashSaleStock(
		ctx, s.redisClient,
		campaignID, productID,
		userID, requestID, fingerprint, reservationID,
		req.Quantity, resvSeconds,
	)
	if err != nil {
		logger.ErrorContext(ctx, "Lỗi gọi Redis Lua Flash Sale Reserve", "error", err.Error())
		return nil, errors.New("hệ thống đang quá tải, vui lòng thử lại sau")
	}

	switch luaResp.Code {
	case redislock.ResultSoldOut:
		return nil, errors.New("sản phẩm Flash Sale đã hết hàng")
	case redislock.ResultLimitExceeded:
		return nil, errors.New("bạn đã đạt giới hạn mua tối đa của sản phẩm này trong đợt sale")
	case redislock.ResultCampaignNotActive:
		return nil, errors.New("chương trình Flash Sale chưa diễn ra hoặc đã kết thúc")
	case redislock.ResultIdempotencyKeyReused:
		return nil, errors.New("Idempotency-Key đã được sử dụng cho một yêu cầu khác")
	case redislock.ResultInvalidOrderQuantity:
		return nil, fmt.Errorf("số lượng đặt mua vượt quá giới hạn %d món/đơn", item.MaxQuantityPerOrder)
	case redislock.ResultReserved:
		// Thành công, tiếp tục ghi PostgreSQL
	default:
		return nil, fmt.Errorf("kết quả Flash Sale không xác định: %s", luaResp.Code)
	}

	// Sử dụng reservationID thực tế (có thể là ID cũ nếu là Idempotent retry)
	finalResvID := luaResp.ReservationID
	if finalResvID == "" {
		finalResvID = reservationID
	}

	// 4. LƯU BẢN GHI DỰ PHÒNG BỀN VỮNG (DURABLE) & OUTBOX VÀO POSTGRESQL TRONG 1 TRANSACTION
	expiresAt := time.Unix(luaResp.ExpiresAt, 0)
	unitPrice := item.SalePrice
	totalAmount := unitPrice * float64(req.Quantity)

	resv := &domain.FlashSaleReservation{
		ID:                 finalResvID,
		RequestID:          requestID,
		RequestFingerprint: fingerprint,
		CampaignID:         campaignID,
		FlashSaleItemID:    item.ID,
		ProductID:          productID,
		UserID:             userID,
		Quantity:           req.Quantity,
		UnitPrice:          unitPrice,
		TotalAmount:        totalAmount,
		PaymentMethod:      paymentMethod,
		Status:             domain.ReservationStatusReserved,
		ExpiresAt:          expiresAt,
		CreatedAt:          time.Now(),
		UpdatedAt:          time.Now(),
	}

	// Chuẩn bị payload cho Outbox Event
	outboxID := uuid.New().String()
	outboxPayload := map[string]interface{}{
		"event_id":         outboxID,
		"reservation_id":   finalResvID,
		"request_id":       requestID,
		"campaign_id":      campaignID,
		"product_id":       productID,
		"user_id":          userID,
		"quantity":         req.Quantity,
		"unit_price":       unitPrice,
		"total_amount":     totalAmount,
		"payment_method":   paymentMethod,
		"customer_name":    req.CustomerName,
		"customer_email":   req.CustomerEmail,
		"customer_phone":   req.CustomerPhone,
		"shipping_address": req.ShippingAddress,
		"trace_id":         logger.GetTraceID(ctx),
	}
	payloadBytes, _ := json.Marshal(outboxPayload)

	outbox := &domain.OutboxEvent{
		ID:            outboxID,
		AggregateType: "FlashSaleReservation",
		AggregateID:   finalResvID,
		EventType:     "FLASH_SALE_RESERVED",
		Topic:         pkgKafka.TopicFlashSaleOrders,
		PartitionKey:  userID,
		Payload:       string(payloadBytes),
		Status:        domain.OutboxStatusPending,
		NextAttemptAt: time.Now(),
		CreatedAt:     time.Now(),
	}

	dbErr := s.repo.CreateReservationWithOutbox(resv, outbox)
	if dbErr != nil {
		logger.ErrorContext(ctx, "❌ Lỗi lưu reservation & outbox vào DB, tiến hành nhả kho Redis", "reservation_id", finalResvID, "error", dbErr.Error())
		// Hoàn lại kho trên Redis ngay lập tức
		_, _ = redislock.ReleaseFlashSaleReservation(ctx, s.redisClient, campaignID, productID, finalResvID, "CANCELLED")
		return nil, fmt.Errorf("không thể hoàn tất giữ chỗ đơn hàng: %w", dbErr)
	}

	// 5. Lưu status snapshot vào Redis cho Client Polling nhanh (Zero DB Hit)
	statusSnapshot := dto.FlashSaleOrderStatusResponse{
		ReservationID: finalResvID,
		Status:        "RESERVED",
		ExpiresAt:     expiresAt,
		UpdatedAt:     time.Now(),
	}
	if statusJSON, sErr := json.Marshal(statusSnapshot); sErr == nil && s.redisClient != nil {
		_ = s.redisClient.Set(ctx, redislock.KeyOrderStatus(finalResvID), string(statusJSON), 24*time.Hour)
	}

	logger.InfoContext(ctx, "⚡ Giữ chỗ Flash Sale thành công",
		"reservation_id", finalResvID,
		"user_id", userID,
		"product_id", productID,
		"quantity", req.Quantity,
	)

	return &dto.FlashSaleOrderResponse{
		ReservationID: finalResvID,
		Status:        "RESERVED",
		ExpiresAt:     expiresAt,
		StatusURL:     fmt.Sprintf("/flash-sales/orders/%s", finalResvID),
		StreamURL:     fmt.Sprintf("/flash-sales/orders/%s/stream", finalResvID),
	}, nil
}

func (s *flashSaleService) GetOrderStatus(ctx context.Context, reservationID string, userID string, isAdmin bool) (*dto.FlashSaleOrderStatusResponse, error) {
	// 1. Kiểm tra RAM Redis trước
	if s.redisClient != nil {
		val, err := s.redisClient.Get(ctx, redislock.KeyOrderStatus(reservationID)).Result()
		if err == nil && val != "" {
			var resp dto.FlashSaleOrderStatusResponse
			if err := json.Unmarshal([]byte(val), &resp); err == nil {
				return &resp, nil
			}
		}
	}

	// 2. Cache Miss: Đọc từ PostgreSQL (Single Source of Truth)
	resv, err := s.repo.FindReservationByID(reservationID)
	if err != nil {
		return nil, errors.New("mã đơn giữ chỗ không tồn tại hoặc đã hết hạn")
	}

	if !isAdmin && resv.UserID != userID {
		return nil, errors.New("bạn không có quyền truy cập thông tin đơn giữ chỗ này")
	}

	return &dto.FlashSaleOrderStatusResponse{
		ReservationID: resv.ID,
		Status:        string(resv.Status),
		OrderID:       resv.OrderID,
		ExpiresAt:     resv.ExpiresAt,
		FailureReason: resv.FailureReason,
		UpdatedAt:     resv.UpdatedAt,
	}, nil
}

func (s *flashSaleService) computeFingerprint(req *dto.FlashSaleCustomerOrderRequest) string {
	raw := fmt.Sprintf("%d|%s|%s|%s|%s|%s",
		req.Quantity,
		strings.TrimSpace(req.PaymentMethod),
		strings.TrimSpace(req.CustomerName),
		strings.TrimSpace(req.CustomerEmail),
		strings.TrimSpace(req.CustomerPhone),
		strings.TrimSpace(req.ShippingAddress),
	)
	hash := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(hash[:])
}

func (s *flashSaleService) toCampaignResponse(c *domain.FlashSaleCampaign) *dto.FlashSaleCampaignResponse {
	resp := &dto.FlashSaleCampaignResponse{
		ID:          c.ID,
		Name:        c.Name,
		Description: c.Description,
		StartsAt:    c.StartsAt,
		EndsAt:      c.EndsAt,
		Status:      string(c.Status),
		CreatedAt:   c.CreatedAt,
		UpdatedAt:   c.UpdatedAt,
	}
	for _, item := range c.Items {
		resp.Items = append(resp.Items, s.toItemResponse(&item))
	}
	return resp
}

func (s *flashSaleService) toItemResponse(item *domain.FlashSaleItem) *dto.FlashSaleItemResponse {
	return &dto.FlashSaleItemResponse{
		ID:                  item.ID,
		CampaignID:          item.CampaignID,
		ProductID:           item.ProductID,
		SalePrice:           item.SalePrice,
		OriginalPrice:       item.OriginalPrice,
		AllocatedStock:      item.AllocatedStock,
		ReservedStock:       item.ReservedStock,
		SoldStock:           item.SoldStock,
		MaxQuantityPerUser:  item.MaxQuantityPerUser,
		MaxQuantityPerOrder: item.MaxQuantityPerOrder,
		ReservationSeconds:  item.ReservationSeconds,
		CreatedAt:           item.CreatedAt,
		UpdatedAt:           item.UpdatedAt,
	}
}

func (s *flashSaleService) GetProductOffer(ctx context.Context, productID uint, userID string) (*dto.ProductOfferResponse, error) {
	batchResp, err := s.GetBatchProductOffers(ctx, []uint{productID}, userID)
	if err != nil {
		return nil, err
	}
	if offer, ok := batchResp.Offers[productID]; ok {
		return offer, nil
	}
	return &dto.ProductOfferResponse{
		ProductID:    productID,
		PurchaseMode: "REGULAR",
	}, nil
}

func (s *flashSaleService) GetBatchProductOffers(ctx context.Context, productIDs []uint, userID string) (*dto.BatchOfferResponse, error) {
	resp := &dto.BatchOfferResponse{
		Offers: make(map[uint]*dto.ProductOfferResponse),
	}
	if len(productIDs) == 0 {
		return resp, nil
	}
	// Giới hạn tối đa 100 IDs mỗi request để bảo vệ backend
	if len(productIDs) > 100 {
		productIDs = productIDs[:100]
	}

	// 1. Lấy Active Campaign (có preload items)
	camp, err := s.repo.GetActiveCampaign()
	if err != nil {
		logger.WarnContext(ctx, "Lỗi lấy active campaign khi tra cứu offer", "error", err.Error())
	}

	type activeItemInfo struct {
		item *domain.FlashSaleItem
		camp *domain.FlashSaleCampaign
	}
	activeMap := make(map[uint]activeItemInfo)
	now := time.Now()
	if camp != nil && camp.Status == domain.CampaignStatusActive && camp.StartsAt.Before(now) && camp.EndsAt.After(now) {
		for i := range camp.Items {
			it := &camp.Items[i]
			activeMap[it.ProductID] = activeItemInfo{item: it, camp: camp}
		}
	}

	for _, pid := range productIDs {
		info, isActive := activeMap[pid]
		if !isActive {
			resp.Offers[pid] = &dto.ProductOfferResponse{
				ProductID:    pid,
				PurchaseMode: "REGULAR",
				HasFlashSale: false,
			}
			continue
		}

		it := info.item
		c := info.camp

		// Đọc tồn kho thực tế từ RAM Redis nếu có
		remaining := it.AllocatedStock - it.SoldStock - it.ReservedStock
		if s.redisClient != nil {
			stockKey := redislock.KeyStock(c.ID, pid)
			if stockVal, err := s.redisClient.Get(ctx, stockKey).Int(); err == nil {
				remaining = stockVal
			}
		}

		if remaining <= 0 {
			resp.Offers[pid] = &dto.ProductOfferResponse{
				ProductID:        pid,
				PurchaseMode:     "REGULAR",
				HasFlashSale:     false,
				EffectivePrice:   it.OriginalPrice,
				RegularPrice:     it.OriginalPrice,
				OriginalPrice:    it.OriginalPrice,
				RemainingStock:   0,
				RemainingDisplay: 0,
			}
			continue
		}

		// Kiểm tra hạn mức người dùng (User Quota)
		isEligible := true
		if userID != "" && it.MaxQuantityPerUser > 0 && s.redisClient != nil {
			resvKey := fmt.Sprintf("fs:{c:%d:p:%d}:user:%s:resv", c.ID, pid, userID)
			purchasedKey := fmt.Sprintf("fs:{c:%d:p:%d}:user:%s:purchased", c.ID, pid, userID)
			resvQty, _ := s.redisClient.Get(ctx, resvKey).Int()
			purchasedQty, _ := s.redisClient.Get(ctx, purchasedKey).Int()
			if resvQty+purchasedQty >= it.MaxQuantityPerUser {
				isEligible = false
			}
		}

		if !isEligible {
			resp.Offers[pid] = &dto.ProductOfferResponse{
				ProductID:          pid,
				PurchaseMode:       "REGULAR",
				HasFlashSale:       false,
				EffectivePrice:     it.OriginalPrice,
				RegularPrice:       it.OriginalPrice,
				OriginalPrice:      it.OriginalPrice,
				RemainingStock:     remaining,
				RemainingDisplay:   remaining,
				MaxQuantityPerUser: it.MaxQuantityPerUser,
			}
			continue
		}

		discountPercent := 0
		if it.OriginalPrice > 0 && it.SalePrice < it.OriginalPrice {
			discountPercent = int(((it.OriginalPrice - it.SalePrice) / it.OriginalPrice) * 100)
		}

		salePrice := it.SalePrice
		endsAt := c.EndsAt
		campID := c.ID
		resp.Offers[pid] = &dto.ProductOfferResponse{
			ProductID:          pid,
			PurchaseMode:       "FLASH_SALE",
			HasFlashSale:       true,
			EffectivePrice:     it.SalePrice,
			RegularPrice:       it.OriginalPrice,
			OriginalPrice:      it.OriginalPrice,
			CampaignID:         &campID,
			CampaignName:       c.Name,
			SalePrice:          &salePrice,
			DiscountPercent:    discountPercent,
			RemainingStock:     remaining,
			RemainingDisplay:   remaining,
			MaxQuantityPerUser: it.MaxQuantityPerUser,
			EndsAt:             &endsAt,
		}
	}

	return resp, nil
}
