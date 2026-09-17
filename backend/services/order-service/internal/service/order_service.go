package service

import (
	"context"
	"crypto/sha256"
	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/redislock"
	"ecomerce-service/services/order-service/internal/client"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/dto"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

const CanonicalFingerprintVersion = 1

type canonicalFingerprintStruct struct {
	Version         int                          `json:"version"`
	UserID          string                       `json:"user_id"`
	CustomerName    string                       `json:"customer_name"`
	CustomerEmail   string                       `json:"customer_email"`
	CustomerPhone   string                       `json:"customer_phone"`
	ShippingAddress string                       `json:"shipping_address"`
	Note            string                       `json:"note"`
	PaymentMethod   string                       `json:"payment_method"`
	QuoteToken      string                       `json:"quote_token"`
	FromCart        bool                         `json:"from_cart"`
	Items           []dto.CreateOrderItemRequest `json:"items,omitempty"`
}

// normalizeAndMergeItems chuẩn hóa gộp các dòng cùng ProductID và sort tăng dần (R5)
func normalizeAndMergeItems(rawItems []dto.CreateOrderItemRequest) []dto.CreateOrderItemRequest {
	if len(rawItems) == 0 {
		return nil
	}
	merged := make(map[uint]int)
	for _, item := range rawItems {
		if item.Quantity > 0 {
			merged[item.ProductID] += item.Quantity
		}
	}
	items := make([]dto.CreateOrderItemRequest, 0, len(merged))
	for productID, qty := range merged {
		items = append(items, dto.CreateOrderItemRequest{
			ProductID: productID,
			Quantity:  qty,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].ProductID < items[j].ProductID
	})
	return items
}

func computeCanonicalFingerprint(userID string, req *dto.CreateOrderRequest) string {
	payload := canonicalFingerprintStruct{
		Version:         CanonicalFingerprintVersion,
		UserID:          strings.TrimSpace(userID),
		CustomerName:    strings.TrimSpace(req.CustomerName),
		CustomerEmail:   strings.ToLower(strings.TrimSpace(req.CustomerEmail)),
		CustomerPhone:   strings.TrimSpace(req.CustomerPhone),
		ShippingAddress: strings.TrimSpace(req.ShippingAddress),
		Note:            strings.TrimSpace(req.Note),
		PaymentMethod:   strings.TrimSpace(req.PaymentMethod),
		QuoteToken:      strings.TrimSpace(req.QuoteToken),
		FromCart:        req.FromCart,
		Items:           normalizeAndMergeItems(req.Items),
	}
	data, _ := json.Marshal(payload)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type OrderService interface {
	CreateOrder(ctx context.Context, userID string, req *dto.CreateOrderRequest) (*dto.OrderResponse, error)
	GetBasketQuote(ctx context.Context, userID string, req *dto.BasketQuoteRequest) (*dto.BasketQuoteResponse, error)
	GetOrderByID(ctx context.Context, orderID uint, userID string, role string) (*dto.OrderResponse, error)
	GetUserOrders(ctx context.Context, userID string, page int, limit int) (*dto.OrderListResponse, error)
	UpdateOrderStatus(ctx context.Context, orderID uint, req *dto.UpdateOrderStatusRequest) error

	PrewarmStock(ctx context.Context, productID uint, stock int) error
}

type orderService struct {
	orderRepo     domain.OrderRepository
	cartRepo      domain.CartRepository
	fsRepo        domain.FlashSaleRepository
	db            *gorm.DB
	productClient client.ProductClient
	redisClient   *redis.Client
	kafkaProducer pkgKafka.OrderKafkaProducer
	sfGroup       singleflight.Group
	quoteSecret   []byte
}

func NewOrderService(
	orderRepo domain.OrderRepository,
	cartRepo domain.CartRepository,
	productClient client.ProductClient,
	rClient *redis.Client,
	producer pkgKafka.OrderKafkaProducer,
	quoteSecret ...string,
) OrderService {
	var sec string
	if len(quoteSecret) > 0 {
		sec = quoteSecret[0]
	}
	return &orderService{
		orderRepo:     orderRepo,
		cartRepo:      cartRepo,
		productClient: productClient,
		redisClient:   rClient,
		kafkaProducer: producer,
		quoteSecret:   []byte(sec),
	}
}

func (s *orderService) SetQuoteSecret(secret string) {
	if len(secret) >= 16 {
		s.quoteSecret = []byte(secret)
	}
}

func (s *orderService) SetFlashSale(db *gorm.DB, fsRepo domain.FlashSaleRepository) {
	s.db = db
	s.fsRepo = fsRepo
}

type ManifestItem struct {
	CampaignID    uint   `json:"campaign_id"`
	ProductID     uint   `json:"product_id"`
	ReservationID string `json:"reservation_id"`
}

type flashSaleItemReserveInfo struct {
	itemIndex       int
	campaignID      uint
	flashSaleItemID uint
	productID       uint
	salePrice       float64
	reservationID   string
	quantity        int
}

func (s *orderService) recoverAttemptManifest(ctx context.Context, attempt *domain.CheckoutAttempt) {
	if attempt == nil || attempt.ManifestJSON == "" || s.redisClient == nil {
		return
	}
	var items []ManifestItem
	if err := json.Unmarshal([]byte(attempt.ManifestJSON), &items); err != nil {
		return
	}
	for _, item := range items {
		_, _ = redislock.CloseOrReleaseReservation(ctx, s.redisClient, item.CampaignID, item.ProductID, item.ReservationID)
	}
}

func (s *orderService) resolveCommitOutcome(
	ctx context.Context,
	attempt *domain.CheckoutAttempt,
	workerOwnerToken string,
	reservedList []flashSaleItemReserveInfo,
) (*dto.OrderResponse, bool) {
	if attempt == nil || attempt.ID == 0 || s.db == nil {
		for _, rev := range reservedList {
			if s.redisClient != nil {
				_, _ = redislock.CloseOrReleaseReservation(ctx, s.redisClient, rev.campaignID, rev.productID, rev.reservationID)
			}
		}
		return nil, true
	}

	recoveryCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var lockedAttempt domain.CheckoutAttempt
	var order domain.Order
	var isCompleted bool
	var lostOwnership bool
	var canCleanup bool

	txErr := s.db.WithContext(recoveryCtx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Set("gorm:query_option", "FOR UPDATE").
			Where("id = ?", attempt.ID).First(&lockedAttempt).Error; err != nil {
			return err
		}

		if lockedAttempt.Status == domain.CheckoutAttemptStatusCompleted {
			isCompleted = true
			if lockedAttempt.OrderID != nil {
				_ = tx.Preload("Items").Where("id = ?", *lockedAttempt.OrderID).First(&order).Error
			}
			return nil
		}

		if lockedAttempt.Version != attempt.Version || lockedAttempt.OwnerToken != workerOwnerToken {
			lostOwnership = true
			return nil
		}

		canCleanup = true
		lockedAttempt.Status = domain.CheckoutAttemptStatusRecovering
		lockedAttempt.Version++
		lockedAttempt.RecoveryTarget = "ROLLBACK_CLEANUP"
		lockedAttempt.UpdatedAt = time.Now()
		return tx.Save(&lockedAttempt).Error
	})

	if txErr != nil {
		logger.ErrorContext(ctx, "Outcome resolution timeout hoặc DB lỗi", "error", txErr.Error())
		return nil, false
	}

	if isCompleted {
		if lockedAttempt.ResponsePayload != "" {
			var resp dto.OrderResponse
			if err := json.Unmarshal([]byte(lockedAttempt.ResponsePayload), &resp); err == nil && resp.ID > 0 {
				return &resp, true
			}
		}
		if order.ID > 0 {
			resp := s.toOrderResponse(&order)
			return resp, true
		}
		return nil, false
	}

	if lostOwnership {
		return nil, true
	}

	if canCleanup {
		for _, rev := range reservedList {
			if s.redisClient != nil {
				_, _ = redislock.CloseOrReleaseReservation(ctx, s.redisClient, rev.campaignID, rev.productID, rev.reservationID)
			}
		}
		_, _ = s.orderRepo.TransitionAttemptStatus(lockedAttempt.ID, lockedAttempt.Version, domain.CheckoutAttemptStatusRetryable, "ROLLBACK_DONE")
		return nil, true
	}

	return nil, false
}

func (s *orderService) CreateOrder(ctx context.Context, userID string, req *dto.CreateOrderRequest) (*dto.OrderResponse, error) {
	if req == nil {
		return nil, errors.New("request không được rỗng")
	}

	// R5: Chuẩn hóa trên bản sao cục bộ để tránh data race trên pointer truyền vào từ concurrent caller
	clonedReq := *req
	clonedReq.CustomerName = strings.TrimSpace(req.CustomerName)
	clonedReq.CustomerEmail = strings.ToLower(strings.TrimSpace(req.CustomerEmail))
	clonedReq.CustomerPhone = strings.TrimSpace(req.CustomerPhone)
	clonedReq.ShippingAddress = strings.TrimSpace(req.ShippingAddress)
	clonedReq.Note = strings.TrimSpace(req.Note)
	clonedReq.PaymentMethod = strings.TrimSpace(req.PaymentMethod)
	clonedReq.QuoteToken = strings.TrimSpace(req.QuoteToken)
	clonedReq.Items = normalizeAndMergeItems(req.Items)
	req = &clonedReq

	if req.CustomerName == "" || req.CustomerEmail == "" || req.CustomerPhone == "" || req.ShippingAddress == "" {
		return nil, errors.New("vui lòng điền đầy đủ thông tin nhận hàng (Họ tên, Email, Số điện thoại, Địa chỉ)")
	}

	// 10.2 & 10.3 & R1 & R2 & R5: Canonical Request Fingerprint, Fencing Tokens & CAS State Machine
	requestFingerprint := computeCanonicalFingerprint(userID, req)
	var currentAttempt *domain.CheckoutAttempt
	workerOwnerToken := uuid.New().String()
	now := time.Now()
	leaseExp := now.Add(1 * time.Minute)

	if req.IdempotencyKey != "" && s.orderRepo != nil {
		existingAttempt, err := s.orderRepo.GetCheckoutAttempt(userID, req.IdempotencyKey)
		if err != nil {
			return nil, fmt.Errorf("DB_ERROR: không thể kiểm tra idempotency: %w", err)
		}
		if existingAttempt != nil {
			if existingAttempt.RequestFingerprint != requestFingerprint {
				return nil, errors.New("IDEMPOTENCY_CONFLICT: Idempotency-Key đã được sử dụng cho một yêu cầu đặt hàng khác")
			}
			if existingAttempt.Status == domain.CheckoutAttemptStatusCompleted {
				if existingAttempt.ResponsePayload != "" {
					var replayResp dto.OrderResponse
					if err := json.Unmarshal([]byte(existingAttempt.ResponsePayload), &replayResp); err == nil {
						logger.InfoContext(ctx, "Replay kết quả đơn hàng từ CheckoutAttempt bền vững (Zero Quote Expiration check)",
							"idempotency_key", req.IdempotencyKey, "order_id", replayResp.ID)
						return &replayResp, nil
					}
				}
				if existingAttempt.OrderID != nil {
					if order, err := s.orderRepo.FindByID(*existingAttempt.OrderID); err == nil && order != nil {
						return s.toOrderResponse(order), nil
					}
				}
				return nil, errors.New("CHECKOUT_OUTCOME_UNKNOWN: Không thể phục hồi dữ liệu đơn hàng đã tiếp nhận")
			}
			if existingAttempt.Status == domain.CheckoutAttemptStatusRejected {
				reason := existingAttempt.RecoveryTarget
				if reason == "" {
					reason = "Yêu cầu đã bị từ chối"
				}
				return nil, fmt.Errorf("IDEMPOTENCY_CONFLICT: Yêu cầu đặt hàng trước đó đã bị từ chối (%s)", reason)
			}
			if existingAttempt.Status == domain.CheckoutAttemptStatusPending {
				if existingAttempt.LeaseExpiresAt != nil && existingAttempt.LeaseExpiresAt.After(now) {
					return nil, errors.New("ORDER_PROCESSING: Yêu cầu đặt hàng đang được xử lý, vui lòng không gửi lại")
				}
				// Lease đã hết hạn -> Takeover CAS sang RECOVERING (Mục 3.2 & 3.3)
				updated, claimed, err := s.orderRepo.CASRecoveringTakeover(existingAttempt.ID, existingAttempt.Version, workerOwnerToken, leaseExp, "TAKEOVER_RECOVERY")
				if err != nil || !claimed || updated == nil {
					return nil, errors.New("ORDER_PROCESSING: Yêu cầu đặt hàng đang được xử lý bởi worker khác")
				}
				s.recoverAttemptManifest(ctx, updated)
				updated.Status = domain.CheckoutAttemptStatusPending
				updated.OwnerToken = workerOwnerToken
				updated.Version++
				updated.LeaseExpiresAt = &leaseExp
				_ = s.orderRepo.SaveCheckoutAttempt(updated)
				currentAttempt = updated
			} else if existingAttempt.Status == domain.CheckoutAttemptStatusRecovering {
				if existingAttempt.LeaseExpiresAt != nil && existingAttempt.LeaseExpiresAt.After(now) {
					return nil, errors.New("ORDER_PROCESSING: Yêu cầu đang được phục hồi sau sự cố, vui lòng thử lại sau giây lát")
				}
				updated, claimed, err := s.orderRepo.CASRecoveringTakeover(existingAttempt.ID, existingAttempt.Version, workerOwnerToken, leaseExp, "RECOVERING_TIMEOUT")
				if err != nil || !claimed || updated == nil {
					return nil, errors.New("ORDER_PROCESSING: Quá trình phục hồi đang diễn ra bởi worker khác")
				}
				s.recoverAttemptManifest(ctx, updated)
				updated.Status = domain.CheckoutAttemptStatusPending
				updated.OwnerToken = workerOwnerToken
				updated.Version++
				updated.LeaseExpiresAt = &leaseExp
				_ = s.orderRepo.SaveCheckoutAttempt(updated)
				currentAttempt = updated
			} else if existingAttempt.Status == domain.CheckoutAttemptStatusRetryable {
				// Retryable: CAS claim lại thành PENDING với generation mới (Mục 3.1)
				existingAttempt.Status = domain.CheckoutAttemptStatusPending
				existingAttempt.OwnerToken = workerOwnerToken
				existingAttempt.Version++
				existingAttempt.LeaseExpiresAt = &leaseExp
				existingAttempt.RecoveryTarget = ""
				existingAttempt.ManifestJSON = ""
				existingAttempt.UpdatedAt = now
				if err := s.orderRepo.SaveCheckoutAttempt(existingAttempt); err != nil {
					return nil, errors.New("ORDER_PROCESSING: Không thể claim attempt retryable")
				}
				currentAttempt = existingAttempt
			} else if existingAttempt.Status == domain.CheckoutAttemptStatusFailed {
				// Legacy FAILED recovery
				if existingAttempt.OrderID != nil {
					existingAttempt.Status = domain.CheckoutAttemptStatusCompleted
					_ = s.orderRepo.SaveCheckoutAttempt(existingAttempt)
					if order, err := s.orderRepo.FindByID(*existingAttempt.OrderID); err == nil && order != nil {
						return s.toOrderResponse(order), nil
					}
				}
				s.recoverAttemptManifest(ctx, existingAttempt)
				existingAttempt.Status = domain.CheckoutAttemptStatusPending
				existingAttempt.OwnerToken = workerOwnerToken
				existingAttempt.Version++
				existingAttempt.LeaseExpiresAt = &leaseExp
				existingAttempt.UpdatedAt = now
				_ = s.orderRepo.SaveCheckoutAttempt(existingAttempt)
				currentAttempt = existingAttempt
			}
		} else {
			// Insert attempt mới với status PENDING, version 1
			newAttempt := &domain.CheckoutAttempt{
				UserID:             userID,
				IdempotencyKey:     req.IdempotencyKey,
				RequestFingerprint: requestFingerprint,
				FingerprintVersion: CanonicalFingerprintVersion,
				Status:             domain.CheckoutAttemptStatusPending,
				OwnerToken:         workerOwnerToken,
				Version:            1,
				LeaseExpiresAt:     &leaseExp,
				CreatedAt:          now,
				UpdatedAt:          now,
			}
			if err := s.orderRepo.CreateCheckoutAttempt(newAttempt); err != nil {
				// Cạnh tranh song song -> tải lại attempt đã có
				loaded, getErr := s.orderRepo.GetCheckoutAttempt(userID, req.IdempotencyKey)
				if getErr != nil || loaded == nil {
					return nil, errors.New("ORDER_PROCESSING: Xung đột tạo attempt, vui lòng thử lại")
				}
				if loaded.RequestFingerprint != requestFingerprint {
					return nil, errors.New("IDEMPOTENCY_CONFLICT: Idempotency-Key đã được sử dụng cho một yêu cầu đặt hàng khác")
				}
				if loaded.Status == domain.CheckoutAttemptStatusCompleted && loaded.ResponsePayload != "" {
					var replayResp dto.OrderResponse
					if err := json.Unmarshal([]byte(loaded.ResponsePayload), &replayResp); err == nil {
						return &replayResp, nil
					}
				}
				return nil, errors.New("ORDER_PROCESSING: Yêu cầu đặt hàng đang được xử lý, vui lòng không gửi lại")
			}
			currentAttempt = newAttempt
		}
	}

	rejectAttempt := func(reason string) {
		if currentAttempt != nil && currentAttempt.ID > 0 {
			_, _ = s.orderRepo.TransitionAttemptStatus(currentAttempt.ID, currentAttempt.Version, domain.CheckoutAttemptStatusRejected, reason)
		}
	}

	// 16.5 & 17.1 & 18.2: Bắt buộc và xác thực QuoteToken khi không phải replay đơn đã xong (Fail-fast)
	if req.QuoteToken == "" {
		rejectAttempt("QUOTE_REQUIRED")
		return nil, errors.New("QUOTE_REQUIRED: Bắt buộc phải có quote_token hợp lệ để tiến hành đặt hàng")
	}

	quotePayload, qErr := VerifyQuoteToken(s.quoteSecret, req.QuoteToken, userID)
	if qErr != nil {
		if qErr == ErrQuoteExpired {
			rejectAttempt("QUOTE_EXPIRED")
			return nil, fmt.Errorf("QUOTE_EXPIRED: %w", qErr)
		}
		rejectAttempt("INVALID_QUOTE_TOKEN")
		return nil, fmt.Errorf("INVALID_QUOTE_TOKEN: %w", qErr)
	}

	quotedMap := make(map[uint]QuoteItem)
	for _, qItem := range quotePayload.Items {
		quotedMap[qItem.ProductID] = qItem
	}

	var orderItems []domain.OrderItem
	var totalAmount float64
	var cart *domain.Cart

	if req.FromCart {
		if s.cartRepo == nil {
			return nil, errors.New("cart repository không sẵn sàng")
		}
		var err error
		cart, err = s.cartRepo.GetCartByUserID(userID)
		if err != nil || cart == nil || len(cart.Items) == 0 {
			return nil, errors.New("giỏ hàng của bạn đang trống, không thể tạo đơn")
		}

		if len(req.Items) > 0 {
			// Snapshot items được chỉ định rõ (chuẩn Checkout Snapshot)
			cartItemMap := make(map[uint]*domain.CartItem)
			for i := range cart.Items {
				cartItemMap[cart.Items[i].ProductID] = &cart.Items[i]
			}

			groupedMap := make(map[uint]int)
			var uniqueProductIDs []uint
			for _, itemReq := range req.Items {
				if itemReq.ProductID == 0 || itemReq.Quantity <= 0 {
					return nil, errors.New("món hàng không hợp lệ")
				}
				if _, exists := groupedMap[itemReq.ProductID]; !exists {
					uniqueProductIDs = append(uniqueProductIDs, itemReq.ProductID)
				}
				groupedMap[itemReq.ProductID] += itemReq.Quantity
			}

			for _, pid := range uniqueProductIDs {
				qty := groupedMap[pid]
				cItem, exists := cartItemMap[pid]
				if !exists || cItem.Quantity < qty {
					return nil, fmt.Errorf("sản phẩm #%d trong giỏ hàng không đủ số lượng để thanh toán", pid)
				}
				subtotal := cItem.Price * float64(qty)
				totalAmount += subtotal
				orderItems = append(orderItems, domain.OrderItem{
					ProductID:   cItem.ProductID,
					ProductName: cItem.ProductName,
					ProductSlug: cItem.ProductSlug,
					Thumbnail:   cItem.Thumbnail,
					Price:       cItem.Price,
					Quantity:    qty,
					Subtotal:    subtotal,
				})
			}
		} else {
			for _, item := range cart.Items {
				subtotal := item.Price * float64(item.Quantity)
				totalAmount += subtotal
				orderItems = append(orderItems, domain.OrderItem{
					ProductID:   item.ProductID,
					ProductName: item.ProductName,
					ProductSlug: item.ProductSlug,
					Thumbnail:   item.Thumbnail,
					Price:       item.Price,
					Quantity:    item.Quantity,
					Subtotal:    subtotal,
				})
			}
		}
	} else {
		if len(req.Items) == 0 {
			return nil, errors.New("danh sách sản phẩm đặt hàng không được rỗng")
		}

		// P0: Basket Normalization - Gộp các dòng trùng product_id
		groupedMap := make(map[uint]int)
		var uniqueProductIDs []uint
		for _, itemReq := range req.Items {
			if itemReq.ProductID == 0 || itemReq.Quantity <= 0 {
				return nil, errors.New("món hàng không hợp lệ")
			}
			if _, exists := groupedMap[itemReq.ProductID]; !exists {
				uniqueProductIDs = append(uniqueProductIDs, itemReq.ProductID)
			}
			groupedMap[itemReq.ProductID] += itemReq.Quantity
		}

		for _, pid := range uniqueProductIDs {
			qty := groupedMap[pid]
			var name, slug, thumbnail string
			var price float64

			if s.productClient != nil {
				prod, err := s.productClient.GetProduct(ctx, pid)
				if err != nil || prod == nil {
					return nil, fmt.Errorf("sản phẩm #%d không tồn tại", pid)
				}
				name = prod.Name
				slug = prod.Slug
				thumbnail = prod.Thumbnail
				if prod.DiscountPrice > 0 {
					price = prod.DiscountPrice
				} else {
					price = prod.Price
				}
			} else if quoted, ok := quotedMap[pid]; ok {
				price = quoted.QuotedPrice
			}

			subtotal := price * float64(qty)
			totalAmount += subtotal
			orderItems = append(orderItems, domain.OrderItem{
				ProductID:   pid,
				ProductName: name,
				ProductSlug: slug,
				Thumbnail:   thumbnail,
				Price:       price,
				Quantity:    qty,
				Subtotal:    subtotal,
			})
		}
	}

	// 1. Kiểm tra và áp dụng ưu đãi Flash Sale cho từng món hàng nếu có active campaign
	var fsReserves []flashSaleItemReserveInfo
	hasFlashSale := false

	// Section 17.1: Strict Basket Validation - Kiểm tra giỏ hàng gửi lên khớp 1-1 với QuoteToken
	var reqItems []dto.CreateOrderItemRequest
	if req.FromCart {
		for _, it := range orderItems {
			reqItems = append(reqItems, dto.CreateOrderItemRequest{ProductID: it.ProductID, Quantity: it.Quantity})
		}
	} else {
		reqItems = req.Items
	}
	if err := quotePayload.ValidateBasketMatch(reqItems); err != nil {
		return nil, fmt.Errorf("INVALID_QUOTE_TOKEN: %w", err)
	}

	if s.db == nil {
		return nil, errors.New("DATABASE_REQUIRED: Hệ thống đặt hàng yêu cầu kết nối cơ sở dữ liệu để thực hiện transaction")
	}

	var affectedItems []dto.PriceConflictItem

	if s.fsRepo != nil {
		camp, _ := s.fsRepo.GetActiveCampaign()
		now := time.Now()
		isCampActive := camp != nil && camp.Status == domain.CampaignStatusActive && camp.StartsAt.Before(now) && camp.EndsAt.After(now)
		activeMap := make(map[uint]*domain.FlashSaleItem)
		if isCampActive {
			for i := range camp.Items {
				it := &camp.Items[i]
				activeMap[it.ProductID] = it
			}
		}

		for idx := range orderItems {
			pid := orderItems[idx].ProductID
			quoted, wasQuoted := quotedMap[pid]

			// Section 17.1: Nếu khách hàng đã được quote ở chế độ mua giá thường (PurchaseMode == "REGULAR"),
			// TUYỆT ĐỐI KHÔNG kiểm tra Flash Sale để tránh lặp 409 vô tận khi campaign còn ACTIVE nhưng hết suất!
			if wasQuoted && quoted.PurchaseMode == "REGULAR" {
				if orderItems[idx].Price != quoted.QuotedPrice {
					affectedItems = append(affectedItems, dto.PriceConflictItem{
						ProductID:    pid,
						ProductName:  orderItems[idx].ProductName,
						WasFlashSale: false,
						QuotedPrice:  quoted.QuotedPrice,
						UpdatedPrice: orderItems[idx].Price,
						Reason:       "PRICE_CHANGED",
					})
				} else {
					orderItems[idx].Price = quoted.QuotedPrice
					orderItems[idx].Subtotal = quoted.QuotedPrice * float64(orderItems[idx].Quantity)
					orderItems[idx].IsFlashSale = false
					orderItems[idx].CampaignID = nil
				}
				continue
			}

			if it, exists := activeMap[pid]; exists && isCampActive {
				// 10.4: Fail-Closed Redis Policy: Đơn có Flash Sale bắt buộc Redis phải online
				if s.redisClient == nil {
					rejectAttempt("FLASH_SALE_SERVICE_UNAVAILABLE")
					return nil, errors.New("FLASH_SALE_SERVICE_UNAVAILABLE: Hệ thống Flash Sale tạm thời gián đoạn (Redis nil). Vui lòng thử lại sau ít phút")
				}
				stockKey := redislock.KeyStock(camp.ID, pid)
				stockVal, err := s.redisClient.Get(ctx, stockKey).Int()
				if err != nil {
					rejectAttempt("FLASH_SALE_SERVICE_UNAVAILABLE")
					return nil, fmt.Errorf("FLASH_SALE_SERVICE_UNAVAILABLE: Không thể kiểm tra tồn kho Flash Sale (%w)", err)
				}
				remaining := stockVal

				isEligible := true
				if userID != "" && it.MaxQuantityPerUser > 0 {
					resvKey := fmt.Sprintf("fs:{c:%d:p:%d}:user:%s:resv", camp.ID, pid, userID)
					purchasedKey := fmt.Sprintf("fs:{c:%d:p:%d}:user:%s:purchased", camp.ID, pid, userID)
					resvQty, err1 := s.redisClient.Get(ctx, resvKey).Int()
					if err1 != nil && !errors.Is(err1, redis.Nil) {
						rejectAttempt("FLASH_SALE_SERVICE_UNAVAILABLE")
						return nil, fmt.Errorf("FLASH_SALE_SERVICE_UNAVAILABLE: Không thể kiểm tra hạn mức mua hàng Flash Sale (%w)", err1)
					}
					purchasedQty, err2 := s.redisClient.Get(ctx, purchasedKey).Int()
					if err2 != nil && !errors.Is(err2, redis.Nil) {
						rejectAttempt("FLASH_SALE_SERVICE_UNAVAILABLE")
						return nil, fmt.Errorf("FLASH_SALE_SERVICE_UNAVAILABLE: Không thể kiểm tra hạn mức mua hàng Flash Sale (%w)", err2)
					}
					if resvQty+purchasedQty+orderItems[idx].Quantity > it.MaxQuantityPerUser {
						isEligible = false
					}
				}

				if remaining < orderItems[idx].Quantity {
					if wasQuoted && quoted.IsFlashSale {
						affectedItems = append(affectedItems, dto.PriceConflictItem{
							ProductID:    pid,
							ProductName:  orderItems[idx].ProductName,
							WasFlashSale: true,
							QuotedPrice:  quoted.QuotedPrice,
							UpdatedPrice: orderItems[idx].Price, // Giá thường gốc
							Reason:       "FLASH_SALE_OUT_OF_STOCK",
						})
						continue
					}
					return nil, fmt.Errorf("FLASH_SALE_OUT_OF_STOCK: Sản phẩm '%s' đã hết suất ưu đãi Flash Sale (chỉ còn %d suất)", orderItems[idx].ProductName, remaining)
				}
				if !isEligible {
					if wasQuoted && quoted.IsFlashSale {
						affectedItems = append(affectedItems, dto.PriceConflictItem{
							ProductID:    pid,
							ProductName:  orderItems[idx].ProductName,
							WasFlashSale: true,
							QuotedPrice:  quoted.QuotedPrice,
							UpdatedPrice: orderItems[idx].Price,
							Reason:       "FLASH_SALE_QUOTA_EXCEEDED",
						})
						continue
					}
					return nil, fmt.Errorf("FLASH_SALE_QUOTA_EXCEEDED: Bạn đã vượt quá giới hạn mua ưu đãi (%d sản phẩm) cho sản phẩm '%s'", it.MaxQuantityPerUser, orderItems[idx].ProductName)
				}

				// 18.1: So sánh giá sale hiện tại với giá đã quote.
				// Nếu giá sale bị tăng lên sau khi quote, trả về 409 Conflict thay vì âm thầm tính giá cao hơn.
				if wasQuoted && it.SalePrice > quoted.QuotedPrice {
					orderItems[idx].Price = it.SalePrice
					orderItems[idx].Subtotal = it.SalePrice * float64(orderItems[idx].Quantity)
					orderItems[idx].IsFlashSale = true
					campID := camp.ID
					orderItems[idx].CampaignID = &campID
					affectedItems = append(affectedItems, dto.PriceConflictItem{
						ProductID:    pid,
						ProductName:  orderItems[idx].ProductName,
						WasFlashSale: true,
						QuotedPrice:  quoted.QuotedPrice,
						UpdatedPrice: it.SalePrice,
						Reason:       "PRICE_CHANGED",
					})
					continue
				}

				hasFlashSale = true
				orderItems[idx].IsFlashSale = true
				campID := camp.ID
				orderItems[idx].CampaignID = &campID
				orderItems[idx].Price = it.SalePrice
				orderItems[idx].Subtotal = it.SalePrice * float64(orderItems[idx].Quantity)

				fsReserves = append(fsReserves, flashSaleItemReserveInfo{
					itemIndex:       idx,
					campaignID:      camp.ID,
					flashSaleItemID: it.ID, // Gán chính xác ID của FlashSaleItem
					productID:       pid,
					salePrice:       it.SalePrice,
					quantity:        orderItems[idx].Quantity,
				})
			} else {
				// Không nằm trong active campaign: Nếu lúc xem giỏ khách hàng đã được quote là Flash Sale hoặc giá thấp hơn:
				if wasQuoted && quoted.IsFlashSale {
					affectedItems = append(affectedItems, dto.PriceConflictItem{
						ProductID:    pid,
						ProductName:  orderItems[idx].ProductName,
						WasFlashSale: true,
						QuotedPrice:  quoted.QuotedPrice,
						UpdatedPrice: orderItems[idx].Price,
						Reason:       "FLASH_SALE_EXPIRED",
					})
				} else if wasQuoted && orderItems[idx].Price > quoted.QuotedPrice {
					affectedItems = append(affectedItems, dto.PriceConflictItem{
						ProductID:    pid,
						ProductName:  orderItems[idx].ProductName,
						WasFlashSale: false,
						QuotedPrice:  quoted.QuotedPrice,
						UpdatedPrice: orderItems[idx].Price,
						Reason:       "PRICE_CHANGED",
					})
				}
			}
		}
	}

	// 16.5 & 17.1: Nếu có bất kỳ thay đổi giá hoặc hết suất/hết hạn sale, từ chối tạo đơn và trả về 409 Re-quote
	if len(affectedItems) > 0 {
		rejectAttempt("PRICE_CHANGED")
		var newQuoteItems []QuoteItem
		var newQuoteLines []dto.QuoteLineDTO
		var newTotal float64
		for _, it := range orderItems {
			mode := "FLASH_SALE"
			if !it.IsFlashSale {
				mode = "REGULAR"
			}
			newQuoteItems = append(newQuoteItems, QuoteItem{
				ProductID:    it.ProductID,
				Quantity:     it.Quantity,
				QuotedPrice:  it.Price,
				IsFlashSale:  it.IsFlashSale,
				CampaignID:   it.CampaignID,
				PurchaseMode: mode,
			})
			newQuoteLines = append(newQuoteLines, dto.QuoteLineDTO{
				ProductID:    it.ProductID,
				ProductName:  it.ProductName,
				Quantity:     it.Quantity,
				UnitPrice:    it.Price,
				Subtotal:     it.Subtotal,
				IsFlashSale:  it.IsFlashSale,
				CampaignID:   it.CampaignID,
				PurchaseMode: mode,
			})
			newTotal += it.Subtotal
		}
		newQuoteToken, _ := GenerateQuoteToken(s.quoteSecret, userID, newQuoteItems, newTotal, 10*time.Minute)
		return nil, &PriceConflictError{
			ErrorCode: "PRICE_CHANGED",
			Message:   "Một số sản phẩm trong giỏ hàng đã thay đổi giá hoặc hết suất ưu đãi Flash Sale. Vui lòng xác nhận lại đơn hàng.",
			Response: dto.PriceConflictResponse{
				ErrorCode:     "PRICE_CHANGED",
				Message:       "Giá sản phẩm đã được cập nhật theo giá hiện hành.",
				NewQuoteToken: newQuoteToken,
				NewTotal:      newTotal,
				AffectedItems: affectedItems,
				NewItems:      newQuoteLines,
			},
		}
	}

	// Cập nhật lại tổng tiền sau khi áp giá Flash Sale
	totalAmount = 0
	for _, it := range orderItems {
		totalAmount += it.Subtotal
	}

	orderCode := fmt.Sprintf("ORD-%s", strings.ToUpper(uuid.New().String()[:8]))

	if s.db != nil {
		if hasFlashSale {
			// 10.4: Fail-Closed Redis Policy: Đơn có Flash Sale bắt buộc Redis phải online
			if s.redisClient == nil {
				rejectAttempt("FLASH_SALE_SERVICE_UNAVAILABLE")
				return nil, errors.New("FLASH_SALE_SERVICE_UNAVAILABLE: Hệ thống Flash Sale tạm thời gián đoạn (Redis nil). Vui lòng thử lại sau ít phút")
			}
			if err := s.redisClient.Ping(ctx).Err(); err != nil {
				rejectAttempt("FLASH_SALE_SERVICE_UNAVAILABLE")
				return nil, fmt.Errorf("FLASH_SALE_SERVICE_UNAVAILABLE: Hệ thống Flash Sale tạm thời gián đoạn (%w). Vui lòng thử lại sau ít phút", err)
			}
			if req.PaymentMethod != domain.PaymentMethodCOD {
				rejectAttempt("COD_REQUIRED")
				return nil, errors.New("đơn hàng có sản phẩm Flash Sale hiện chỉ hỗ trợ phương thức thanh toán khi nhận hàng (COD)")
			}
		}

		// Mục 3.3: Chốt ID ổn định theo (attempt_id, generation, campaign_id, product_id)
		var manifestItems []ManifestItem
		var attemptID uint
		var attemptVer uint64 = 1
		if currentAttempt != nil {
			attemptID = currentAttempt.ID
			attemptVer = currentAttempt.Version
		}
		for _, fs := range fsReserves {
			resvID := fmt.Sprintf("FSR-%d-%d-%d-%d", attemptID, attemptVer, fs.campaignID, fs.productID)
			manifestItems = append(manifestItems, ManifestItem{
				CampaignID:    fs.campaignID,
				ProductID:     fs.productID,
				ReservationID: resvID,
			})
		}
		// Trước khi gọi Lua, persist manifest các ID dự định giữ (Mục 3.3)
		if currentAttempt != nil && len(manifestItems) > 0 {
			manifestBytes, _ := json.Marshal(manifestItems)
			currentAttempt.ManifestJSON = string(manifestBytes)
			_ = s.orderRepo.SaveCheckoutAttempt(currentAttempt)
		}

		// Giữ chỗ từng món Flash Sale trên Redis (TTL 5 phút)
		var reservedList []flashSaleItemReserveInfo
		for idx, fs := range fsReserves {
			resvID := manifestItems[idx].ReservationID
			if s.redisClient == nil {
				rejectAttempt("FLASH_SALE_SERVICE_UNAVAILABLE")
				return nil, errors.New("FLASH_SALE_SERVICE_UNAVAILABLE: Hệ thống Flash Sale tạm thời gián đoạn")
			}
			resp, err := redislock.ReserveFlashSaleStock(
				ctx, s.redisClient,
				fs.campaignID, fs.productID,
				userID, fmt.Sprintf("req-%s", resvID), "mixed-checkout", resvID,
				fs.quantity,
				300,
			)
			if err != nil {
				for _, rev := range reservedList {
					_, _ = redislock.CloseOrReleaseReservation(ctx, s.redisClient, rev.campaignID, rev.productID, rev.reservationID)
				}
				rejectAttempt("FLASH_SALE_SERVICE_UNAVAILABLE")
				return nil, fmt.Errorf("FLASH_SALE_SERVICE_UNAVAILABLE: lỗi giữ chỗ tồn kho trên Redis: %w", err)
			}
			if resp == nil || resp.Code != redislock.ResultReserved {
				for _, rev := range reservedList {
					_, _ = redislock.CloseOrReleaseReservation(ctx, s.redisClient, rev.campaignID, rev.productID, rev.reservationID)
				}
				code := "UNKNOWN"
				if resp != nil {
					code = string(resp.Code)
				}
				rejectAttempt("FLASH_SALE_OUT_OF_STOCK")
				return nil, fmt.Errorf("FLASH_SALE_OUT_OF_STOCK: sản phẩm #%d không thể giữ chỗ trên Redis (code: %s)", fs.productID, code)
			}
			fs.reservationID = resvID
			orderItems[fs.itemIndex].ReservationID = resvID
			reservedList = append(reservedList, fs)
		}

		// Tách các món thường trong đơn
		var regularItems []pkgKafka.OrderItemPayload
		for _, it := range orderItems {
			if !it.IsFlashSale {
				regularItems = append(regularItems, pkgKafka.OrderItemPayload{
					ProductID:   it.ProductID,
					ProductName: it.ProductName,
					ProductSlug: it.ProductSlug,
					Thumbnail:   it.Thumbnail,
					Price:       it.Price,
					Quantity:    it.Quantity,
					Subtotal:    it.Subtotal,
				})
			}
		}

		order := &domain.Order{
			OrderCode:       orderCode,
			UserID:          userID,
			CustomerName:    req.CustomerName,
			CustomerEmail:   req.CustomerEmail,
			CustomerPhone:   req.CustomerPhone,
			ShippingAddress: req.ShippingAddress,
			Note:            req.Note,
			PaymentMethod:   req.PaymentMethod,
			PaymentStatus:   domain.PaymentStatusPending,
			OrderStatus:     domain.OrderStatusPending,
			TotalAmount:     totalAmount,
			Items:           orderItems,
		}

		// Point 1, 2, 5: Thực hiện trong 1 Transaction Database nguyên tử duy nhất
		now := time.Now()
		var txErr error

		if s.db != nil {
			txErr = s.db.Transaction(func(tx *gorm.DB) error {
				// Bước 1 & 2 (Mục 3.2): SELECT ... FOR UPDATE attempt và xác minh Fencing
				if currentAttempt != nil && currentAttempt.ID > 0 {
					var lockedAttempt domain.CheckoutAttempt
					if err := tx.Set("gorm:query_option", "FOR UPDATE").
						Where("id = ?", currentAttempt.ID).First(&lockedAttempt).Error; err != nil {
						return fmt.Errorf("FENCING_ERROR: không thể khóa attempt: %w", err)
					}
					if lockedAttempt.Status != domain.CheckoutAttemptStatusPending {
						return fmt.Errorf("FENCING_LOST: attempt status là %s, kỳ vọng PENDING", lockedAttempt.Status)
					}
					if lockedAttempt.OwnerToken != workerOwnerToken {
						return errors.New("FENCING_LOST: owner_token không trùng khớp, worker khác đã takeover")
					}
					if lockedAttempt.Version != currentAttempt.Version {
						return fmt.Errorf("FENCING_LOST: attempt version %d đã bị thay đổi thành %d", currentAttempt.Version, lockedAttempt.Version)
					}
				}

				// Bước 3 (Mục 3.2): Campaign Lifecycle Synchronization: Lấy SHARE lock trên các campaigns tham gia
				if len(reservedList) > 0 && s.fsRepo != nil {
					var campIDs []uint
					campSeen := make(map[uint]bool)
					for _, rev := range reservedList {
						if !campSeen[rev.campaignID] {
							campSeen[rev.campaignID] = true
							campIDs = append(campIDs, rev.campaignID)
						}
					}
					sort.Slice(campIDs, func(i, j int) bool { return campIDs[i] < campIDs[j] })
					lockedCamps, err := s.fsRepo.GetCampaignsForShare(tx, campIDs)
					if err != nil {
						return fmt.Errorf("lỗi kiểm tra trạng thái chiến dịch Flash Sale: %w", err)
					}
					if len(lockedCamps) != len(campIDs) {
						return errors.New("CAMPAIGN_NOT_FOUND: Một số chiến dịch Flash Sale không tồn tại")
					}
					for _, lc := range lockedCamps {
						if lc.Status != domain.CampaignStatusActive {
							return fmt.Errorf("CAMPAIGN_ENDED: Chiến dịch Flash Sale '%s' đã kết thúc hoặc không còn hoạt động (hiện tại: %s)", lc.Name, lc.Status)
						}
					}
				}

				// Bước 4 (Mục 3.2): Tạo đơn hàng gắn CheckoutAttemptID và chi tiết
				if currentAttempt != nil && currentAttempt.ID > 0 {
					order.CheckoutAttemptID = &currentAttempt.ID
				}
				if err := tx.Create(order).Error; err != nil {
					return fmt.Errorf("lỗi tạo đơn hàng: %w", err)
				}

				// 2. Tạo bản ghi flash_sale_reservations và tăng reserved_stock trên flash_sale_items
				for _, rev := range reservedList {
					resvRecord := domain.FlashSaleReservation{
						ID:                 rev.reservationID,
						RequestID:          fmt.Sprintf("req-%s", rev.reservationID),
						RequestFingerprint: "mixed-cart-checkout",
						CampaignID:         rev.campaignID,
						FlashSaleItemID:    rev.flashSaleItemID, // Point 1: ID chính xác của FlashSaleItem
						ProductID:          rev.productID,
						UserID:             userID,
						Quantity:           rev.quantity,
						UnitPrice:          rev.salePrice,
						TotalAmount:        rev.salePrice * float64(rev.quantity),
						PaymentMethod:      req.PaymentMethod,
						Status:             domain.ReservationStatusReserved,
						OrderID:            &order.ID,
						ExpiresAt:          now.Add(5 * time.Minute),
					}
					if err := tx.Create(&resvRecord).Error; err != nil {
						return fmt.Errorf("lỗi tạo reservation #%s: %w", rev.reservationID, err)
					}

					// 16.3: Bảo vệ giới hạn Flash Sale allocation bằng conditional update nguyên tử
					res := tx.Model(&domain.FlashSaleItem{}).
						Where("id = ? AND campaign_id = ? AND reserved_stock + sold_stock + ? <= allocated_stock",
							rev.flashSaleItemID, rev.campaignID, rev.quantity).
						Update("reserved_stock", gorm.Expr("reserved_stock + ?", rev.quantity))
					if res.Error != nil {
						return fmt.Errorf("lỗi tăng reserved_stock cho item #%d: %w", rev.flashSaleItemID, res.Error)
					}
					if res.RowsAffected == 0 {
						return fmt.Errorf("ALLOCATION_EXCEEDED: Sản phẩm Flash Sale #%d đã hết hạn mức phân bổ trong campaign #%d", rev.flashSaleItemID, rev.campaignID)
					}
				}

				if len(regularItems) > 0 {
					// Point 2: Ghi Outbox Event MIXED_STOCK_DEDUCT_REQUEST để đảm bảo tin cậy 100%
					reqPayload := pkgKafka.MixedOrderStockRequestPayload{
						EventID:      fmt.Sprintf("mixed-req-%d", order.ID),
						EventType:    pkgKafka.EventMixedStockDeductRequest,
						OrderID:      order.ID,
						OrderCode:    order.OrderCode,
						RegularItems: regularItems,
						TraceID:      logger.GetTraceID(ctx),
						Timestamp:    now,
					}
					reqData, err := json.Marshal(reqPayload)
					if err != nil {
						return fmt.Errorf("lỗi serialize MixedOrderStockRequestPayload: %w", err)
					}

					outboxReq := &domain.OutboxEvent{
						ID:            fmt.Sprintf("outbox-mixed-req-%d", order.ID),
						AggregateType: "order",
						AggregateID:   fmt.Sprintf("%d", order.ID),
						EventType:     pkgKafka.EventMixedStockDeductRequest,
						Topic:         pkgKafka.TopicMixedOrderStockRequest,
						PartitionKey:  fmt.Sprintf("order-%d", order.ID),
						Payload:       string(reqData),
						Status:        domain.OutboxStatusPending,
						CreatedAt:     now,
					}
					if err := tx.Create(outboxReq).Error; err != nil {
						return fmt.Errorf("lỗi ghi outbox MixedOrderStockRequest: %w", err)
					}
				} else {
					// Point 5: Đơn 100% Flash Sale -> Xác nhận ngay lập tức trong transaction
					order.OrderStatus = domain.OrderStatusConfirmed
					if err := tx.Model(&domain.Order{}).Where("id = ?", order.ID).Update("order_status", domain.OrderStatusConfirmed).Error; err != nil {
						return fmt.Errorf("lỗi cập nhật order_status CONFIRMED: %w", err)
					}

					for _, rev := range reservedList {
						// Xác nhận reservation trong DB
						if s.fsRepo != nil {
							if err := s.fsRepo.ConfirmReservationDB(tx, rev.reservationID, order.ID); err != nil {
								return fmt.Errorf("lỗi xác nhận reservation #%s: %w", rev.reservationID, err)
							}
						}

						// Ghi Outbox FLASH_SALE_ORDER_CONFIRMED để Product Service tăng sold_quantity
						fsConfirmedPayload := pkgKafka.FlashSaleOrderConfirmedPayload{
							EventID:       fmt.Sprintf("fs-conf-%s", rev.reservationID),
							EventType:     pkgKafka.EventFlashSaleOrderConfirmed,
							OccurredAt:    now,
							TraceID:       logger.GetTraceID(ctx),
							OrderID:       order.ID,
							OrderCode:     order.OrderCode,
							ReservationID: rev.reservationID,
							CampaignID:    rev.campaignID,
							ProductID:     rev.productID,
							Quantity:      rev.quantity,
						}
						fsData, _ := json.Marshal(fsConfirmedPayload)
						outboxFS := &domain.OutboxEvent{
							ID:            fmt.Sprintf("outbox-fs-conf-%s", rev.reservationID),
							AggregateType: "flash_sale",
							AggregateID:   fmt.Sprintf("%d", rev.campaignID),
							EventType:     pkgKafka.EventFlashSaleOrderConfirmed,
							Topic:         pkgKafka.TopicFlashSaleConfirmed,
							PartitionKey:  fmt.Sprintf("product-%d", rev.productID),
							Payload:       string(fsData),
							Status:        domain.OutboxStatusPending,
							CreatedAt:     now,
						}
						if err := tx.Create(outboxFS).Error; err != nil {
							return fmt.Errorf("lỗi tạo outbox FLASH_SALE_ORDER_CONFIRMED: %w", err)
						}
					}

					// Ghi Outbox ORDER_CREATED cho downstream notification
					var eventItems []pkgKafka.OrderItemPayload
					for _, it := range order.Items {
						eventItems = append(eventItems, pkgKafka.OrderItemPayload{
							ProductID:     it.ProductID,
							ProductName:   it.ProductName,
							ProductSlug:   it.ProductSlug,
							Thumbnail:     it.Thumbnail,
							Price:         it.Price,
							Quantity:      it.Quantity,
							Subtotal:      it.Subtotal,
							IsFlashSale:   it.IsFlashSale,
							CampaignID:    it.CampaignID,
							ReservationID: it.ReservationID,
						})
					}

					orderCreatedPayload := pkgKafka.OrderCreatedPayload{
						EventType:          pkgKafka.EventOrderCreated,
						OrderID:            order.ID,
						OrderCode:          order.OrderCode,
						UserID:             order.UserID,
						CustomerEmail:      order.CustomerEmail,
						CustomerName:       order.CustomerName,
						CustomerPhone:      order.CustomerPhone,
						ShippingAddress:    order.ShippingAddress,
						TotalAmount:        order.TotalAmount,
						PaymentMethod:      order.PaymentMethod,
						Items:              eventItems,
						IsFlashSale:        true,
						StockHandledBySaga: true,
						CampaignID:         &reservedList[0].campaignID,
						ReservationID:      reservedList[0].reservationID,
						TraceID:            logger.GetTraceID(ctx),
						CreatedAt:          now,
					}
					orderData, _ := json.Marshal(orderCreatedPayload)
					outboxOrder := &domain.OutboxEvent{
						ID:            fmt.Sprintf("outbox-order-created-%d", order.ID),
						AggregateType: "order",
						AggregateID:   fmt.Sprintf("%d", order.ID),
						EventType:     pkgKafka.EventOrderCreated,
						Topic:         pkgKafka.TopicOrderEvents,
						PartitionKey:  fmt.Sprintf("order-%d", order.ID),
						Payload:       string(orderData),
						Status:        domain.OutboxStatusPending,
						CreatedAt:     now,
					}
					if err := tx.Create(outboxOrder).Error; err != nil {
						return fmt.Errorf("lỗi tạo outbox ORDER_CREATED: %w", err)
					}
				}

				// Bước 5 (Mục 3.2 & R1): Cập nhật CheckoutAttempt thành COMPLETED trong cùng Transaction có Fencing check
				if req.IdempotencyKey != "" {
					respObj := s.toOrderResponse(order)
					respData, _ := json.Marshal(respObj)
					if currentAttempt != nil && currentAttempt.ID > 0 {
						if err := s.orderRepo.CompleteAttemptInTx(tx, currentAttempt.ID, currentAttempt.Version, order.ID, order.OrderCode, string(respData)); err != nil {
							return fmt.Errorf("lỗi cập nhật checkout_attempt: %w", err)
						}
					} else {
						attemptRecord := &domain.CheckoutAttempt{
							UserID:             userID,
							IdempotencyKey:     req.IdempotencyKey,
							RequestFingerprint: requestFingerprint,
							FingerprintVersion: CanonicalFingerprintVersion,
							Status:             domain.CheckoutAttemptStatusCompleted,
							OrderID:            &order.ID,
							OrderCode:          order.OrderCode,
							ResponsePayload:    string(respData),
							CreatedAt:          now,
							UpdatedAt:          now,
						}
						if err := tx.Create(attemptRecord).Error; err != nil {
							return fmt.Errorf("lỗi ghi checkout_attempt: %w", err)
						}
					}
				}

				return nil
			})
		}

		if txErr != nil {
			// R4: Outcome Resolution - Phân giải outcome trước khi giải phóng Redis
			orderResp, resolved := s.resolveCommitOutcome(ctx, currentAttempt, workerOwnerToken, reservedList)
			if resolved {
				if orderResp != nil {
					return orderResp, nil
				}
				logger.ErrorContext(ctx, "Lỗi tạo đơn hàng trong Transaction (đã phân giải rollback và dọn dẹp)", "error", txErr.Error())
				return nil, fmt.Errorf("không thể tạo đơn hàng: %w", txErr)
			}
			logger.ErrorContext(ctx, "Commit outcome chưa xác định (DB timeout / network partition)", "error", txErr.Error())
			return nil, errors.New("CHECKOUT_OUTCOME_UNKNOWN: Trạng thái đơn hàng chưa được xác định. Vui lòng kiểm tra lại sau ít phút")
		}

		if s.cartRepo != nil && req.FromCart {
			// Section 6.3: Dọn giỏ hàng theo quantity delta snapshot, bảo toàn các món khách thêm sau
			latestCart, err := s.cartRepo.GetCartByUserID(userID)
			if err == nil && latestCart != nil && len(latestCart.Items) > 0 {
				for _, orderItem := range orderItems {
					for _, cartItem := range latestCart.Items {
						if cartItem.ProductID == orderItem.ProductID {
							remainingQty := cartItem.Quantity - orderItem.Quantity
							if remainingQty <= 0 {
								_ = s.cartRepo.RemoveItem(latestCart.ID, cartItem.ID)
							} else {
								_ = s.cartRepo.UpdateItemQuantity(latestCart.ID, cartItem.ID, remainingQty)
							}
							break
						}
					}
				}
			}
		}

		if len(regularItems) > 0 {
			// 16.1: Bỏ direct publish qua HTTP; 100% việc phát yêu cầu trừ kho qua Outbox Publisher Worker
		} else {
			// Đơn 100% Flash Sale -> Sau khi DB commit thành công, confirm trên Redis
			for _, rev := range reservedList {
				if s.redisClient != nil {
					_, _ = redislock.ConfirmFlashSaleReservation(ctx, s.redisClient, rev.campaignID, rev.productID, rev.reservationID)
				}
			}
		}

		return s.toOrderResponse(order), nil
	}

	return nil, errors.New("DATABASE_REQUIRED: Hệ thống đặt hàng yêu cầu kết nối cơ sở dữ liệu để thực hiện transaction")
}


// PrewarmStock nạp trước số lượng tồn kho Flash Sale lên RAM Redis
func (s *orderService) PrewarmStock(ctx context.Context, productID uint, stock int) error {
	if stock < 0 {
		return errors.New("số lượng tồn kho không hợp lệ")
	}

	return redislock.PrewarmStock(ctx, s.redisClient, productID, stock)
}

func (s *orderService) GetOrderByID(ctx context.Context, orderID uint, userID string, role string) (*dto.OrderResponse, error) {
	order, err := s.orderRepo.FindByID(orderID)
	if err != nil || order == nil {
		return nil, errors.New("không tìm thấy đơn hàng")
	}

	if role != domain.RoleAdmin && role != domain.RoleTechnician && order.UserID != userID {
		return nil, errors.New("bạn không có quyền xem đơn hàng này")
	}

	return s.toOrderResponse(order), nil
}

func (s *orderService) GetUserOrders(ctx context.Context, userID string, page int, limit int) (*dto.OrderListResponse, error) {
	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 50 {
		limit = 10
	}

	orders, total, err := s.orderRepo.FindByUserID(userID, page, limit)
	if err != nil {
		return nil, fmt.Errorf("lỗi lấy danh sách đơn hàng: %w", err)
	}

	var orderResponses []*dto.OrderResponse
	for _, o := range orders {
		orderResponses = append(orderResponses, s.toOrderResponse(o))
	}

	totalPages := int((total + int64(limit) - 1) / int64(limit))

	return &dto.OrderListResponse{
		Orders:     orderResponses,
		Total:      total,
		Page:       page,
		Limit:      limit,
		TotalPages: totalPages,
	}, nil
}

func (s *orderService) UpdateOrderStatus(ctx context.Context, orderID uint, req *dto.UpdateOrderStatusRequest) error {
	order, err := s.orderRepo.FindByID(orderID)
	if err != nil || order == nil {
		return errors.New("không tìm thấy đơn hàng")
	}

	if err := s.orderRepo.UpdateStatus(orderID, req.Status); err != nil {
		return fmt.Errorf("lỗi cập nhật trạng thái đơn hàng: %w", err)
	}

	return nil
}

func (s *orderService) toOrderResponse(order *domain.Order) *dto.OrderResponse {
	var itemResponses []dto.OrderItemResponse
	for _, item := range order.Items {
		itemResponses = append(itemResponses, dto.OrderItemResponse{
			ID:          item.ID,
			ProductID:   item.ProductID,
			ProductName: item.ProductName,
			ProductSlug: item.ProductSlug,
			Thumbnail:   item.Thumbnail,
			Price:       item.Price,
			Quantity:    item.Quantity,
			Subtotal:    item.Subtotal,
			IsFlashSale: item.IsFlashSale,
			CampaignID:  item.CampaignID,
		})
	}

	return &dto.OrderResponse{
		ID:              order.ID,
		OrderCode:       order.OrderCode,
		UserID:          order.UserID,
		CustomerName:    order.CustomerName,
		CustomerEmail:   order.CustomerEmail,
		CustomerPhone:   order.CustomerPhone,
		ShippingAddress: order.ShippingAddress,
		Note:            order.Note,
		PaymentMethod:   order.PaymentMethod,
		PaymentStatus:   order.PaymentStatus,
		OrderStatus:     order.OrderStatus,
		TotalAmount:     order.TotalAmount,
		Items:           itemResponses,
		CreatedAt:       order.CreatedAt.Format("02/01/2006 15:04:05"),
	}
}

// GetBasketQuote tính toán báo giá cho toàn bộ giỏ hàng, nhận diện chính xác Flash Sale và xuất QuoteToken đã ký (17.1)
func (s *orderService) GetBasketQuote(ctx context.Context, userID string, req *dto.BasketQuoteRequest) (*dto.BasketQuoteResponse, error) {
	if req == nil {
		return nil, errors.New("request không hợp lệ")
	}

	type rawQuoteItem struct {
		productID uint
		quantity  int
	}
	var itemsToQuote []rawQuoteItem

	if req.FromCart {
		if s.cartRepo == nil {
			return nil, errors.New("cart repository không sẵn sàng")
		}
		cart, err := s.cartRepo.GetCartByUserID(userID)
		if err != nil || cart == nil || len(cart.Items) == 0 {
			return nil, errors.New("giỏ hàng của bạn đang trống, không thể báo giá")
		}
		for _, it := range cart.Items {
			itemsToQuote = append(itemsToQuote, rawQuoteItem{productID: it.ProductID, quantity: it.Quantity})
		}
	} else {
		if len(req.Items) == 0 {
			return nil, errors.New("danh sách sản phẩm cần báo giá không được rỗng")
		}
		for _, it := range req.Items {
			if it.ProductID == 0 || it.Quantity <= 0 {
				return nil, errors.New("sản phẩm báo giá không hợp lệ")
			}
			itemsToQuote = append(itemsToQuote, rawQuoteItem{productID: it.ProductID, quantity: it.Quantity})
		}
	}

	var activeMap map[uint]*domain.FlashSaleItem
	var campID uint
	isCampActive := false

	if s.fsRepo != nil {
		camp, _ := s.fsRepo.GetActiveCampaign()
		now := time.Now()
		if camp != nil && camp.Status == domain.CampaignStatusActive && camp.StartsAt.Before(now) && camp.EndsAt.After(now) {
			isCampActive = true
			campID = camp.ID
			activeMap = make(map[uint]*domain.FlashSaleItem)
			for i := range camp.Items {
				it := &camp.Items[i]
				activeMap[it.ProductID] = it
			}
		}
	}

	var quoteLines []dto.QuoteLineDTO
	var quoteItems []QuoteItem
	var total float64

	for _, item := range itemsToQuote {
		var name string
		var regularPrice float64

		if s.productClient != nil {
			prod, err := s.productClient.GetProduct(ctx, item.productID)
			if err != nil || prod == nil {
				return nil, fmt.Errorf("sản phẩm #%d không tồn tại", item.productID)
			}
			name = prod.Name
			if prod.DiscountPrice > 0 {
				regularPrice = prod.DiscountPrice
			} else {
				regularPrice = prod.Price
			}
		} else {
			name = fmt.Sprintf("Product %d", item.productID)
			regularPrice = 100000
		}

		unitPrice := regularPrice
		isFlashSale := false
		purchaseMode := "REGULAR"
		var assignedCampID *uint

		if isCampActive {
			if fsItem, exists := activeMap[item.productID]; exists {
				remaining := fsItem.AllocatedStock - fsItem.SoldStock - fsItem.ReservedStock
				if s.redisClient != nil {
					stockKey := redislock.KeyStock(campID, item.productID)
					if stockVal, err := s.redisClient.Get(ctx, stockKey).Int(); err == nil {
						remaining = stockVal
					}
				}

				isEligible := true
				if userID != "" && fsItem.MaxQuantityPerUser > 0 && s.redisClient != nil {
					resvKey := fmt.Sprintf("fs:{c:%d:p:%d}:user:%s:resv", campID, item.productID, userID)
					purchasedKey := fmt.Sprintf("fs:{c:%d:p:%d}:user:%s:purchased", campID, item.productID, userID)
					resvQty, _ := s.redisClient.Get(ctx, resvKey).Int()
					purchasedQty, _ := s.redisClient.Get(ctx, purchasedKey).Int()
					if resvQty+purchasedQty+item.quantity > fsItem.MaxQuantityPerUser {
						isEligible = false
					}
				}

				if remaining >= item.quantity && isEligible {
					unitPrice = fsItem.SalePrice
					isFlashSale = true
					purchaseMode = "FLASH_SALE"
					assignedCampID = &campID
				}
			}
		}

		subtotal := unitPrice * float64(item.quantity)
		total += subtotal

		quoteLines = append(quoteLines, dto.QuoteLineDTO{
			ProductID:    item.productID,
			ProductName:  name,
			Quantity:     item.quantity,
			UnitPrice:    unitPrice,
			Subtotal:     subtotal,
			IsFlashSale:  isFlashSale,
			CampaignID:   assignedCampID,
			PurchaseMode: purchaseMode,
		})

		quoteItems = append(quoteItems, QuoteItem{
			ProductID:    item.productID,
			Quantity:     item.quantity,
			QuotedPrice:  unitPrice,
			IsFlashSale:  isFlashSale,
			CampaignID:   assignedCampID,
			PurchaseMode: purchaseMode,
		})
	}

	ttl := 10 * time.Minute
	token, err := GenerateQuoteToken(s.quoteSecret, userID, quoteItems, total, ttl)
	if err != nil {
		return nil, fmt.Errorf("lỗi sinh quote token: %w", err)
	}

	return &dto.BasketQuoteResponse{
		QuoteToken: token,
		Total:      total,
		ExpiresAt:  time.Now().Add(ttl).Unix(),
		Items:      quoteLines,
	}, nil
}
