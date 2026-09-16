package service

import (
	"context"
	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/redislock"
	"ecomerce-service/services/order-service/internal/client"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/dto"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

type OrderService interface {
	CreateOrder(ctx context.Context, userID string, req *dto.CreateOrderRequest) (*dto.OrderResponse, error)
	GetBasketQuote(ctx context.Context, userID string, req *dto.BasketQuoteRequest) (*dto.BasketQuoteResponse, error)
	GetOrderByID(ctx context.Context, orderID uint, userID string, role string) (*dto.OrderResponse, error)
	GetUserOrders(ctx context.Context, userID string, page int, limit int) (*dto.OrderListResponse, error)
	UpdateOrderStatus(ctx context.Context, orderID uint, req *dto.UpdateOrderStatusRequest) error

	// Flash Sale High-Concurrency Methods
	CreateFlashSaleOrderAsync(ctx context.Context, userID string, req *dto.FlashSaleOrderRequest) (*dto.FlashSaleOrderAsyncResponse, error)
	GetFlashSaleOrderStatus(ctx context.Context, token string) (*dto.FlashSaleStatusResponse, error)
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

func (s *orderService) CreateOrder(ctx context.Context, userID string, req *dto.CreateOrderRequest) (*dto.OrderResponse, error) {
	if req.CustomerName == "" || req.CustomerEmail == "" || req.CustomerPhone == "" || req.ShippingAddress == "" {
		return nil, errors.New("vui lòng điền đầy đủ thông tin nhận hàng (Họ tên, Email, Số điện thoại, Địa chỉ)")
	}

	// 16.5 & 17.1 & 18.2: Bắt buộc và xác thực QuoteToken ngay từ cửa vào (Fail-fast)
	if req.QuoteToken == "" {
		return nil, errors.New("QUOTE_REQUIRED: Bắt buộc phải có quote_token hợp lệ để tiến hành đặt hàng")
	}

	quotePayload, qErr := VerifyQuoteToken(s.quoteSecret, req.QuoteToken, userID)
	if qErr != nil {
		if qErr == ErrQuoteExpired {
			return nil, fmt.Errorf("QUOTE_EXPIRED: %w", qErr)
		}
		return nil, fmt.Errorf("INVALID_QUOTE_TOKEN: %w", qErr)
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
	} else {
		if len(req.Items) == 0 {
			return nil, errors.New("danh sách sản phẩm đặt hàng không được rỗng")
		}

		for _, itemReq := range req.Items {
			if itemReq.ProductID == 0 || itemReq.Quantity <= 0 {
				return nil, errors.New("món hàng không hợp lệ")
			}

			var name, slug, thumbnail string
			var price float64

			if s.productClient != nil {
				prod, err := s.productClient.GetProduct(ctx, itemReq.ProductID)
				if err != nil || prod == nil {
					return nil, fmt.Errorf("sản phẩm #%d không tồn tại", itemReq.ProductID)
				}
				name = prod.Name
				slug = prod.Slug
				thumbnail = prod.Thumbnail
				if prod.DiscountPrice > 0 {
					price = prod.DiscountPrice
				} else {
					price = prod.Price
				}
			}

			subtotal := price * float64(itemReq.Quantity)
			totalAmount += subtotal
			orderItems = append(orderItems, domain.OrderItem{
				ProductID:   itemReq.ProductID,
				ProductName: name,
				ProductSlug: slug,
				Thumbnail:   thumbnail,
				Price:       price,
				Quantity:    itemReq.Quantity,
				Subtotal:    subtotal,
			})
		}
	}

	// 1. Kiểm tra và áp dụng ưu đãi Flash Sale cho từng món hàng nếu có active campaign
	type flashSaleItemReserveInfo struct {
		itemIndex       int
		campaignID      uint
		flashSaleItemID uint // Point 1: Phải là FlashSaleItem.ID chính xác
		productID       uint
		salePrice       float64
		reservationID   string
		quantity        int
	}
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

	quotedMap := make(map[uint]QuoteItem)
	for _, qItem := range quotePayload.Items {
		quotedMap[qItem.ProductID] = qItem
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
				remaining := it.AllocatedStock - it.SoldStock - it.ReservedStock
				if s.redisClient != nil {
					stockKey := redislock.KeyStock(camp.ID, pid)
					if stockVal, err := s.redisClient.Get(ctx, stockKey).Int(); err == nil {
						remaining = stockVal
					}
				}

				isEligible := true
				if userID != "" && it.MaxQuantityPerUser > 0 && s.redisClient != nil {
					resvKey := fmt.Sprintf("fs:{c:%d:p:%d}:user:%s:resv", camp.ID, pid, userID)
					purchasedKey := fmt.Sprintf("fs:{c:%d:p:%d}:user:%s:purchased", camp.ID, pid, userID)
					resvQty, _ := s.redisClient.Get(ctx, resvKey).Int()
					purchasedQty, _ := s.redisClient.Get(ctx, purchasedKey).Int()
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

	// Route mọi checkout có DB qua cùng Saga trừ kho có operation ledger và
	// transactional outbox. Đường legacy order.created -> ProductStockWorker
	// không an toàn khi publish result lỗi rồi message được giao lại.
	if s.db != nil {
		if hasFlashSale && req.PaymentMethod != domain.PaymentMethodCOD {
			return nil, errors.New("đơn hàng có sản phẩm Flash Sale hiện chỉ hỗ trợ phương thức thanh toán khi nhận hàng (COD)")
		}

		// Giữ chỗ từng món Flash Sale trên Redis (TTL 5 phút)
		var reservedList []flashSaleItemReserveInfo
		for _, fs := range fsReserves {
			resvID := fmt.Sprintf("FSR-MIX-%s", strings.ToUpper(uuid.New().String()[:8]))
			if s.redisClient != nil {
				resp, err := redislock.ReserveFlashSaleStock(
					ctx, s.redisClient,
					fs.campaignID, fs.productID,
					userID, fmt.Sprintf("req-%s", resvID), "mixed-checkout", resvID,
					fs.quantity,
					300,
				)
				if err != nil || (resp != nil && resp.Code != redislock.ResultReserved) {
					for _, rev := range reservedList {
						_, _ = redislock.ReleaseFlashSaleReservation(ctx, s.redisClient, rev.campaignID, rev.productID, rev.reservationID, "CANCELLED")
					}
					return nil, fmt.Errorf("FLASH_SALE_OUT_OF_STOCK: sản phẩm #%d không thể giữ chỗ trên Redis", fs.productID)
				}
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
				// 1. Tạo đơn hàng và chi tiết
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

				return nil
			})
		} else {
			txErr = s.orderRepo.CreateOrder(order)
		}

		if txErr != nil {
			for _, rev := range reservedList {
				if s.redisClient != nil {
					_, _ = redislock.ReleaseFlashSaleReservation(ctx, s.redisClient, rev.campaignID, rev.productID, rev.reservationID, "CANCELLED")
				}
			}
			logger.ErrorContext(ctx, "Lỗi tạo đơn hàng trong Transaction", "error", txErr.Error())
			return nil, fmt.Errorf("không thể tạo đơn hàng: %w", txErr)
		}

		if req.FromCart && cart != nil {
			_ = s.cartRepo.ClearCart(cart.ID)
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

	// Đơn hàng thông thường 100%
	deductedItems, err := s.deductStockWithRedis(ctx, orderItems)
	if err != nil {
		return nil, err
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

	if err := s.orderRepo.CreateOrder(order); err != nil {
		s.rollbackDeductedStock(ctx, deductedItems)
		logger.ErrorContext(ctx, "Lỗi tạo đơn hàng trong PostgreSQL, đã hoàn lại tồn kho Redis", "error", err.Error())
		return nil, errors.New("không thể tạo đơn hàng, vui lòng thử lại sau")
	}

	if req.FromCart && cart != nil {
		_ = s.cartRepo.ClearCart(cart.ID)
	}

	var eventItems []pkgKafka.OrderItemPayload
	for _, item := range order.Items {
		eventItems = append(eventItems, pkgKafka.OrderItemPayload{
			ProductID:   item.ProductID,
			ProductName: item.ProductName,
			ProductSlug: item.ProductSlug,
			Thumbnail:   item.Thumbnail,
			Price:       item.Price,
			Quantity:    item.Quantity,
			Subtotal:    item.Subtotal,
		})
	}

	if s.kafkaProducer != nil {
		_ = s.kafkaProducer.PublishOrderCreated(ctx, pkgKafka.OrderCreatedPayload{
			EventType:       pkgKafka.EventOrderCreated,
			OrderID:         order.ID,
			OrderCode:       order.OrderCode,
			UserID:          order.UserID,
			CustomerEmail:   order.CustomerEmail,
			CustomerName:    order.CustomerName,
			CustomerPhone:   order.CustomerPhone,
			ShippingAddress: order.ShippingAddress,
			TotalAmount:     order.TotalAmount,
			PaymentMethod:   order.PaymentMethod,
			Items:           eventItems,
			TraceID:         logger.GetTraceID(ctx),
			CreatedAt:       order.CreatedAt,
		})
	}

	return s.toOrderResponse(order), nil
}

// CreateFlashSaleOrderAsync tạo đơn Flash Sale siêu tốc: Trừ kho RAM -> Bắn Kafka -> Trả về 202 Accepted
func (s *orderService) CreateFlashSaleOrderAsync(ctx context.Context, userID string, req *dto.FlashSaleOrderRequest) (*dto.FlashSaleOrderAsyncResponse, error) {
	if req.ProductID == 0 || req.Quantity <= 0 {
		return nil, errors.New("thông tin sản phẩm đặt mua Flash Sale không hợp lệ")
	}
	if req.CustomerName == "" || req.CustomerEmail == "" || req.CustomerPhone == "" || req.ShippingAddress == "" {
		return nil, errors.New("vui lòng điền đầy đủ thông tin nhận hàng")
	}

	// 1. CHẶN ĐẦU TIÊN: Trừ tồn kho Atomic trên Redis kết hợp giới hạn 1 User / 1 Món
	res, err := redislock.DeductFlashSaleStockAtomic(ctx, s.redisClient, req.ProductID, userID, req.Quantity)
	if err != nil {
		logger.ErrorContext(ctx, "Lỗi trừ tồn kho Flash Sale trên Redis", "error", err.Error())
		return nil, fmt.Errorf("hệ thống đang quá tải, vui lòng thử lại sau")
	}

	switch res {
	case redislock.StockResultAlreadyPurchased:
		return nil, errors.New("bạn đã mua sản phẩm này trong đợt Flash Sale (giới hạn 1 món/người)")
	case redislock.StockResultInsufficient:
		return nil, errors.New("sản phẩm Flash Sale đã hết hàng hoặc không đủ tồn kho")
	case redislock.StockResultNotFound:
		// Chống Cache Stampede: Dùng Singleflight gọi sang Product Service nạp kho Redis
		if s.productClient != nil {
			sfPrewarmKey := fmt.Sprintf("prewarm_stock_%d", req.ProductID)
			_, _, _ = s.sfGroup.Do(sfPrewarmKey, func() (interface{}, error) {
				currentStock, sErr := redislock.GetStock(ctx, s.redisClient, req.ProductID)
				if sErr == nil && currentStock >= 0 {
					return nil, nil
				}

				prod, pErr := s.productClient.GetProduct(ctx, req.ProductID)
				if pErr == nil && prod != nil && prod.Stock > 0 {
					_ = redislock.PrewarmStock(ctx, s.redisClient, req.ProductID, prod.Stock)
				}
				return nil, nil
			})

			retryRes, _ := redislock.DeductFlashSaleStockAtomic(ctx, s.redisClient, req.ProductID, userID, req.Quantity)
			if retryRes == redislock.StockResultSuccess {
				goto winnerFound
			}
			if retryRes == redislock.StockResultAlreadyPurchased {
				return nil, errors.New("bạn đã mua sản phẩm này trong đợt Flash Sale (giới hạn 1 món/người)")
			}
		}
		return nil, errors.New("sản phẩm Flash Sale chưa sẵn sàng hoặc đã hết hàng")
	}

winnerFound:
	// 2. CHỈ DÀNH CHO NGƯỜI THẮNG: Lấy thông tin giá từ Redis Cache hoặc ProductClient
	var price float64
	cacheKey := fmt.Sprintf("cache:product:%d", req.ProductID)

	if s.redisClient != nil {
		cachedJSON, cErr := s.redisClient.Get(ctx, cacheKey).Result()
		if cErr == nil && cachedJSON != "" {
			var cachedProd dto.ProductDetailResponse
			if err := json.Unmarshal([]byte(cachedJSON), &cachedProd); err == nil {
				if cachedProd.DiscountPrice > 0 {
					price = cachedProd.DiscountPrice
				} else {
					price = cachedProd.Price
				}
			}
		}
	}

	if price == 0 && s.productClient != nil {
		sfKey := fmt.Sprintf("flash_sale_prod_%d", req.ProductID)
		v, sfErr, _ := s.sfGroup.Do(sfKey, func() (interface{}, error) {
			prod, err := s.productClient.GetProduct(ctx, req.ProductID)
			if err != nil || prod == nil {
				return float64(1000000), err
			}
			p := prod.Price
			if prod.DiscountPrice > 0 {
				p = prod.DiscountPrice
			}
			return p, nil
		})
		if sfErr == nil && v != nil {
			price = v.(float64)
		}
	}
	if price == 0 {
		price = 1000000 // Fallback an toàn
	}

	// 3. Sinh mã token theo dõi tiến độ
	orderToken := fmt.Sprintf("FSO-%s", strings.ToUpper(uuid.New().String()))

	// 4. Lưu trạng thái PENDING vào Redis RAM (TTL 15 phút)
	initialStatus := dto.FlashSaleStatusResponse{
		OrderToken: orderToken,
		Status:     "PENDING",
		UpdatedAt:  time.Now().Format(time.RFC3339),
	}
	statusJSON, _ := json.Marshal(initialStatus)
	_ = redislock.SetFlashSaleOrderStatus(ctx, s.redisClient, orderToken, string(statusJSON), 15*time.Minute)

	// 5. Bắn tác vụ vào Apache Kafka topic flashsale.orders để cắt đỉnh tải
	taskPayload := pkgKafka.FlashSaleOrderTaskPayload{
		OrderToken:      orderToken,
		UserID:          userID,
		ProductID:       req.ProductID,
		Quantity:        req.Quantity,
		Price:           price,
		CustomerName:    req.CustomerName,
		CustomerEmail:   req.CustomerEmail,
		CustomerPhone:   req.CustomerPhone,
		ShippingAddress: req.ShippingAddress,
		PaymentMethod:   req.PaymentMethod,
		TraceID:         logger.GetTraceID(ctx),
		CreatedAt:       time.Now(),
	}

	if s.kafkaProducer != nil {
		err = s.kafkaProducer.PublishFlashSaleOrderTask(ctx, taskPayload)
		if err != nil {
			// Rollback kho nếu không thể bắn vào Kafka
			_ = redislock.RevertFlashSaleStockAtomic(ctx, s.redisClient, req.ProductID, userID, req.Quantity)
			return nil, fmt.Errorf("không thể tiếp nhận đơn Flash Sale vào hàng đợi: %w", err)
		}
	}

	logger.InfoContext(ctx, "⚡ [FLASH SALE] Tiếp nhận đơn hàng thành công vào Kafka",
		"order_token", orderToken,
		"user_id", userID,
		"product_id", req.ProductID,
	)

	return &dto.FlashSaleOrderAsyncResponse{
		OrderToken:     orderToken,
		Status:         "PENDING",
		Message:        "Đơn hàng Flash Sale đang được xử lý trong hàng đợi Kafka",
		CheckStatusURL: fmt.Sprintf("/orders/flash-sale/status/%s", orderToken),
	}, nil
}

// GetFlashSaleOrderStatus kiểm tra trạng thái đơn hàng trực tiếp từ RAM Redis (Zero DB Hit)
func (s *orderService) GetFlashSaleOrderStatus(ctx context.Context, token string) (*dto.FlashSaleStatusResponse, error) {
	val, err := redislock.GetFlashSaleOrderStatus(ctx, s.redisClient, token)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, errors.New("mã đơn hàng Flash Sale không tồn tại hoặc đã hết hạn")
		}
		return nil, fmt.Errorf("lỗi tra cứu trạng thái: %w", err)
	}

	var resp dto.FlashSaleStatusResponse
	if err := json.Unmarshal([]byte(val), &resp); err != nil {
		return nil, fmt.Errorf("lỗi đọc dữ liệu trạng thái: %w", err)
	}

	return &resp, nil
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

func (s *orderService) deductStockWithRedis(ctx context.Context, items []domain.OrderItem) ([]domain.OrderItem, error) {
	if s.redisClient == nil {
		return nil, nil
	}

	var deductedItems []domain.OrderItem

	for _, item := range items {
		result, err := redislock.DeductStockAtomic(ctx, s.redisClient, item.ProductID, item.Quantity)
		if err != nil {
			s.rollbackDeductedStock(ctx, deductedItems)
			return nil, fmt.Errorf("lỗi kết nối Redis khi kiểm tra tồn kho: %w", err)
		}

		switch result {
		case redislock.StockResultSuccess:
			deductedItems = append(deductedItems, item)

		case redislock.StockResultInsufficient:
			s.rollbackDeductedStock(ctx, deductedItems)
			return nil, fmt.Errorf("sản phẩm '%s' đã hết hàng hoặc không đủ tồn kho", item.ProductName)

		case redislock.StockResultNotFound:
			if s.productClient != nil {
				prod, err := s.productClient.GetProduct(ctx, item.ProductID)
				if err != nil || prod == nil {
					s.rollbackDeductedStock(ctx, deductedItems)
					return nil, fmt.Errorf("sản phẩm #%d không tồn tại", item.ProductID)
				}

				if prod.Stock < item.Quantity {
					_ = redislock.SetStock(ctx, s.redisClient, item.ProductID, prod.Stock)
					s.rollbackDeductedStock(ctx, deductedItems)
					return nil, fmt.Errorf("sản phẩm '%s' đã hết hàng trong kho", prod.Name)
				}

				_ = redislock.SetStock(ctx, s.redisClient, item.ProductID, prod.Stock)
				retryRes, retryErr := redislock.DeductStockAtomic(ctx, s.redisClient, item.ProductID, item.Quantity)
				if retryErr != nil || retryRes != redislock.StockResultSuccess {
					s.rollbackDeductedStock(ctx, deductedItems)
					return nil, fmt.Errorf("sản phẩm '%s' đã hết hàng hoặc không đủ tồn kho", item.ProductName)
				}

				deductedItems = append(deductedItems, item)
			}
		}
	}

	return deductedItems, nil
}

func (s *orderService) rollbackDeductedStock(ctx context.Context, items []domain.OrderItem) {
	if s.redisClient == nil || len(items) == 0 {
		return
	}
	for _, item := range items {
		_ = redislock.RevertStockAtomic(ctx, s.redisClient, item.ProductID, item.Quantity)
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
