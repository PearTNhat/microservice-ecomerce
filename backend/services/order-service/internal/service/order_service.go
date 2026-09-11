package service

import (
	"context"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/redislock"
	"ecomerce-service/services/order-service/internal/client"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/dto"
	pkgKafka "ecomerce-service/pkg/kafka"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type OrderService interface {
	CreateOrder(ctx context.Context, userID string, req *dto.CreateOrderRequest) (*dto.OrderResponse, error)
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
	productClient client.ProductClient
	redisClient   *redis.Client
	kafkaProducer pkgKafka.OrderKafkaProducer
	sfGroup       singleflight.Group
}

func NewOrderService(
	orderRepo domain.OrderRepository,
	cartRepo domain.CartRepository,
	productClient client.ProductClient,
	rClient *redis.Client,
	producer pkgKafka.OrderKafkaProducer,
) OrderService {
	return &orderService{
		orderRepo:     orderRepo,
		cartRepo:      cartRepo,
		productClient: productClient,
		redisClient:   rClient,
		kafkaProducer: producer,
	}
}

func (s *orderService) CreateOrder(ctx context.Context, userID string, req *dto.CreateOrderRequest) (*dto.OrderResponse, error) {
	if req.CustomerName == "" || req.CustomerEmail == "" || req.CustomerPhone == "" || req.ShippingAddress == "" {
		return nil, errors.New("vui lòng điền đầy đủ thông tin nhận hàng (Họ tên, Email, Số điện thoại, Địa chỉ)")
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

	// Kiểm tra và giữ tạm tồn kho trên Redis RAM
	deductedItems, err := s.deductStockWithRedis(ctx, orderItems)
	if err != nil {
		return nil, err
	}

	orderCode := fmt.Sprintf("ORD-%s", strings.ToUpper(uuid.New().String()[:8]))

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

	// Lưu đơn hàng vào Database riêng ecom_order_db
	if err := s.orderRepo.CreateOrder(order); err != nil {
		s.rollbackDeductedStock(ctx, deductedItems)
		logger.ErrorContext(ctx, "Lỗi tạo đơn hàng trong PostgreSQL, đã hoàn lại tồn kho Redis", "error", err.Error())
		return nil, errors.New("không thể tạo đơn hàng, vui lòng thử lại sau")
	}

	if req.FromCart && cart != nil {
		_ = s.cartRepo.ClearCart(cart.ID)
	}

	// Chuẩn bị payload event bắn vào Apache Kafka
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

	// SAGA CHOREOGRAPHY: Bắn event order.created vào Apache Kafka
	// Product Service sẽ lắng nghe event này để tự trừ tồn kho trong Database của nó!
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
