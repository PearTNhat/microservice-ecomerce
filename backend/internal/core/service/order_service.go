package service

import (
	"context"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/dto"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/rabbitmq"
	"ecomerce-service/pkg/redislock"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type OrderService interface {
	CreateOrder(ctx context.Context, userID string, req *dto.CreateOrderRequest) (*dto.OrderResponse, error)
	GetOrderByID(ctx context.Context, orderID uint, userID string, role string) (*dto.OrderResponse, error)
	GetUserOrders(ctx context.Context, userID string, page int, limit int) (*dto.OrderListResponse, error)
	UpdateOrderStatus(ctx context.Context, orderID uint, req *dto.UpdateOrderStatusRequest) error
}

type orderService struct {
	orderRepo        domain.OrderRepository
	cartRepo         domain.CartRepository
	productRepo      domain.ProductRepository
	redisClient      *redis.Client
	rabbitMQProducer rabbitmq.OrderEventProducer
}

func NewOrderService(
	orderRepo domain.OrderRepository,
	cartRepo domain.CartRepository,
	productRepo domain.ProductRepository,
	rClient *redis.Client,
	producer rabbitmq.OrderEventProducer,
) OrderService {
	return &orderService{
		orderRepo:        orderRepo,
		cartRepo:         cartRepo,
		productRepo:      productRepo,
		redisClient:      rClient,
		rabbitMQProducer: producer,
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

			if s.productRepo != nil {
				prod, err := s.productRepo.FindById(itemReq.ProductID)
				if err != nil || prod == nil {
					return nil, fmt.Errorf("sản phẩm #%d không tồn tại", itemReq.ProductID)
				}
				name = prod.Name
				slug = prod.Slug
				thumbnail = prod.Thumbnail
				price = prod.Price
				if prod.DiscountPrice > 0 {
					price = prod.DiscountPrice
				}
			} else {
				name = fmt.Sprintf("Sản phẩm #%d", itemReq.ProductID)
				price = 100000
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

	// 1. Kiểm tra và Trừ kho Atomic bằng Redis Lua Script (Flash Sale Lock) với cơ chế Rollback & Fallback DB
	deductedItems, err := s.deductStockWithRollback(ctx, orderItems)
	if err != nil {
		return nil, err
	}

	// 2. Tạo mã đơn hàng duy nhất
	orderCode := fmt.Sprintf("ORD-%d-%s", time.Now().Unix(), strings.ToUpper(uuid.New().String()[:6]))

	paymentMethod := req.PaymentMethod
	if paymentMethod == "" {
		paymentMethod = domain.PaymentMethodCOD
	}

	order := domain.Order{
		OrderCode:       orderCode,
		UserID:          userID,
		CustomerName:    req.CustomerName,
		CustomerEmail:   req.CustomerEmail,
		CustomerPhone:   req.CustomerPhone,
		ShippingAddress: req.ShippingAddress,
		Note:            req.Note,
		PaymentMethod:   paymentMethod,
		PaymentStatus:   domain.PaymentStatusPending,
		OrderStatus:     domain.OrderStatusPending,
		TotalAmount:     totalAmount,
		Items:           orderItems,
	}

	// 3. Lưu đơn hàng vào PostgreSQL
	if err := s.orderRepo.CreateOrder(&order); err != nil {
		s.rollbackDeductedStock(ctx, deductedItems)
		logger.ErrorContext(ctx, "❌ Lỗi tạo đơn hàng trong PostgreSQL", "error", err.Error())
		return nil, fmt.Errorf("không thể tạo đơn hàng: %w", err)
	}

	// 4. Nếu đặt từ giỏ hàng -> Xóa sạch giỏ hàng
	if req.FromCart && cart != nil && s.cartRepo != nil {
		_ = s.cartRepo.ClearCart(cart.ID)
	}

	// 5. Bắn sự kiện order.created vào RabbitMQ (Bất đồng bộ)
	if s.rabbitMQProducer != nil {
		eventItems := make([]rabbitmq.OrderItemEventPayload, len(orderItems))
		for i, item := range orderItems {
			eventItems[i] = rabbitmq.OrderItemEventPayload{
				ProductID:   item.ProductID,
				ProductName: item.ProductName,
				Price:       item.Price,
				Quantity:    item.Quantity,
				Subtotal:    item.Subtotal,
			}
		}

		payload := rabbitmq.OrderCreatedPayload{
			OrderID:         order.ID,
			UserID:          userID,
			CustomerEmail:   order.CustomerEmail,
			CustomerName:    order.CustomerName,
			CustomerPhone:   order.CustomerPhone,
			ShippingAddress: order.ShippingAddress,
			TotalAmount:     order.TotalAmount,
			PaymentMethod:   order.PaymentMethod,
			Items:           eventItems,
			TraceID:         logger.GetTraceID(ctx),
			CreatedAt:       order.CreatedAt,
		}

		go func() {
			_ = s.rabbitMQProducer.PublishOrderCreated(context.Background(), payload)
		}()
	}

	logger.InfoContext(ctx, "🎉 Đã tạo đơn hàng thành công",
		"order_code", order.OrderCode,
		"user_id", userID,
		"total_amount", order.TotalAmount,
	)

	return s.mapOrderToResponse(&order), nil
}

func (s *orderService) GetOrderByID(ctx context.Context, orderID uint, userID string, role string) (*dto.OrderResponse, error) {
	order, err := s.orderRepo.FindByID(orderID)
	if err != nil {
		return nil, err
	}
	if order == nil {
		return nil, errors.New("đơn hàng không tồn tại")
	}

	// Chỉ Admin / Staff hoặc chính chủ đơn hàng mới được xem
	if role != domain.RoleAdmin && role != domain.RoleTechnician && order.UserID != userID {
		return nil, errors.New("bạn không có quyền xem đơn hàng này")
	}

	return s.mapOrderToResponse(order), nil
}

func (s *orderService) GetUserOrders(ctx context.Context, userID string, page int, limit int) (*dto.OrderListResponse, error) {
	orders, total, err := s.orderRepo.FindByUserID(userID, page, limit)
	if err != nil {
		return nil, err
	}

	var resp []*dto.OrderResponse
	for _, o := range orders {
		resp = append(resp, s.mapOrderToResponse(o))
	}

	if limit <= 0 {
		limit = 10
	}
	totalPages := int((total + int64(limit) - 1) / int64(limit))

	return &dto.OrderListResponse{
		Orders:     resp,
		Total:      total,
		Page:       page,
		Limit:      limit,
		TotalPages: totalPages,
	}, nil
}

func (s *orderService) UpdateOrderStatus(ctx context.Context, orderID uint, req *dto.UpdateOrderStatusRequest) error {
	order, err := s.orderRepo.FindByID(orderID)
	if err != nil || order == nil {
		return errors.New("đơn hàng không tồn tại")
	}

	return s.orderRepo.UpdateStatus(orderID, req.Status)
}

func (s *orderService) mapOrderToResponse(order *domain.Order) *dto.OrderResponse {
	if order == nil {
		return nil
	}

	var items []dto.OrderItemResponse
	for _, item := range order.Items {
		items = append(items, dto.OrderItemResponse{
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
		Items:           items,
		CreatedAt:       order.CreatedAt.Format(time.RFC3339),
	}
}

// deductStockWithRollback trừ kho trên Redis có hỗ trợ Lazy Loading từ DB và tự động rollback nếu có item thất bại
func (s *orderService) deductStockWithRollback(ctx context.Context, items []domain.OrderItem) ([]domain.OrderItem, error) {
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
			// Redis chưa có key (Cache Miss) -> Fallback lấy từ DB nạp lên Redis (Lazy Loading / Pre-warming)
			if s.productRepo != nil {
				prod, err := s.productRepo.FindById(item.ProductID)
				if err != nil || prod == nil {
					s.rollbackDeductedStock(ctx, deductedItems)
					return nil, fmt.Errorf("sản phẩm #%d không tồn tại", item.ProductID)
				}

				if prod.Stock < item.Quantity {
					_ = redislock.SetStock(ctx, s.redisClient, item.ProductID, prod.Stock)
					s.rollbackDeductedStock(ctx, deductedItems)
					return nil, fmt.Errorf("sản phẩm '%s' đã hết hàng trong kho", prod.Name)
				}

				// Nạp tồn kho từ DB lên Redis và thử trừ lại
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

// rollbackDeductedStock hoàn lại số lượng tồn kho trên Redis nếu có lỗi phát sinh giữa chừng
func (s *orderService) rollbackDeductedStock(ctx context.Context, items []domain.OrderItem) {
	if s.redisClient == nil || len(items) == 0 {
		return
	}
	for _, item := range items {
		_ = redislock.RevertStockAtomic(ctx, s.redisClient, item.ProductID, item.Quantity)
	}
}

