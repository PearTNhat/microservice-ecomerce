package worker

import (
	"context"
	"ecomerce-service/config"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/dto"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/rabbitmq"
	"ecomerce-service/pkg/redislock"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type FlashSaleWorker struct {
	amqpURL          string
	conn             *amqp.Connection
	channel          *amqp.Channel
	orderRepo        domain.OrderRepository
	productRepo      domain.ProductRepository
	redisClient      *redis.Client
	rabbitMQProducer rabbitmq.OrderEventProducer
	appConfig        config.AppConfig
}

func NewFlashSaleWorker(
	cfg config.AppConfig,
	orderRepo domain.OrderRepository,
	productRepo domain.ProductRepository,
	redisClient *redis.Client,
	producer rabbitmq.OrderEventProducer,
) *FlashSaleWorker {
	if cfg.RabbitMQURL == "" {
		return nil
	}

	return &FlashSaleWorker{
		amqpURL:          cfg.RabbitMQURL,
		orderRepo:        orderRepo,
		productRepo:      productRepo,
		redisClient:      redisClient,
		rabbitMQProducer: producer,
		appConfig:        cfg,
	}
}

// Start lắng nghe và xử lý các tác vụ tạo đơn Flash Sale từ RabbitMQ (Cắt đỉnh tải)
func (w *FlashSaleWorker) Start(ctx context.Context) error {
	conn, err := amqp.Dial(w.amqpURL)
	if err != nil {
		logger.Warn("⚠️ FlashSaleWorker: Không thể kết nối RabbitMQ (Worker tạm hoãn)", "error", err.Error())
		return err
	}
	w.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		logger.Error("❌ FlashSaleWorker: Không thể mở channel", "error", err.Error())
		conn.Close()
		return err
	}
	w.channel = ch

	// Giới hạn prefetch 20 tin nhắn để kiểm soát tốc độ ghi vào PostgreSQL
	_ = ch.Qos(20, 0, false)

	msgs, err := ch.Consume(
		rabbitmq.QueueFlashSaleOrders,
		"flash-sale-order-consumer",
		false, // auto-ack = false (manual ack sau khi ghi DB thành công)
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Error("❌ FlashSaleWorker: Không thể bắt đầu consume", "error", err.Error())
		return err
	}

	logger.Info("⚡ [RABBITMQ FLASH SALE WORKER] Đang lắng nghe hàng đợi tạo đơn...", "queue", rabbitmq.QueueFlashSaleOrders)

	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("🛑 FlashSaleWorker: Nhận tín hiệu dừng")
				w.Close()
				return
			case d, ok := <-msgs:
				if !ok {
					logger.Warn("⚠️ FlashSaleWorker: Channel đóng, dừng nhận tin")
					return
				}
				w.processFlashSaleOrder(ctx, d)
			}
		}
	}()

	return nil
}

func (w *FlashSaleWorker) processFlashSaleOrder(ctx context.Context, d amqp.Delivery) {
	var task rabbitmq.FlashSaleOrderTaskPayload
	if err := json.Unmarshal(d.Body, &task); err != nil {
		logger.Error("❌ FlashSaleWorker: Lỗi deserialize JSON payload", "error", err.Error())
		_ = d.Nack(false, false) // Reject and send to DLQ
		return
	}

	traceID := task.TraceID
	if traceID == "" {
		traceID = "fs-worker-" + task.OrderToken
	}
	reqCtx := logger.SetTraceID(ctx, traceID)

	logger.InfoContext(reqCtx, "⚡ [FLASH SALE WORKER] Bắt đầu ghi đơn hàng vào PostgreSQL",
		"order_token", task.OrderToken,
		"user_id", task.UserID,
		"product_id", task.ProductID,
	)

	// Lấy thông tin sản phẩm
	var name, slug, thumbnail string
	if w.productRepo != nil {
		prod, err := w.productRepo.FindById(task.ProductID)
		if err == nil && prod != nil {
			name = prod.Name
			slug = prod.Slug
			thumbnail = prod.Thumbnail
		}
	}
	if name == "" {
		name = fmt.Sprintf("Sản phẩm Flash Sale #%d", task.ProductID)
	}

	subtotal := task.Price * float64(task.Quantity)
	orderCode := fmt.Sprintf("ORD-FS-%s", strings.ToUpper(task.OrderToken[len(task.OrderToken)-8:]))

	orderItem := domain.OrderItem{
		ProductID:   task.ProductID,
		ProductName: name,
		ProductSlug: slug,
		Thumbnail:   thumbnail,
		Price:       task.Price,
		Quantity:    task.Quantity,
		Subtotal:    subtotal,
	}

	order := &domain.Order{
		OrderCode:       orderCode,
		UserID:          task.UserID,
		CustomerName:    task.CustomerName,
		CustomerEmail:   task.CustomerEmail,
		CustomerPhone:   task.CustomerPhone,
		ShippingAddress: task.ShippingAddress,
		Note:            fmt.Sprintf("Đơn hàng Flash Sale (Token: %s)", task.OrderToken),
		PaymentMethod:   task.PaymentMethod,
		PaymentStatus:   domain.PaymentStatusPending,
		OrderStatus:     domain.OrderStatusPending,
		TotalAmount:     subtotal,
		Items:           []domain.OrderItem{orderItem},
	}

	// 1. Trừ tồn kho trực tiếp trong PostgreSQL (Atomic SQL Decrement chống Lost Update)
	if w.productRepo != nil {
		if err := w.productRepo.DeductStock(task.ProductID, task.Quantity); err != nil {
			logger.WarnContext(reqCtx, "⚠️ FlashSaleWorker: Không thể trừ tồn kho trong PostgreSQL", "error", err.Error())
		}
	}

	// 2. Ghi đơn hàng vào PostgreSQL (Hỗ trợ mô phỏng lỗi để test luồng Rollback từ Frontend)
	var err error
	if strings.Contains(task.CustomerName, "[TEST_FAIL]") {
		err = errors.New("mô phỏng sự cố Database: hệ thống đã kích hoạt Rollback tự động hoàn trả tồn kho và mở khóa mua cho bạn")
	} else {
		err = w.orderRepo.CreateOrder(order)
	}

	if err != nil {
		logger.ErrorContext(reqCtx, "❌ FlashSaleWorker: Ghi Database thất bại, tiến hành hoàn lại tồn kho Redis và PostgreSQL",
			"order_token", task.OrderToken,
			"error", err.Error(),
		)

		// Hoàn lại tồn kho trong PostgreSQL
		if w.productRepo != nil {
			_ = w.productRepo.RevertStock(task.ProductID, task.Quantity)
		}

		// Hoàn lại kho trên Redis và xóa user khỏi danh sách đã mua
		_ = redislock.RevertFlashSaleStockAtomic(reqCtx, w.redisClient, task.ProductID, task.UserID, task.Quantity)

		// Cập nhật trạng thái FAILED vào Redis để Client polling biết kết quả
		failedStatus := dto.FlashSaleStatusResponse{
			OrderToken: task.OrderToken,
			Status:     "FAILED",
			Reason:     "Lỗi lưu trữ đơn hàng: " + err.Error(),
			UpdatedAt:  time.Now().Format(time.RFC3339),
		}
		failedJSON, _ := json.Marshal(failedStatus)
		_ = redislock.SetFlashSaleOrderStatus(reqCtx, w.redisClient, task.OrderToken, string(failedJSON), 15*time.Minute)

		_ = d.Nack(false, false)
		return
	}

	// 3. Xóa cache chi tiết sản phẩm trên Redis để người xem luôn thấy số tồn kho mới nhất
	if w.redisClient != nil {
		_ = w.redisClient.Del(reqCtx, fmt.Sprintf("cache:product:%d", task.ProductID))
	}

	// Ghi DB thành công -> 1. Cập nhật trạng thái SUCCESS vào Redis
	successStatus := dto.FlashSaleStatusResponse{
		OrderToken: task.OrderToken,
		Status:     "SUCCESS",
		OrderID:    order.ID,
		OrderCode:  order.OrderCode,
		UpdatedAt:  time.Now().Format(time.RFC3339),
	}
	successJSON, _ := json.Marshal(successStatus)
	_ = redislock.SetFlashSaleOrderStatus(reqCtx, w.redisClient, task.OrderToken, string(successJSON), 15*time.Minute)

	// 2. Bắn sự kiện order.created để gửi email xác nhận
	if w.rabbitMQProducer != nil {
		_ = w.rabbitMQProducer.PublishOrderCreated(reqCtx, rabbitmq.OrderCreatedPayload{
			OrderID:         order.ID,
			UserID:          order.UserID,
			CustomerEmail:   order.CustomerEmail,
			CustomerName:    order.CustomerName,
			CustomerPhone:   order.CustomerPhone,
			ShippingAddress: order.ShippingAddress,
			TotalAmount:     order.TotalAmount,
			PaymentMethod:   order.PaymentMethod,
			Items: []rabbitmq.OrderItemEventPayload{{
				ProductID:   task.ProductID,
				ProductName: name,
				Price:       task.Price,
				Quantity:    task.Quantity,
				Subtotal:    subtotal,
			}},
			TraceID:   traceID,
			CreatedAt: order.CreatedAt,
		})
	}

	// 3. Ack hoàn tất tin nhắn
	_ = d.Ack(false)

	logger.InfoContext(reqCtx, "🎉 [FLASH SALE WORKER] Đã tạo thành công đơn hàng Flash Sale",
		"order_token", task.OrderToken,
		"order_id", order.ID,
		"order_code", order.OrderCode,
	)
}

func (w *FlashSaleWorker) Close() {
	if w.channel != nil {
		_ = w.channel.Close()
	}
	if w.conn != nil {
		_ = w.conn.Close()
	}
}
