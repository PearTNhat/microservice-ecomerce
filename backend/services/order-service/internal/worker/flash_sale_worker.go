package worker

import (
	"context"
	"ecomerce-service/pkg/config"
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

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type FlashSaleWorker struct {
	reader        *kafka.Reader
	orderRepo     domain.OrderRepository
	productClient client.ProductClient
	redisClient   *redis.Client
	kafkaProducer pkgKafka.OrderKafkaProducer
	appConfig     config.AppConfig
}

func NewFlashSaleWorker(
	brokers []string,
	cfg config.AppConfig,
	orderRepo domain.OrderRepository,
	productClient client.ProductClient,
	redisClient *redis.Client,
	producer pkgKafka.OrderKafkaProducer,
) *FlashSaleWorker {
	if len(brokers) == 0 {
		return nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          pkgKafka.TopicFlashSaleOrders,
		GroupID:        pkgKafka.ConsumerGroupFlashSale,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})

	return &FlashSaleWorker{
		reader:        reader,
		orderRepo:     orderRepo,
		productClient: productClient,
		redisClient:   redisClient,
		kafkaProducer: producer,
		appConfig:     cfg,
	}
}

// Start lắng nghe và xử lý các tác vụ tạo đơn Flash Sale từ Kafka (Cắt đỉnh tải)
func (w *FlashSaleWorker) Start(ctx context.Context) {
	if w.reader == nil {
		return
	}

	logger.Info("⚡ [KAFKA FLASH SALE WORKER] Đang lắng nghe topic flashsale.orders...",
		"group_id", pkgKafka.ConsumerGroupFlashSale,
	)

	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("🛑 FlashSaleWorker nhận tín hiệu dừng")
				_ = w.reader.Close()
				return
			default:
				m, err := w.reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					time.Sleep(500 * time.Millisecond)
					continue
				}

				w.processFlashSaleOrder(ctx, m)
				_ = w.reader.CommitMessages(ctx, m)
			}
		}
	}()
}

func (w *FlashSaleWorker) processFlashSaleOrder(ctx context.Context, m kafka.Message) {
	var task pkgKafka.FlashSaleOrderTaskPayload
	if err := json.Unmarshal(m.Value, &task); err != nil {
		logger.Error("❌ FlashSaleWorker: Lỗi deserialize JSON payload", "error", err.Error())
		if w.kafkaProducer != nil {
			_ = w.kafkaProducer.PublishDeadLetter(ctx, m.Topic, string(m.Key), m.Value, err.Error(), "")
		}
		return
	}

	traceID := task.TraceID
	if traceID == "" {
		traceID = "fs-worker-" + task.OrderToken
	}
	reqCtx := logger.SetTraceID(ctx, traceID)

	logger.InfoContext(reqCtx, "⚡ [FLASH SALE WORKER] Bắt đầu ghi đơn hàng vào PostgreSQL ecom_order_db",
		"order_token", task.OrderToken,
		"user_id", task.UserID,
		"product_id", task.ProductID,
	)

	// Lấy thông tin sản phẩm qua ProductClient (Không đụng trực tiếp Product DB)
	var name, slug, thumbnail string
	if w.productClient != nil {
		prod, err := w.productClient.GetProduct(reqCtx, task.ProductID)
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

	// Ghi đơn hàng vào PostgreSQL ecom_order_db
	var err error
	if strings.Contains(task.CustomerName, "[TEST_FAIL]") {
		err = errors.New("mô phỏng sự cố Database: hệ thống đã kích hoạt Rollback tự động hoàn trả tồn kho")
	} else {
		err = w.orderRepo.CreateOrder(order)
	}

	if err != nil {
		logger.ErrorContext(reqCtx, "❌ FlashSaleWorker: Ghi Database thất bại, hoàn lại tồn kho Redis",
			"order_token", task.OrderToken,
			"error", err.Error(),
		)

		// Hoàn lại kho trên Redis
		_ = redislock.RevertFlashSaleStockAtomic(reqCtx, w.redisClient, task.ProductID, task.UserID, task.Quantity)

		// Cập nhật trạng thái FAILED vào Redis
		failedStatus := dto.FlashSaleStatusResponse{
			OrderToken: task.OrderToken,
			Status:     "FAILED",
			Reason:     "Lỗi lưu trữ đơn hàng: " + err.Error(),
			UpdatedAt:  time.Now().Format(time.RFC3339),
		}
		failedJSON, _ := json.Marshal(failedStatus)
		_ = redislock.SetFlashSaleOrderStatus(reqCtx, w.redisClient, task.OrderToken, string(failedJSON), 15*time.Minute)

		if w.kafkaProducer != nil {
			_ = w.kafkaProducer.PublishDeadLetter(reqCtx, m.Topic, task.OrderToken, m.Value, err.Error(), traceID)
		}
		return
	}

	// Ghi DB thành công -> Cập nhật trạng thái SUCCESS vào Redis
	successStatus := dto.FlashSaleStatusResponse{
		OrderToken: task.OrderToken,
		Status:     "SUCCESS",
		OrderID:    order.ID,
		OrderCode:  order.OrderCode,
		UpdatedAt:  time.Now().Format(time.RFC3339),
	}
	successJSON, _ := json.Marshal(successStatus)
	_ = redislock.SetFlashSaleOrderStatus(reqCtx, w.redisClient, task.OrderToken, string(successJSON), 15*time.Minute)

	logger.InfoContext(reqCtx, "✅ [FLASH SALE WORKER] Lưu đơn hàng thành công, bắn event order.created sang Kafka",
		"order_id", order.ID,
		"order_code", order.OrderCode,
	)

	// Bắn event order.created sang Kafka để kích hoạt ProductStockWorker trừ kho DB và EmailWorker gửi mail
	if w.kafkaProducer != nil {
		_ = w.kafkaProducer.PublishOrderCreated(reqCtx, pkgKafka.OrderCreatedPayload{
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
			Items: []pkgKafka.OrderItemPayload{
				{
					ProductID:   orderItem.ProductID,
					ProductName: orderItem.ProductName,
					Price:       orderItem.Price,
					Quantity:    orderItem.Quantity,
					Subtotal:    orderItem.Subtotal,
				},
			},
			TraceID:   traceID,
			CreatedAt: order.CreatedAt,
		})
	}
}

func (w *FlashSaleWorker) Close() error {
	if w.reader != nil {
		return w.reader.Close()
	}
	return nil
}
