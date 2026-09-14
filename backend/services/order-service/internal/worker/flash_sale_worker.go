package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"ecomerce-service/pkg/config"
	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/redislock"
	"ecomerce-service/services/order-service/internal/client"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/dto"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

type FlashSaleWorker struct {
	reader             *kafka.Reader
	db                 *gorm.DB
	orderRepo          domain.OrderRepository
	fsRepo             domain.FlashSaleRepository
	processedEventRepo domain.ProcessedEventRepository
	productClient      client.ProductClient
	redisClient        *redis.Client
	kafkaProducer      pkgKafka.OrderKafkaProducer
	appConfig          config.AppConfig
}

func NewFlashSaleWorker(
	brokers []string,
	cfg config.AppConfig,
	db *gorm.DB,
	orderRepo domain.OrderRepository,
	fsRepo domain.FlashSaleRepository,
	processedEventRepo domain.ProcessedEventRepository,
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
		reader:             reader,
		db:                 db,
		orderRepo:          orderRepo,
		fsRepo:             fsRepo,
		processedEventRepo: processedEventRepo,
		productClient:      productClient,
		redisClient:        redisClient,
		kafkaProducer:      producer,
		appConfig:          cfg,
	}
}

func (w *FlashSaleWorker) Start(ctx context.Context) {
	if w.reader == nil {
		return
	}

	logger.Info("⚡ [KAFKA FLASH SALE WORKER] Đang lắng nghe topic flashsale.orders với Idempotent Consumer...",
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

type flashSaleTaskPayload struct {
	ReservationID   string  `json:"reservation_id"`
	RequestID       string  `json:"request_id"`
	CampaignID      uint    `json:"campaign_id"`
	ProductID       uint    `json:"product_id"`
	UserID          string  `json:"user_id"`
	Quantity        int     `json:"quantity"`
	UnitPrice       float64 `json:"unit_price"`
	TotalAmount     float64 `json:"total_amount"`
	PaymentMethod   string  `json:"payment_method"`
	CustomerName    string  `json:"customer_name"`
	CustomerEmail   string  `json:"customer_email"`
	CustomerPhone   string  `json:"customer_phone"`
	ShippingAddress string  `json:"shipping_address"`
	TraceID         string  `json:"trace_id"`
}

func (w *FlashSaleWorker) processFlashSaleOrder(ctx context.Context, m kafka.Message) {
	var task flashSaleTaskPayload
	if err := json.Unmarshal(m.Value, &task); err != nil {
		logger.Error("❌ FlashSaleWorker: Lỗi deserialize JSON payload", "error", err.Error())
		return
	}

	traceID := task.TraceID
	if traceID == "" {
		traceID = "fs-worker-" + task.ReservationID
	}
	reqCtx := logger.SetTraceID(ctx, traceID)

	// 1. Idempotency Check với processed_events
	eventID := fmt.Sprintf("fs-order-%s", task.ReservationID)
	if w.processedEventRepo != nil {
		processed, err := w.processedEventRepo.HasProcessed("flash-sale-worker", eventID)
		if err == nil && processed {
			logger.InfoContext(reqCtx, "🔁 [IDEMPOTENT] Sự kiện Flash Sale đã được xử lý trước đó, bỏ qua",
				"reservation_id", task.ReservationID,
			)
			return
		}
	}

	// 2. Kiểm tra trạng thái hiện tại của Reservation trong DB
	resv, err := w.fsRepo.FindReservationByID(task.ReservationID)
	if err != nil {
		logger.ErrorContext(reqCtx, "❌ FlashSaleWorker: Không tìm thấy reservation trong DB",
			"reservation_id", task.ReservationID,
			"error", err.Error(),
		)
		return
	}

	if resv.Status == domain.ReservationStatusConfirmed {
		logger.InfoContext(reqCtx, "Đơn Flash Sale đã được CONFIRMED trước đó", "reservation_id", task.ReservationID)
		return
	}
	if resv.Status == domain.ReservationStatusCancelled || resv.Status == domain.ReservationStatusExpired {
		logger.WarnContext(reqCtx, "Reservation đã bị CANCELLED/EXPIRED, hủy xử lý", "reservation_id", task.ReservationID)
		return
	}

	// 3. Lấy thông tin sản phẩm qua ProductClient
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

	orderCode := fmt.Sprintf("ORD-FS-%s", strings.ToUpper(task.ReservationID[len(task.ReservationID)-8:]))
	orderItem := domain.OrderItem{
		ProductID:   task.ProductID,
		ProductName: name,
		ProductSlug: slug,
		Thumbnail:   thumbnail,
		Price:       task.UnitPrice,
		Quantity:    task.Quantity,
		Subtotal:    task.TotalAmount,
	}

	order := &domain.Order{
		OrderCode:       orderCode,
		UserID:          task.UserID,
		CustomerName:    task.CustomerName,
		CustomerEmail:   task.CustomerEmail,
		CustomerPhone:   task.CustomerPhone,
		ShippingAddress: task.ShippingAddress,
		Note:            fmt.Sprintf("Đơn hàng Flash Sale (Mã giữ chỗ: %s)", task.ReservationID),
		PaymentMethod:   task.PaymentMethod,
		PaymentStatus:   domain.PaymentStatusPending,
		OrderStatus:     domain.OrderStatusPending,
		TotalAmount:     task.TotalAmount,
		Items:           []domain.OrderItem{orderItem},
	}

	// 4. THỰC THI GIAO DỊCH DATABASE BẢO ĐẢM TÍNH BỀN VỮNG (DURABLE ATOMIC TRANSACTION)
	dbErr := w.db.Transaction(func(tx *gorm.DB) error {
		// a. Đánh dấu đã xử lý vào processed_events
		if w.processedEventRepo != nil {
			if err := w.processedEventRepo.MarkProcessed(tx, "flash-sale-worker", eventID); err != nil {
				return err
			}
		}

		// b. Tạo Order và OrderItem
		if err := tx.Create(order).Error; err != nil {
			return err
		}

		// c. Confirm Reservation trong DB (CAS chuyển RESERVED -> CONFIRMED, chuyển reserved_stock -> sold_stock)
		if err := w.fsRepo.ConfirmReservationDB(tx, task.ReservationID, order.ID); err != nil {
			return err
		}

		return nil
	})

	if dbErr != nil {
		logger.ErrorContext(reqCtx, "❌ FlashSaleWorker: Lưu đơn hàng thất bại",
			"reservation_id", task.ReservationID,
			"error", dbErr.Error(),
		)
		// Hoàn lại kho trên Redis nếu lỗi nghiệp vụ DB
		_, _ = redislock.ReleaseFlashSaleReservation(reqCtx, w.redisClient, task.CampaignID, task.ProductID, task.ReservationID, "CANCELLED")
		return
	}

	// 5. SAU KHI DB COMMIT THÀNH CÔNG: ĐỒNG BỘ SANG REDIS VÀ THÔNG BÁO CLIENT
	// a. Gọi Confirm trên Redis
	if w.redisClient != nil {
		_, _ = redislock.ConfirmFlashSaleReservation(reqCtx, w.redisClient, task.CampaignID, task.ProductID, task.ReservationID)

		// b. Cập nhật Status Snapshot trên Redis
		statusSnapshot := dto.FlashSaleOrderStatusResponse{
			ReservationID: task.ReservationID,
			Status:        "CONFIRMED",
			OrderID:       &order.ID,
			OrderCode:     order.OrderCode,
			ExpiresAt:     resv.ExpiresAt,
			UpdatedAt:     time.Now(),
		}
		statusJSON, _ := json.Marshal(statusSnapshot)
		_ = w.redisClient.Set(reqCtx, redislock.KeyOrderStatus(task.ReservationID), string(statusJSON), 24*time.Hour)

		// c. Phát Pub/Sub thông báo cho SSE Client đang mở kết nối
		_ = w.redisClient.Publish(reqCtx, fmt.Sprintf("pubsub:order-status:%s", task.ReservationID), string(statusJSON))
	}

	logger.InfoContext(reqCtx, "✅ [FLASH SALE WORKER] Lưu đơn hàng thành công, bắn event order.created (IsFlashSale=true) sang Kafka",
		"order_id", order.ID,
		"order_code", order.OrderCode,
		"reservation_id", task.ReservationID,
	)

	// 6. Bắn event order.created sang Kafka với cờ IsFlashSale = true (để ProductStockWorker không trừ kho lần 2)
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
			IsFlashSale:     true,
			CampaignID:      &task.CampaignID,
			ReservationID:   task.ReservationID,
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
