package worker

import (
	"context"
	"encoding/json"
	"errors"
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

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

type ProcessingResult int

const (
	ProcessingSucceeded ProcessingResult = iota
	ProcessingTerminal
	ProcessingRetryable
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

				result, procErr := w.processFlashSaleOrder(ctx, m)
				switch result {
				case ProcessingSucceeded, ProcessingTerminal:
					_ = w.reader.CommitMessages(ctx, m)
				case ProcessingRetryable:
					if procErr != nil {
						logger.WarnContext(ctx, "⚠️ FlashSaleWorker gặp lỗi có thể retry, hoãn commit message", "error", procErr.Error())
					}
					time.Sleep(1 * time.Second)
				}
			}
		}
	}()
}

func (w *FlashSaleWorker) processFlashSaleOrder(ctx context.Context, m kafka.Message) (ProcessingResult, error) {
	var task pkgKafka.FlashSaleOrderTaskPayload
	if err := json.Unmarshal(m.Value, &task); err != nil {
		logger.Error("❌ FlashSaleWorker: Lỗi deserialize JSON payload", "error", err.Error())
		if w.kafkaProducer != nil {
			_ = w.kafkaProducer.PublishDeadLetter(ctx, m.Topic, string(m.Key), m.Value, "Invalid JSON: "+err.Error(), "")
		}
		return ProcessingTerminal, nil
	}

	traceID := task.TraceID
	if traceID == "" {
		traceID = "fs-worker-" + task.ReservationID
	}
	reqCtx := logger.SetTraceID(ctx, traceID)

	if task.ReservationID == "" {
		errMsg := "ReservationID rỗng"
		logger.ErrorContext(reqCtx, "❌ FlashSaleWorker payload không hợp lệ", "error", errMsg)
		if w.kafkaProducer != nil {
			_ = w.kafkaProducer.PublishDeadLetter(reqCtx, m.Topic, string(m.Key), m.Value, errMsg, traceID)
		}
		return ProcessingTerminal, nil
	}

	// 1. Idempotency Check với processed_events (sử dụng EventID do Producer tạo ra hoặc fallback)
	eventID := task.EventID
	if eventID == "" {
		eventID = fmt.Sprintf("fs-order-%s", task.ReservationID)
	}

	if w.processedEventRepo != nil {
		processed, err := w.processedEventRepo.HasProcessed("flash-sale-worker", eventID)
		if err == nil && processed {
			logger.InfoContext(reqCtx, "🔁 [IDEMPOTENT] Sự kiện Flash Sale đã được xử lý trước đó, bỏ qua",
				"reservation_id", task.ReservationID,
				"event_id", eventID,
			)
			return ProcessingSucceeded, nil
		}
	}

	// 2. Kiểm tra trạng thái hiện tại của Reservation trong DB
	resv, err := w.fsRepo.FindReservationByID(task.ReservationID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			logger.WarnContext(reqCtx, "⚠️ FlashSaleWorker: Không tìm thấy reservation trong DB",
				"reservation_id", task.ReservationID,
			)
			return ProcessingTerminal, nil
		}
		logger.ErrorContext(reqCtx, "❌ FlashSaleWorker: Lỗi truy vấn reservation trong DB",
			"reservation_id", task.ReservationID,
			"error", err.Error(),
		)
		return ProcessingRetryable, err
	}

	if resv.Status == domain.ReservationStatusConfirmed {
		logger.InfoContext(reqCtx, "Đơn Flash Sale đã được CONFIRMED trước đó", "reservation_id", task.ReservationID)
		return ProcessingSucceeded, nil
	}
	if resv.Status == domain.ReservationStatusCancelled || resv.Status == domain.ReservationStatusExpired {
		logger.WarnContext(reqCtx, "Reservation đã bị CANCELLED/EXPIRED, hủy xử lý", "reservation_id", task.ReservationID)
		return ProcessingTerminal, nil
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

	// Chống panic khi reservation_id ngắn hơn 8 ký tự
	var suffix string
	if len(task.ReservationID) >= 8 {
		suffix = task.ReservationID[len(task.ReservationID)-8:]
	} else {
		suffix = fmt.Sprintf("%08s", task.ReservationID)
	}
	orderCode := fmt.Sprintf("ORD-FS-%s", strings.ToUpper(suffix))

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

		// d. Outbox 1: FLASH_SALE_ORDER_CONFIRMED cho Product Service ledger & Redis projection
		fsConfirmedPayload := pkgKafka.FlashSaleOrderConfirmedPayload{
			EventID:       uuid.New().String(),
			EventType:     pkgKafka.EventFlashSaleOrderConfirmed,
			OccurredAt:    time.Now(),
			TraceID:       traceID,
			OrderID:       order.ID,
			OrderCode:     order.OrderCode,
			ReservationID: task.ReservationID,
			CampaignID:    task.CampaignID,
			ProductID:     task.ProductID,
			Quantity:      task.Quantity,
		}
		fsBytes, _ := json.Marshal(fsConfirmedPayload)
		outboxFS := &domain.OutboxEvent{
			ID:            fsConfirmedPayload.EventID,
			AggregateType: "FlashSaleOrder",
			AggregateID:   fmt.Sprintf("%d", order.ID),
			EventType:     pkgKafka.EventFlashSaleOrderConfirmed,
			Topic:         pkgKafka.TopicFlashSaleConfirmed,
			PartitionKey:  fmt.Sprintf("%d:%d", task.CampaignID, task.ProductID),
			Payload:       string(fsBytes),
			Status:        domain.OutboxStatusPending,
			NextAttemptAt: time.Now(),
			CreatedAt:     time.Now(),
		}
		if err := tx.Create(outboxFS).Error; err != nil {
			return err
		}

		// e. Outbox 2: ORDER_CREATED (IsFlashSale=true) cho email & các consumer hạ nguồn
		orderCreatedPayload := pkgKafka.OrderCreatedPayload{
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
		}
		orderBytes, _ := json.Marshal(orderCreatedPayload)
		outboxOrder := &domain.OutboxEvent{
			ID:            uuid.New().String(),
			AggregateType: "Order",
			AggregateID:   fmt.Sprintf("%d", order.ID),
			EventType:     pkgKafka.EventOrderCreated,
			Topic:         pkgKafka.TopicOrderEvents,
			PartitionKey:  order.UserID,
			Payload:       string(orderBytes),
			Status:        domain.OutboxStatusPending,
			NextAttemptAt: time.Now(),
			CreatedAt:     time.Now(),
		}
		if err := tx.Create(outboxOrder).Error; err != nil {
			return err
		}

		return nil
	})

	if dbErr != nil {
		logger.ErrorContext(reqCtx, "❌ FlashSaleWorker: Lưu đơn hàng thất bại",
			"reservation_id", task.ReservationID,
			"error", dbErr.Error(),
		)
		// Không gọi hủy reservation trên Redis đối với lỗi DB/hạ tầng tạm thời.
		return ProcessingRetryable, dbErr
	}

	// 5. HYBRID CONFIRMATION FAST-PATH (Đồng bộ với strict short timeout, không dùng untracked goroutine)
	if w.redisClient != nil {
		fastCtx, fastCancel := context.WithTimeout(reqCtx, 150*time.Millisecond)
		defer fastCancel()

		_, _ = redislock.ConfirmFlashSaleReservation(fastCtx, w.redisClient, task.CampaignID, task.ProductID, task.ReservationID)

		statusSnapshot := dto.FlashSaleOrderStatusResponse{
			ReservationID: task.ReservationID,
			Status:        "CONFIRMED",
			OrderID:       &order.ID,
			OrderCode:     order.OrderCode,
			ExpiresAt:     resv.ExpiresAt,
			UpdatedAt:     time.Now(),
		}
		if statusJSON, err := json.Marshal(statusSnapshot); err == nil {
			_ = w.redisClient.Set(fastCtx, redislock.KeyOrderStatus(task.ReservationID), string(statusJSON), 24*time.Hour)
			_ = w.redisClient.Publish(fastCtx, fmt.Sprintf("pubsub:order-status:%s", task.ReservationID), string(statusJSON))
		}
	}

	logger.InfoContext(reqCtx, "✅ [FLASH SALE WORKER] Lưu đơn hàng thành công và đã ghi nhận 2 Outbox Events",
		"order_id", order.ID,
		"order_code", order.OrderCode,
		"reservation_id", task.ReservationID,
	)

	return ProcessingSucceeded, nil
}

func (w *FlashSaleWorker) Close() error {
	if w.reader != nil {
		return w.reader.Close()
	}
	return nil
}
