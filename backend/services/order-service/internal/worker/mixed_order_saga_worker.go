package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/redislock"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/dto"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

// MixedOrderSagaWorker lắng nghe kết quả trừ kho thường từ TopicMixedOrderStockResult
// và kết quả bồi hoàn tồn kho từ TopicMixedOrderStockCompensateResult (Section 17.2)
type MixedOrderSagaWorker struct {
	reader                 *kafka.Reader
	compensateResultReader *kafka.Reader
	db                     *gorm.DB
	orderRepo              domain.OrderRepository
	fsRepo                 domain.FlashSaleRepository
	outboxRepo             domain.OutboxRepository
	redisClient            *redis.Client
	producer               pkgKafka.OrderKafkaProducer
}

func NewMixedOrderSagaWorker(
	brokers []string,
	db *gorm.DB,
	orderRepo domain.OrderRepository,
	fsRepo domain.FlashSaleRepository,
	outboxRepo domain.OutboxRepository,
	rClient *redis.Client,
	producer pkgKafka.OrderKafkaProducer,
) *MixedOrderSagaWorker {
	if len(brokers) == 0 {
		return nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          pkgKafka.TopicMixedOrderStockResult,
		GroupID:        pkgKafka.ConsumerGroupMixedStockOrder,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})

	compensateResultReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          pkgKafka.TopicMixedOrderStockCompensateResult,
		GroupID:        pkgKafka.ConsumerGroupMixedCompensateOrder,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})

	return &MixedOrderSagaWorker{
		reader:                 reader,
		compensateResultReader: compensateResultReader,
		db:                     db,
		orderRepo:              orderRepo,
		fsRepo:                 fsRepo,
		outboxRepo:             outboxRepo,
		redisClient:            rClient,
		producer:               producer,
	}
}

func (w *MixedOrderSagaWorker) Start(ctx context.Context) {
	if w.reader != nil {
		logger.Info("🔄 [MIXED SAGA] MixedOrderSagaWorker đang lắng nghe topic "+pkgKafka.TopicMixedOrderStockResult+"...",
			"group_id", pkgKafka.ConsumerGroupMixedStockOrder,
		)
		go w.listenResults(ctx)
	}

	if w.compensateResultReader != nil {
		logger.Info("🔄 [MIXED SAGA] MixedOrderSagaWorker đang lắng nghe topic "+pkgKafka.TopicMixedOrderStockCompensateResult+"...",
			"group_id", pkgKafka.ConsumerGroupMixedCompensateOrder,
		)
		go w.listenCompensateResults(ctx)
	}
}

func (w *MixedOrderSagaWorker) listenResults(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Info("🛑 MixedOrderSagaWorker (Results) nhận tín hiệu dừng")
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

			// 20.1 (P0): In-place retry loop cho đúng message 'm'.
			// Không bao giờ fetch message tiếp theo trên cùng partition khi message này chưa đạt durable outcome.
			backoff := 500 * time.Millisecond
			maxBackoff := 10 * time.Second
			for {
				if ctx.Err() != nil {
					return
				}

				if err := w.processMessage(ctx, m); err != nil {
					logger.Error("❌ MixedOrderSagaWorker lỗi xử lý message, retry lại đúng message này",
						"topic", m.Topic,
						"partition", m.Partition,
						"offset", m.Offset,
						"backoff", backoff.String(),
						"error", err.Error(),
					)
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
					continue
				}

				// Synchronous commit retry
				commitBackoff := 500 * time.Millisecond
				for {
					if ctx.Err() != nil {
						return
					}
					if commitErr := w.reader.CommitMessages(ctx, m); commitErr != nil {
						logger.Error("❌ MixedOrderSagaWorker lỗi commit offset, retry commit",
							"topic", m.Topic,
							"partition", m.Partition,
							"offset", m.Offset,
							"error", commitErr.Error(),
						)
						select {
						case <-ctx.Done():
							return
						case <-time.After(commitBackoff):
						}
						commitBackoff *= 2
						if commitBackoff > maxBackoff {
							commitBackoff = maxBackoff
						}
						continue
					}
					break
				}
				break // Hoàn tất message 'm', tiến tới fetch message tiếp theo
			}
		}
	}
}

func (w *MixedOrderSagaWorker) listenCompensateResults(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Info("🛑 MixedOrderSagaWorker (CompensateResults) nhận tín hiệu dừng")
			_ = w.compensateResultReader.Close()
			return
		default:
			m, err := w.compensateResultReader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// 20.1 (P0): In-place retry loop cho đúng message 'm'
			backoff := 500 * time.Millisecond
			maxBackoff := 10 * time.Second
			for {
				if ctx.Err() != nil {
					return
				}

				if err := w.processCompensateResultMessage(ctx, m); err != nil {
					logger.Error("❌ MixedOrderSagaWorker lỗi xử lý compensate result message, retry lại đúng message này",
						"topic", m.Topic,
						"partition", m.Partition,
						"offset", m.Offset,
						"backoff", backoff.String(),
						"error", err.Error(),
					)
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
					continue
				}

				// Synchronous commit retry
				commitBackoff := 500 * time.Millisecond
				for {
					if ctx.Err() != nil {
						return
					}
					if commitErr := w.compensateResultReader.CommitMessages(ctx, m); commitErr != nil {
						logger.Error("❌ MixedOrderSagaWorker lỗi commit compensate result offset, retry commit",
							"topic", m.Topic,
							"partition", m.Partition,
							"offset", m.Offset,
							"error", commitErr.Error(),
						)
						select {
						case <-ctx.Done():
							return
						case <-time.After(commitBackoff):
						}
						commitBackoff *= 2
						if commitBackoff > maxBackoff {
							commitBackoff = maxBackoff
						}
						continue
					}
					break
				}
				break // Hoàn tất message 'm', tiến tới fetch message tiếp theo
			}
		}
	}
}

func (w *MixedOrderSagaWorker) processMessage(ctx context.Context, m kafka.Message) error {
	var payload pkgKafka.MixedOrderStockResultPayload
	if err := json.Unmarshal(m.Value, &payload); err != nil {
		logger.Error("❌ MixedOrderSagaWorker lỗi deserialize MixedOrderStockResultPayload", "error", err.Error())
		if w.producer != nil {
			if dlqErr := w.producer.PublishDeadLetter(ctx, m.Topic, string(m.Key), m.Value, err.Error(), ""); dlqErr != nil {
				return fmt.Errorf("lỗi gửi DLQ cho message lỗi cú pháp: %w (nguyên nhân parse: %v)", dlqErr, err)
			}
		}
		return nil // Chỉ commit offset nếu DLQ gửi thành công
	}

	traceID := payload.TraceID
	reqCtx := logger.SetTraceID(ctx, traceID)

	order, err := w.orderRepo.FindByID(payload.OrderID)
	if err != nil {
		return fmt.Errorf("lỗi truy vấn đơn hàng #%d: %w", payload.OrderID, err)
	}
	if order == nil {
		logger.WarnContext(reqCtx, "⚠️ Không tìm thấy đơn hàng trong DB", "order_id", payload.OrderID)
		return nil
	}

	// Point 4: Xử lý Late Success khi đơn hàng đã bị Expiry Worker hủy trước đó
	if order.OrderStatus == domain.OrderStatusCancelled && payload.Success && len(payload.DeductedItems) > 0 {
		logger.WarnContext(reqCtx, "⚠️ Đơn hàng đã bị CANCELLED nhưng nhận Late Success, bồi hoàn kho thường", "order_id", order.ID)
		return w.compensateLateSuccess(reqCtx, order, payload.DeductedItems)
	}

	// Đơn hàng không ở trạng thái PENDING thì bỏ qua (Idempotency)
	if order.OrderStatus != domain.OrderStatusPending {
		logger.InfoContext(reqCtx, "ℹ️ Đơn hàng đã ở trạng thái kết thúc, bỏ qua",
			"order_id", order.ID,
			"order_status", order.OrderStatus,
		)
		return nil
	}

	if payload.Success {
		return w.handleStockSuccess(reqCtx, order)
	}

	return w.handleStockFailure(reqCtx, order, payload.Reason, nil)
}

func (w *MixedOrderSagaWorker) compensateLateSuccess(ctx context.Context, order *domain.Order, items []pkgKafka.OrderItemPayload) error {
	now := time.Now()
	compPayload := pkgKafka.MixedOrderStockCompensatePayload{
		EventID:   fmt.Sprintf("comp-late-%d-%d", order.ID, now.UnixNano()),
		EventType: pkgKafka.EventMixedStockCompensate,
		OrderID:   order.ID,
		OrderCode: order.OrderCode,
		Items:     items,
		Reason:    "Late stock success arrived for already cancelled order",
		TraceID:   logger.GetTraceID(ctx),
		Timestamp: now,
	}
	data, err := json.Marshal(compPayload)
	if err != nil {
		return fmt.Errorf("lỗi serialize compensate payload: %w", err)
	}

	outboxComp := &domain.OutboxEvent{
		ID:            fmt.Sprintf("outbox-comp-late-%d", order.ID),
		AggregateType: "order",
		AggregateID:   fmt.Sprintf("%d", order.ID),
		EventType:     pkgKafka.EventMixedStockCompensate,
		Topic:         pkgKafka.TopicMixedOrderStockCompensate,
		PartitionKey:  fmt.Sprintf("order-%d", order.ID),
		Payload:       string(data),
		Status:        domain.OutboxStatusPending,
		CreatedAt:     now,
	}

	if w.db != nil {
		if err := w.db.Create(outboxComp).Error; err != nil {
			return fmt.Errorf("lỗi ghi outbox bồi hoàn late success: %w", err)
		}
	}

	if w.producer != nil {
		_ = w.producer.PublishMixedOrderStockCompensate(ctx, compPayload)
	}

	logger.InfoContext(ctx, "✅ [MIXED SAGA] Đã phát yêu cầu bồi hoàn cho Late Success", "order_id", order.ID)
	return nil
}

func (w *MixedOrderSagaWorker) handleStockSuccess(ctx context.Context, order *domain.Order) error {
	// 1. Kiểm tra tính hợp lệ của các reservation Flash Sale (chưa hết hạn)
	now := time.Now()
	var flashSaleItems []domain.OrderItem
	for _, item := range order.Items {
		if item.IsFlashSale && item.ReservationID != "" {
			flashSaleItems = append(flashSaleItems, item)
		}
	}

	allReservationsValid := true
	for _, item := range flashSaleItems {
		resv, err := w.fsRepo.FindReservationByID(item.ReservationID)
		if err != nil || resv == nil || resv.Status != domain.ReservationStatusReserved || resv.ExpiresAt.Before(now) {
			allReservationsValid = false
			logger.WarnContext(ctx, "⚠️ Suất giữ chỗ Flash Sale đã hết hạn hoặc không hợp lệ",
				"order_id", order.ID,
				"reservation_id", item.ReservationID,
			)
			break
		}
	}

	if !allReservationsValid {
		// Suất Flash Sale đã hết hạn trước khi có kết quả kho thường!
		// Hủy đơn hàng và bồi hoàn kho thường đã trừ (Point 4 fix)
		var regularItems []pkgKafka.OrderItemPayload
		for _, it := range order.Items {
			if !it.IsFlashSale {
				regularItems = append(regularItems, pkgKafka.OrderItemPayload{
					ProductID:   it.ProductID,
					ProductName: it.ProductName,
					Quantity:    it.Quantity,
				})
			}
		}
		return w.handleStockFailure(ctx, order, "Suất Flash Sale đã hết hạn trong thời gian chờ xác nhận kho", regularItems)
	}

	// 2. ACID Transaction: Confirm Order, Confirm Flash Sale Reservations, Write Outbox
	err := w.db.Transaction(func(tx *gorm.DB) error {
		// Cập nhật Order status -> CONFIRMED
		res := tx.Model(&domain.Order{}).Where("id = ? AND order_status = ?", order.ID, domain.OrderStatusPending).
			Update("order_status", domain.OrderStatusConfirmed)
		if res.Error != nil {
			return fmt.Errorf("lỗi update order CONFIRMED: %w", res.Error)
		}
		if res.RowsAffected == 0 {
			return fmt.Errorf("đơn hàng #%d không còn ở trạng thái PENDING", order.ID)
		}

		// Xác nhận từng reservation Flash Sale và tạo outbox FLASH_SALE_ORDER_CONFIRMED
		for _, item := range flashSaleItems {
			if err := w.fsRepo.ConfirmReservationDB(tx, item.ReservationID, order.ID); err != nil {
				return fmt.Errorf("lỗi confirm reservation %s: %w", item.ReservationID, err)
			}

			campID := uint(0)
			if item.CampaignID != nil {
				campID = *item.CampaignID
			}

			fsPayload := pkgKafka.FlashSaleOrderConfirmedPayload{
				EventID:       uuid.New().String(),
				EventType:     pkgKafka.EventFlashSaleOrderConfirmed,
				OccurredAt:    time.Now(),
				TraceID:       logger.GetTraceID(ctx),
				OrderID:       order.ID,
				OrderCode:     order.OrderCode,
				ReservationID: item.ReservationID,
				CampaignID:    campID,
				ProductID:     item.ProductID,
				Quantity:      item.Quantity,
			}
			payloadBytes, _ := json.Marshal(fsPayload)
			outboxFS := &domain.OutboxEvent{
				ID:            fsPayload.EventID,
				AggregateType: "FlashSaleOrder",
				AggregateID:   fmt.Sprintf("%d", order.ID),
				EventType:     pkgKafka.EventFlashSaleOrderConfirmed,
				Topic:         pkgKafka.TopicFlashSaleConfirmed,
				PartitionKey:  fmt.Sprintf("%d:%d", campID, item.ProductID),
				Payload:       string(payloadBytes),
				Status:        domain.OutboxStatusPending,
				CreatedAt:     time.Now(),
			}
			if err := tx.Create(outboxFS).Error; err != nil {
				return fmt.Errorf("lỗi tạo outbox FLASH_SALE_ORDER_CONFIRMED: %w", err)
			}
		}

		// Tạo outbox ORDER_CREATED cho downstream (email, analytics)
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
		orderPayload := pkgKafka.OrderCreatedPayload{
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
			IsFlashSale:        len(flashSaleItems) > 0,
			StockHandledBySaga: true, // Hợp đồng trừ kho rõ ràng: Đã được MixedOrderStockWorker trừ kho thường
			TraceID:            logger.GetTraceID(ctx),
			CreatedAt:          order.CreatedAt,
		}
		orderData, _ := json.Marshal(orderPayload)
		outboxOrder := &domain.OutboxEvent{
			ID:            uuid.New().String(),
			AggregateType: "Order",
			AggregateID:   fmt.Sprintf("%d", order.ID),
			EventType:     pkgKafka.EventOrderCreated,
			Topic:         pkgKafka.TopicOrderEvents,
			PartitionKey:  fmt.Sprintf("%d", order.ID),
			Payload:       string(orderData),
			Status:        domain.OutboxStatusPending,
			CreatedAt:     time.Now(),
		}
		return tx.Create(outboxOrder).Error
	})

	if err != nil {
		logger.ErrorContext(ctx, "❌ [MIXED SAGA] Lỗi commit confirm order và reservations", "error", err.Error())
		return err // Point 6 fix: Không nuốt lỗi, trả về để Kafka retry
	}

	// 3. Inline Best-Effort Redis Confirm
	for _, item := range flashSaleItems {
		campID := uint(0)
		if item.CampaignID != nil {
			campID = *item.CampaignID
		}
		if w.redisClient != nil {
			_, _ = redislock.ConfirmFlashSaleReservation(ctx, w.redisClient, campID, item.ProductID, item.ReservationID)
		}
	}

	// Cập nhật snapshot trạng thái đơn hàng trên Redis để phục vụ Client Polling
	if w.redisClient != nil {
		statusSnapshot := dto.FlashSaleOrderStatusResponse{
			Status:    domain.OrderStatusConfirmed,
			OrderID:   &order.ID,
			OrderCode: order.OrderCode,
			UpdatedAt: time.Now(),
		}
		statusJSON, _ := json.Marshal(statusSnapshot)
		_ = w.redisClient.Set(ctx, fmt.Sprintf("order:status:%d", order.ID), string(statusJSON), 24*time.Hour)
	}

	logger.InfoContext(ctx, "🎉 [MIXED SAGA COMPLETED] Đơn hàng hỗn hợp đã được xác nhận thành công!",
		"order_id", order.ID,
		"order_code", order.OrderCode,
	)

	return nil
}

func (w *MixedOrderSagaWorker) handleStockFailure(ctx context.Context, order *domain.Order, reason string, regularItemsToCompensate []pkgKafka.OrderItemPayload) error {
	logger.WarnContext(ctx, "🚫 [MIXED SAGA FAILED] Hủy đơn hàng hỗn hợp do lỗi tồn kho hoặc hết hạn",
		"order_id", order.ID,
		"reason", reason,
	)

	now := time.Now()

	// 17.2: Nếu có món thường cần bồi hoàn bên Product Service, KHÔNG ĐƯỢC chuyển ngay sang CANCELLED
	// Phải chuyển sang COMPENSATING và chờ TopicMixedOrderStockCompensateResult xác nhận!
	targetStatus := domain.OrderStatusCancelled
	if len(regularItemsToCompensate) > 0 {
		targetStatus = domain.OrderStatusCompensating
	}

	// 1. Transaction Database: Cập nhật status COMPENSATING hoặc CANCELLED, Release Reservation trong DB, và ghi outbox bồi hoàn nếu cần
	err := w.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&domain.Order{}).Where("id = ?", order.ID).
			Update("order_status", targetStatus).Error; err != nil {
			return fmt.Errorf("lỗi cập nhật order %s: %w", targetStatus, err)
		}

		for _, item := range order.Items {
			if item.IsFlashSale && item.ReservationID != "" {
				if err := w.fsRepo.ReleaseReservationDB(tx, item.ReservationID, domain.ReservationStatusCancelled); err != nil {
					return fmt.Errorf("lỗi release reservation %s: %w", item.ReservationID, err)
				}
			}
		}

		// Nếu có món thường cần bồi hoàn (Point 4 & Section 17.2 fix)
		if len(regularItemsToCompensate) > 0 {
			compPayload := pkgKafka.MixedOrderStockCompensatePayload{
				EventID:   fmt.Sprintf("comp-fail-%d-%d", order.ID, now.UnixNano()),
				EventType: pkgKafka.EventMixedStockCompensate,
				OrderID:   order.ID,
				OrderCode: order.OrderCode,
				Items:     regularItemsToCompensate,
				Reason:    reason,
				TraceID:   logger.GetTraceID(ctx),
				Timestamp: now,
			}
			compData, _ := json.Marshal(compPayload)
			outboxComp := &domain.OutboxEvent{
				ID:            fmt.Sprintf("outbox-comp-fail-%d", order.ID),
				AggregateType: "order",
				AggregateID:   fmt.Sprintf("%d", order.ID),
				EventType:     pkgKafka.EventMixedStockCompensate,
				Topic:         pkgKafka.TopicMixedOrderStockCompensate,
				PartitionKey:  fmt.Sprintf("order-%d", order.ID),
				Payload:       string(compData),
				Status:        domain.OutboxStatusPending,
				CreatedAt:     now,
			}
			if err := tx.Create(outboxComp).Error; err != nil {
				return fmt.Errorf("lỗi ghi outbox bồi hoàn: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		logger.ErrorContext(ctx, "❌ [MIXED SAGA] Lỗi transaction hủy đơn hàng", "error", err.Error())
		return err // Point 6 fix: Return error for retry
	}

	// 2. Giải phóng các suất Flash Sale trên Redis
	for _, item := range order.Items {
		if item.IsFlashSale && item.ReservationID != "" {
			campID := uint(0)
			if item.CampaignID != nil {
				campID = *item.CampaignID
			}

			if w.redisClient != nil {
				_, _ = redislock.ReleaseFlashSaleReservation(ctx, w.redisClient, campID, item.ProductID, item.ReservationID, "CANCELLED")
			}
		}
	}

	// Bắn ngay event bồi hoàn qua producer (best-effort kèm outbox)
	if w.producer != nil && len(regularItemsToCompensate) > 0 {
		_ = w.producer.PublishMixedOrderStockCompensate(ctx, pkgKafka.MixedOrderStockCompensatePayload{
			EventID:   fmt.Sprintf("comp-fail-%d-%d", order.ID, now.UnixNano()),
			EventType: pkgKafka.EventMixedStockCompensate,
			OrderID:   order.ID,
			OrderCode: order.OrderCode,
			Items:     regularItemsToCompensate,
			Reason:    reason,
			TraceID:   logger.GetTraceID(ctx),
			Timestamp: now,
		})
	}

	// 3. Cập nhật snapshot trạng thái đơn hàng trên Redis
	if w.redisClient != nil {
		statusSnapshot := dto.FlashSaleOrderStatusResponse{
			Status:        targetStatus,
			OrderID:       &order.ID,
			OrderCode:     order.OrderCode,
			FailureReason: reason,
			UpdatedAt:     time.Now(),
		}
		statusJSON, _ := json.Marshal(statusSnapshot)
		_ = w.redisClient.Set(ctx, fmt.Sprintf("order:status:%d", order.ID), string(statusJSON), 24*time.Hour)
	}

	return nil
}

// 17.2: processCompensateResultMessage xử lý kết quả bồi hoàn tồn kho thường từ Product Service
// Chuyển đơn hàng từ COMPENSATING -> CANCELLED
func (w *MixedOrderSagaWorker) processCompensateResultMessage(ctx context.Context, m kafka.Message) error {
	var payload pkgKafka.MixedOrderStockCompensateResultPayload
	if err := json.Unmarshal(m.Value, &payload); err != nil {
		if w.producer != nil {
			if dlqErr := w.producer.PublishDeadLetter(ctx, m.Topic, string(m.Key), m.Value, err.Error(), ""); dlqErr != nil {
				return fmt.Errorf("lỗi gửi DLQ cho compensate result message lỗi cú pháp: %w (nguyên nhân parse: %v)", dlqErr, err)
			}
		}
		return nil // Chỉ commit offset khi DLQ gửi thành công
	}

	traceID := payload.TraceID
	reqCtx := logger.SetTraceID(ctx, traceID)

	logger.InfoContext(reqCtx, "📥 [MIXED SAGA] Nhận kết quả bồi hoàn tồn kho thường từ Product Service",
		"order_id", payload.OrderID,
		"status", payload.Status,
		"success", payload.Success,
	)

	order, err := w.orderRepo.FindByID(payload.OrderID)
	if err != nil {
		return fmt.Errorf("lỗi truy vấn đơn hàng #%d: %w", payload.OrderID, err)
	}
	if order == nil {
		logger.WarnContext(reqCtx, "⚠️ Không tìm thấy đơn hàng khi nhận compensate result", "order_id", payload.OrderID)
		return nil
	}

	if order.OrderStatus == domain.OrderStatusCancelled {
		logger.InfoContext(reqCtx, "ℹ️ Đơn hàng đã ở trạng thái CANCELLED trước đó, bỏ qua", "order_id", order.ID)
		return nil
	}

	// 17.2: Chuyển trạng thái từ COMPENSATING -> CANCELLED khi nhận kết quả thành công
	if payload.Success {
		err := w.db.Transaction(func(tx *gorm.DB) error {
			res := tx.Model(&domain.Order{}).
				Where("id = ? AND order_status = ?", order.ID, domain.OrderStatusCompensating).
				Update("order_status", domain.OrderStatusCancelled)
			if res.Error != nil {
				return res.Error
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("lỗi cập nhật order #%d sang CANCELLED: %w", order.ID, err)
		}

		if w.redisClient != nil {
			statusSnapshot := dto.FlashSaleOrderStatusResponse{
				Status:        domain.OrderStatusCancelled,
				OrderID:       &order.ID,
				OrderCode:     order.OrderCode,
				FailureReason: "Đơn hàng đã được bồi hoàn kho thường và hủy thành công",
				UpdatedAt:     time.Now(),
			}
			statusJSON, _ := json.Marshal(statusSnapshot)
			_ = w.redisClient.Set(ctx, fmt.Sprintf("order:status:%d", order.ID), string(statusJSON), 24*time.Hour)
		}

		logger.InfoContext(reqCtx, "✅ [MIXED SAGA] Đã hoàn tất bồi hoàn và chuyển đơn hàng sang CANCELLED",
			"order_id", order.ID,
		)
	}

	return nil
}

func (w *MixedOrderSagaWorker) Close() error {
	if w.reader != nil {
		_ = w.reader.Close()
	}
	if w.compensateResultReader != nil {
		_ = w.compensateResultReader.Close()
	}
	return nil
}
