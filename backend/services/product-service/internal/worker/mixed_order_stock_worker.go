package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/services/product-service/internal/domain"
	"ecomerce-service/services/product-service/internal/repository"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

// MixedOrderStockWorker lắng nghe yêu cầu trừ kho cho các món thường trong đơn hỗn hợp (TopicMixedOrderStockRequest)
// và yêu cầu bồi hoàn tồn kho khi đơn bị hủy (TopicMixedOrderStockCompensate)
type MixedOrderStockWorker struct {
	reader             *kafka.Reader
	compensateReader   *kafka.Reader
	db                 *gorm.DB
	repo               domain.ProductRepository
	opRepo             domain.MixedOrderStockOperationRepository
	outboxRepo         domain.ProductOutboxRepository
	processedEventRepo domain.ProcessedEventRepository
	redisClient        *redis.Client
	producer           pkgKafka.OrderKafkaProducer
}

func NewMixedOrderStockWorker(
	brokers []string,
	db *gorm.DB,
	repo domain.ProductRepository,
	processedEventRepo domain.ProcessedEventRepository,
	rClient *redis.Client,
	producer pkgKafka.OrderKafkaProducer,
) *MixedOrderStockWorker {
	if len(brokers) == 0 {
		return nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          pkgKafka.TopicMixedOrderStockRequest,
		GroupID:        pkgKafka.ConsumerGroupMixedStockProduct,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})

	compensateReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          pkgKafka.TopicMixedOrderStockCompensate,
		GroupID:        pkgKafka.ConsumerGroupMixedStockProduct + "-compensate",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})

	var opRepo domain.MixedOrderStockOperationRepository
	var outboxRepo domain.ProductOutboxRepository
	if db != nil {
		opRepo = repository.NewMixedOrderStockOperationRepository(db)
		outboxRepo = repository.NewProductOutboxRepository(db)
	}

	return &MixedOrderStockWorker{
		reader:             reader,
		compensateReader:   compensateReader,
		db:                 db,
		repo:               repo,
		opRepo:             opRepo,
		outboxRepo:         outboxRepo,
		processedEventRepo: processedEventRepo,
		redisClient:        rClient,
		producer:           producer,
	}
}

// SetOpRepo cho phép inject mock repository trong tests
func (w *MixedOrderStockWorker) SetOpRepo(opRepo domain.MixedOrderStockOperationRepository) {
	w.opRepo = opRepo
}

// SetOutboxRepo cho phép inject mock outbox repository trong tests
func (w *MixedOrderStockWorker) SetOutboxRepo(outboxRepo domain.ProductOutboxRepository) {
	w.outboxRepo = outboxRepo
}

func (w *MixedOrderStockWorker) Start(ctx context.Context) {
	if w.reader != nil {
		logger.Info("📦 [MIXED SAGA] MixedOrderStockWorker đang lắng nghe topic "+pkgKafka.TopicMixedOrderStockRequest+"...",
			"group_id", pkgKafka.ConsumerGroupMixedStockProduct,
		)
		go w.listenRequests(ctx)
	}

	if w.compensateReader != nil {
		logger.Info("🔄 [MIXED SAGA] MixedOrderStockWorker đang lắng nghe topic "+pkgKafka.TopicMixedOrderStockCompensate+"...",
			"group_id", pkgKafka.ConsumerGroupMixedStockProduct+"-compensate",
		)
		go w.listenCompensations(ctx)
	}
}

func (w *MixedOrderStockWorker) listenRequests(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Info("🛑 MixedOrderStockWorker (Requests) nhận tín hiệu dừng")
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
					logger.Error("❌ MixedOrderStockWorker lỗi xử lý message, retry lại đúng message này",
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

				// Xử lý thành công hoặc đã lưu DLQ bền vững -> Commit synchronous offset
				commitBackoff := 500 * time.Millisecond
				for {
					if ctx.Err() != nil {
						return
					}
					if commitErr := w.reader.CommitMessages(ctx, m); commitErr != nil {
						logger.Error("❌ MixedOrderStockWorker lỗi commit offset, retry commit",
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

func (w *MixedOrderStockWorker) listenCompensations(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Info("🛑 MixedOrderStockWorker (Compensations) nhận tín hiệu dừng")
			_ = w.compensateReader.Close()
			return
		default:
			m, err := w.compensateReader.FetchMessage(ctx)
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

				if err := w.processCompensateMessage(ctx, m); err != nil {
					logger.Error("❌ MixedOrderStockWorker lỗi xử lý compensate message, retry lại đúng message này",
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
					if commitErr := w.compensateReader.CommitMessages(ctx, m); commitErr != nil {
						logger.Error("❌ MixedOrderStockWorker lỗi commit compensate offset, retry commit",
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

func (w *MixedOrderStockWorker) processMessage(ctx context.Context, m kafka.Message) error {
	var payload pkgKafka.MixedOrderStockRequestPayload
	if err := json.Unmarshal(m.Value, &payload); err != nil {
		logger.Error("❌ MixedOrderStockWorker: Lỗi parse JSON payload", "error", err.Error())
		if w.producer != nil {
			if dlqErr := w.producer.PublishDeadLetter(ctx, m.Topic, string(m.Key), m.Value, err.Error(), ""); dlqErr != nil {
				return fmt.Errorf("lỗi gửi DLQ cho message lỗi cú pháp: %w", dlqErr)
			}
		}
		return nil // Chỉ commit offset nếu DLQ gửi thành công
	}

	traceID := payload.TraceID
	if traceID == "" {
		traceID = fmt.Sprintf("mixed-saga-%d", payload.OrderID)
	}
	reqCtx := logger.SetTraceID(ctx, traceID)

	var resultPayload pkgKafka.MixedOrderStockResultPayload
	var shouldPublishResult bool
	var businessFailureErr error

	// Section 16.1 & 16.2: Xử lý State Machine trong 1 DB Transaction duy nhất có row lock
	txErr := w.db.Transaction(func(tx *gorm.DB) error {
		// Khóa bản ghi operation theo OrderID bằng SELECT ... FOR UPDATE
		op, err := w.opRepo.GetByOrderIDWithLock(tx, payload.OrderID)
		if err != nil {
			return fmt.Errorf("lỗi kiểm tra mixed_order_stock_operations: %w", err)
		}

		if op != nil {
			switch op.Status {
			case domain.MixedStockOpDeducted:
				// Đã trừ kho thành công trước đó: Lấy lại kết quả đã lưu, không trừ lại
				shouldPublishResult = true
				if op.ResultPayload != "" {
					_ = json.Unmarshal([]byte(op.ResultPayload), &resultPayload)
				} else {
					var items []pkgKafka.OrderItemPayload
					_ = json.Unmarshal([]byte(op.DeductedItems), &items)
					resultPayload = pkgKafka.MixedOrderStockResultPayload{
						EventID:       payload.EventID,
						EventType:     pkgKafka.EventMixedStockDeductResult,
						OrderID:       payload.OrderID,
						OrderCode:     payload.OrderCode,
						Success:       true,
						DeductedItems: items,
						TraceID:       traceID,
						Timestamp:     time.Now(),
					}
				}
				logger.InfoContext(reqCtx, "ℹ️ [MIXED SAGA] Yêu cầu trừ kho trùng lặp, gửi lại kết quả DEDUCTED đã lưu",
					"order_id", payload.OrderID,
				)
				return nil

			case domain.MixedStockOpFailed:
				// Đã thất bại trước đó: Gửi lại kết quả FAILED đã lưu, không trừ lại
				shouldPublishResult = true
				resultPayload = pkgKafka.MixedOrderStockResultPayload{
					EventID:   payload.EventID,
					EventType: pkgKafka.EventMixedStockDeductResult,
					OrderID:   payload.OrderID,
					OrderCode: payload.OrderCode,
					Success:   false,
					Reason:    op.Reason,
					TraceID:   traceID,
					Timestamp: time.Now(),
				}
				logger.InfoContext(reqCtx, "ℹ️ [MIXED SAGA] Yêu cầu trừ kho trùng lặp, gửi lại kết quả FAILED đã lưu",
					"order_id", payload.OrderID,
				)
				return nil

			case domain.MixedStockOpCancelledBeforeDeduct, domain.MixedStockOpCompensated:
				// 16.2: Compensate đã đến trước hoặc đơn đã bị hủy trước khi trừ kho -> Tuyệt đối không trừ kho!
				shouldPublishResult = true
				resultPayload = pkgKafka.MixedOrderStockResultPayload{
					EventID:   payload.EventID,
					EventType: pkgKafka.EventMixedStockDeductResult,
					OrderID:   payload.OrderID,
					OrderCode: payload.OrderCode,
					Success:   false,
					Reason:    "ORDER_CANCELLED_BEFORE_DEDUCT: Đơn hàng đã bị hủy hoặc bồi hoàn trước khi trừ kho",
					TraceID:   traceID,
					Timestamp: time.Now(),
				}
				logger.WarnContext(reqCtx, "⚠️ [MIXED SAGA] Đơn hàng đã CANCELLED_BEFORE_DEDUCT / COMPENSATED, từ chối trừ kho",
					"order_id", payload.OrderID,
				)
				return nil

			default:
				return fmt.Errorf("trạng thái operation không xác định: %s", op.Status)
			}
		}

		// Chưa có operation: Thử trừ kho toàn bộ các mặt hàng thường bằng conditional update nguyên tử
		var deductedItems []pkgKafka.OrderItemPayload
		for _, item := range payload.RegularItems {
			res := tx.Model(&domain.Product{}).
				Where("id = ? AND stock >= ?", item.ProductID, item.Quantity).
				Update("stock", gorm.Expr("stock - ?", item.Quantity))
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected == 0 {
				businessFailureErr = fmt.Errorf("sản phẩm #%d '%s' không đủ tồn kho thường", item.ProductID, item.ProductName)
				return businessFailureErr // Rollback toàn bộ các sản phẩm đã trừ trước đó trong tx
			}
			deductedItems = append(deductedItems, item)
		}

		// Trừ kho thành công toàn bộ: Ghi nhận DEDUCTED và lưu kết quả bền vững
		deductedBytes, _ := json.Marshal(deductedItems)
		resultPayload = pkgKafka.MixedOrderStockResultPayload{
			EventID:       payload.EventID,
			EventType:     pkgKafka.EventMixedStockDeductResult,
			OrderID:       payload.OrderID,
			OrderCode:     payload.OrderCode,
			Success:       true,
			DeductedItems: deductedItems,
			TraceID:       traceID,
			Timestamp:     time.Now(),
		}
		resBytes, _ := json.Marshal(resultPayload)

		newOp := &domain.MixedOrderStockOperation{
			OrderID:       payload.OrderID,
			OrderCode:     payload.OrderCode,
			Status:        domain.MixedStockOpDeducted,
			DeductedItems: string(deductedBytes),
			ResultPayload: string(resBytes),
		}
		if err := w.opRepo.Create(tx, newOp); err != nil {
			return fmt.Errorf("lỗi lưu operation DEDUCTED: %w", err)
		}

		// 18.5: Ghi nhận sự kiện vào Transactional Outbox trong CÙNG transaction
		if w.outboxRepo != nil {
			outboxID := fmt.Sprintf("ob-deduct-%d-%s", payload.OrderID, payload.EventID)
			outboxEv := &domain.ProductOutboxEvent{
				ID:            outboxID,
				AggregateType: "MIXED_ORDER_STOCK",
				AggregateID:   fmt.Sprintf("%d", payload.OrderID),
				EventType:     pkgKafka.EventMixedStockDeductResult,
				Topic:         pkgKafka.TopicMixedOrderStockResult,
				PartitionKey:  fmt.Sprintf("%d", payload.OrderID),
				Payload:       string(resBytes),
				Status:        domain.ProductOutboxStatusPending,
				NextAttemptAt: time.Now(),
			}
			if err := w.outboxRepo.Create(tx, outboxEv); err != nil {
				return fmt.Errorf("lỗi lưu ProductOutboxEvent DEDUCTED: %w", err)
			}
		}

		// Đồng bộ processed_events nếu repo tồn tại
		if w.processedEventRepo != nil {
			_ = w.processedEventRepo.SaveEvent(tx, "product-mixed-stock-consumer", payload.EventID, "SUCCESS", string(resBytes))
		}

		shouldPublishResult = true
		return nil
	})

	// Xử lý Business Failure (thiếu hàng): Ghi nhận FAILED bền vững trong transaction riêng (16.1 point 4)
	if businessFailureErr != nil {
		logger.WarnContext(reqCtx, "⚠️ [MIXED SAGA] Trừ kho thường thất bại, đã rollback toàn bộ trong transaction",
			"order_id", payload.OrderID,
			"error", businessFailureErr.Error(),
		)

		failPayload := pkgKafka.MixedOrderStockResultPayload{
			EventID:   payload.EventID,
			EventType: pkgKafka.EventMixedStockDeductResult,
			OrderID:   payload.OrderID,
			OrderCode: payload.OrderCode,
			Success:   false,
			Reason:    businessFailureErr.Error(),
			TraceID:   traceID,
			Timestamp: time.Now(),
		}
		failBytes, _ := json.Marshal(failPayload)

		saveErr := w.db.Transaction(func(txFail *gorm.DB) error {
			op, err := w.opRepo.GetByOrderIDWithLock(txFail, payload.OrderID)
			if err != nil {
				return err
			}
			if op == nil {
				if err := w.opRepo.Create(txFail, &domain.MixedOrderStockOperation{
					OrderID:       payload.OrderID,
					OrderCode:     payload.OrderCode,
					Status:        domain.MixedStockOpFailed,
					Reason:        businessFailureErr.Error(),
					ResultPayload: string(failBytes),
				}); err != nil {
					return err
				}
			}

			// 18.5: Ghi nhận sự kiện FAILED vào Transactional Outbox trong transaction
			if w.outboxRepo != nil {
				outboxID := fmt.Sprintf("ob-fail-%d-%s", payload.OrderID, payload.EventID)
				outboxEv := &domain.ProductOutboxEvent{
					ID:            outboxID,
					AggregateType: "MIXED_ORDER_STOCK",
					AggregateID:   fmt.Sprintf("%d", payload.OrderID),
					EventType:     pkgKafka.EventMixedStockDeductResult,
					Topic:         pkgKafka.TopicMixedOrderStockResult,
					PartitionKey:  fmt.Sprintf("%d", payload.OrderID),
					Payload:       string(failBytes),
					Status:        domain.ProductOutboxStatusPending,
					NextAttemptAt: time.Now(),
				}
				if err := w.outboxRepo.Create(txFail, outboxEv); err != nil {
					return fmt.Errorf("lỗi lưu ProductOutboxEvent FAILED: %w", err)
				}
			}
			return nil
		})
		if saveErr != nil {
			return fmt.Errorf("lỗi lưu trạng thái FAILED bền vững: %w", saveErr)
		}

		if w.processedEventRepo != nil {
			_ = w.processedEventRepo.SaveEvent(nil, "product-mixed-stock-consumer", payload.EventID, "FAILED", string(failBytes))
		}

		if w.producer != nil {
			if pubErr := w.producer.PublishMixedOrderStockResult(reqCtx, failPayload); pubErr != nil {
				logger.WarnContext(reqCtx, "⚠️ [MIXED SAGA] Direct publish FAILED lỗi, Outbox Publisher sẽ thử lại", "error", pubErr.Error())
			}
		}
		return nil
	}

	// Nếu gặp lỗi DB tạm thời (deadlock, connection error): Trả về error để retry, KHÔNG commit offset
	if txErr != nil {
		return fmt.Errorf("lỗi transaction xử lý stock request: %w", txErr)
	}

	// Xóa cache Redis cho các mặt hàng vừa trừ kho thành công
	if resultPayload.Success {
		for _, item := range resultPayload.DeductedItems {
			if w.redisClient != nil {
				_ = w.redisClient.Del(reqCtx, fmt.Sprintf("cache:product:%d", item.ProductID))
			}
		}
	}

	// Phát kết quả qua Kafka (Fast-path). Outbox Publisher Worker đảm bảo tính tin cậy
	if shouldPublishResult && w.producer != nil {
		if pubErr := w.producer.PublishMixedOrderStockResult(reqCtx, resultPayload); pubErr != nil {
			logger.WarnContext(reqCtx, "⚠️ [MIXED SAGA] Direct publish lỗi, Outbox Publisher Worker sẽ đảm nhận phát", "error", pubErr.Error())
		}
	}

	return nil
}

func (w *MixedOrderStockWorker) processCompensateMessage(ctx context.Context, m kafka.Message) error {
	var payload pkgKafka.MixedOrderStockCompensatePayload
	if err := json.Unmarshal(m.Value, &payload); err != nil {
		logger.Error("❌ MixedOrderStockWorker: Lỗi parse compensate payload", "error", err.Error())
		if w.producer != nil {
			if dlqErr := w.producer.PublishDeadLetter(ctx, m.Topic, string(m.Key), m.Value, err.Error(), ""); dlqErr != nil {
				return fmt.Errorf("lỗi gửi DLQ cho compensate message: %w", dlqErr)
			}
		}
		return nil
	}

	traceID := payload.TraceID
	if traceID == "" {
		traceID = fmt.Sprintf("mixed-comp-%d", payload.OrderID)
	}
	reqCtx := logger.SetTraceID(ctx, traceID)

	var itemsRefunded []pkgKafka.OrderItemPayload
	var resultStatus string
	var compResult pkgKafka.MixedOrderStockCompensateResultPayload

	// 16.2 & 17.3: State Machine xử lý bồi hoàn dưới khóa dòng theo OrderID
	txErr := w.db.Transaction(func(tx *gorm.DB) error {
		op, err := w.opRepo.GetByOrderIDWithLock(tx, payload.OrderID)
		if err != nil {
			return fmt.Errorf("lỗi kiểm tra mixed_order_stock_operations: %w", err)
		}

		if op == nil {
			// Case: Compensate đến trước Request!
			// 16.2: Lưu CANCELLED_BEFORE_DEDUCT; TUYỆT ĐỐI KHÔNG CỘNG KHO!
			newOp := &domain.MixedOrderStockOperation{
				OrderID:   payload.OrderID,
				OrderCode: payload.OrderCode,
				Status:    domain.MixedStockOpCancelledBeforeDeduct,
				Reason:    "Compensate nhận được trước khi trừ kho",
			}
			if err := w.opRepo.Create(tx, newOp); err != nil {
				return fmt.Errorf("lỗi lưu CANCELLED_BEFORE_DEDUCT: %w", err)
			}
			resultStatus = string(domain.MixedStockOpCancelledBeforeDeduct)
			logger.WarnContext(reqCtx, "⚠️ [MIXED SAGA] Nhận Compensate trước Request -> Lưu CANCELLED_BEFORE_DEDUCT, KHÔNG cộng kho",
				"order_id", payload.OrderID,
			)
		} else {
			switch op.Status {
			case domain.MixedStockOpDeducted:
				// 17.3: Đã trừ kho trước đó: Hoàn lại ĐÚNG các dòng đã thực tế trừ trong DeductedItems một lần
				// TUYỆT ĐỐI KHÔNG fallback sang payload.Items nếu dữ liệu hỏng để tránh sinh tồn kho ảo!
				if op.DeductedItems == "" {
					return fmt.Errorf("dữ liệu DeductedItems rỗng cho order #%d, không thể bồi hoàn tồn kho", payload.OrderID)
				}
				var itemsToRefund []pkgKafka.OrderItemPayload
				if err := json.Unmarshal([]byte(op.DeductedItems), &itemsToRefund); err != nil {
					return fmt.Errorf("dữ liệu DeductedItems bị hỏng cho order #%d, từ chối bồi hoàn để bảo toàn sổ cái: %w", payload.OrderID, err)
				}
				if len(itemsToRefund) == 0 {
					return fmt.Errorf("danh sách DeductedItems trống cho order #%d", payload.OrderID)
				}

				for _, item := range itemsToRefund {
					if err := tx.Model(&domain.Product{}).
						Where("id = ?", item.ProductID).
						Update("stock", gorm.Expr("stock + ?", item.Quantity)).Error; err != nil {
						return fmt.Errorf("lỗi bồi hoàn kho cho sản phẩm #%d: %w", item.ProductID, err)
					}
				}
				itemsRefunded = itemsToRefund

				op.Status = domain.MixedStockOpCompensated
				op.Reason = "Compensated successfully"
				if err := w.opRepo.Update(tx, op); err != nil {
					return fmt.Errorf("lỗi cập nhật trạng thái COMPENSATED: %w", err)
				}
				resultStatus = string(domain.MixedStockOpCompensated)
				logger.InfoContext(reqCtx, "✅ [MIXED SAGA] Hoàn kho thường thành công và cập nhật COMPENSATED",
					"order_id", payload.OrderID,
					"refunded_count", len(itemsRefunded),
				)

			case domain.MixedStockOpCancelledBeforeDeduct:
				// Đã nhận compensate trước đó: No-op! Không cộng kho!
				resultStatus = string(domain.MixedStockOpCancelledBeforeDeduct)
				logger.InfoContext(reqCtx, "ℹ️ [MIXED SAGA] Đơn hàng đã ở trạng thái CANCELLED_BEFORE_DEDUCT, bỏ qua không cộng kho",
					"order_id", payload.OrderID,
				)

			case domain.MixedStockOpCompensated:
				// Đã hoàn kho rồi: No-op! Không cộng kho lại!
				resultStatus = string(domain.MixedStockOpCompensated)
				logger.InfoContext(reqCtx, "ℹ️ [MIXED SAGA] Đơn hàng đã ở trạng thái COMPENSATED, bỏ qua không cộng kho lại",
					"order_id", payload.OrderID,
				)

			case domain.MixedStockOpFailed:
				// Đơn hàng trước đó đã FAILED (chưa từng trừ kho): No-op! Không cộng kho!
				resultStatus = string(domain.MixedStockOpFailed)
				logger.InfoContext(reqCtx, "ℹ️ [MIXED SAGA] Đơn hàng trước đó đã FAILED (chưa từng trừ kho), bỏ qua không cộng kho",
					"order_id", payload.OrderID,
				)

			default:
				return fmt.Errorf("trạng thái operation không xác định: %s", op.Status)
			}
		}

		// 19.2 (P0): Ghi nhận sự kiện bồi hoàn vào Outbox trong CÙNG transaction với hoàn kho và cập nhật status
		compResult = pkgKafka.MixedOrderStockCompensateResultPayload{
			EventID:   payload.EventID,
			EventType: pkgKafka.EventMixedStockCompensateResult,
			OrderID:   payload.OrderID,
			OrderCode: payload.OrderCode,
			Success:   true,
			Status:    resultStatus,
			Reason:    "Stock compensated or no-op confirmed",
			TraceID:   traceID,
			Timestamp: time.Now(),
		}
		compBytes, err := json.Marshal(compResult)
		if err != nil {
			return fmt.Errorf("lỗi marshal compensate result payload: %w", err)
		}

		if w.outboxRepo != nil {
			outboxID := fmt.Sprintf("ob-comp-%d-%s", payload.OrderID, payload.EventID)
			outboxEv := &domain.ProductOutboxEvent{
				ID:            outboxID,
				AggregateType: "MIXED_ORDER_STOCK_COMPENSATE",
				AggregateID:   fmt.Sprintf("%d", payload.OrderID),
				EventType:     pkgKafka.EventMixedStockCompensateResult,
				Topic:         pkgKafka.TopicMixedOrderStockCompensateResult,
				PartitionKey:  fmt.Sprintf("%d", payload.OrderID),
				Payload:       string(compBytes),
				Status:        domain.ProductOutboxStatusPending,
				NextAttemptAt: time.Now(),
			}
			if err := w.outboxRepo.Create(tx, outboxEv); err != nil {
				return fmt.Errorf("lỗi ghi nhận ProductOutboxEvent COMPENSATE trong transaction: %w", err)
			}
		}

		// Đồng bộ processed_events nếu repo tồn tại
		if w.processedEventRepo != nil {
			_ = w.processedEventRepo.SaveEvent(tx, "product-mixed-stock-compensate-consumer", payload.EventID, "SUCCESS", string(compBytes))
		}

		return nil
	})

	if txErr != nil {
		return fmt.Errorf("lỗi transaction bồi hoàn tồn kho: %w", txErr)
	}

	// Xóa cache Redis cho các sản phẩm vừa hoàn kho
	for _, item := range itemsRefunded {
		if w.redisClient != nil {
			_ = w.redisClient.Del(reqCtx, fmt.Sprintf("cache:product:%d", item.ProductID))
		}
	}

	// 17.4: Phát kết quả bồi hoàn qua Kafka (Fast-path)
	if w.producer != nil {
		if pubErr := w.producer.PublishMixedOrderStockCompensateResult(reqCtx, compResult); pubErr != nil {
			logger.WarnContext(reqCtx, "⚠️ [MIXED SAGA] Direct publish compensate result lỗi, Outbox Publisher sẽ phát", "error", pubErr.Error())
		}
	}

	return nil
}

func (w *MixedOrderStockWorker) Close() error {
	if w.reader != nil {
		_ = w.reader.Close()
	}
	if w.compensateReader != nil {
		_ = w.compensateReader.Close()
	}
	return nil
}
