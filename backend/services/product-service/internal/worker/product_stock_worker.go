package worker

import (
	"context"
	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/services/product-service/internal/domain"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// ProductStockWorker lắng nghe event ORDER_CREATED từ Kafka để trừ tồn kho trong Database của Product Service
type ProductStockWorker struct {
	reader      *kafka.Reader
	producer    pkgKafka.OrderKafkaProducer
	repo        domain.ProductRepository
	redisClient *redis.Client
}

// NewProductStockWorker khởi tạo Kafka Consumer cho Product Service
func NewProductStockWorker(
	brokers []string,
	repo domain.ProductRepository,
	rClient *redis.Client,
	producer pkgKafka.OrderKafkaProducer,
) *ProductStockWorker {
	if len(brokers) == 0 {
		return nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          pkgKafka.TopicOrderEvents,
		GroupID:        pkgKafka.ConsumerGroupProductStock,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})

	return &ProductStockWorker{
		reader:      reader,
		producer:    producer,
		repo:        repo,
		redisClient: rClient,
	}
}

// Start bắt đầu lắng nghe sự kiện từ Kafka
func (w *ProductStockWorker) Start(ctx context.Context) {
	if w.reader == nil {
		return
	}

	logger.Info("📦 [KAFKA SAGA] ProductStockWorker đang lắng nghe topic order.events...",
		"group_id", pkgKafka.ConsumerGroupProductStock,
	)

	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("🛑 ProductStockWorker nhận tín hiệu dừng")
				_ = w.reader.Close()
				return
			default:
				m, err := w.reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					logger.Warn("⚠️ ProductStockWorker fetch message lỗi", "error", err.Error())
					time.Sleep(500 * time.Millisecond)
					continue
				}

				w.processMessage(ctx, m)
				_ = w.reader.CommitMessages(ctx, m)
			}
		}
	}()
}

func (w *ProductStockWorker) processMessage(ctx context.Context, m kafka.Message) {
	var event struct {
		EventType string `json:"event_type"`
	}
	if err := json.Unmarshal(m.Value, &event); err != nil {
		logger.Error("❌ ProductStockWorker lỗi deserialize header", "error", err.Error())
		if w.producer != nil {
			_ = w.producer.PublishDeadLetter(ctx, m.Topic, string(m.Key), m.Value, err.Error(), "")
		}
		return
	}

	// Chỉ quan tâm đến sự kiện ORDER_CREATED
	if event.EventType != pkgKafka.EventOrderCreated {
		return
	}

	var payload pkgKafka.OrderCreatedPayload
	if err := json.Unmarshal(m.Value, &payload); err != nil {
		logger.Error("❌ ProductStockWorker lỗi deserialize OrderCreatedPayload", "error", err.Error())
		if w.producer != nil {
			_ = w.producer.PublishDeadLetter(ctx, m.Topic, string(m.Key), m.Value, err.Error(), "")
		}
		return
	}

	traceID := payload.TraceID
	if traceID == "" {
		traceID = fmt.Sprintf("saga-%d", payload.OrderID)
	}
	reqCtx := logger.SetTraceID(ctx, traceID)

	logger.InfoContext(reqCtx, "📦 [SAGA CHOREOGRAPHY] Bắt đầu trừ tồn kho cho đơn hàng",
		"order_id", payload.OrderID,
		"order_code", payload.OrderCode,
		"items_count", len(payload.Items),
	)

	var deductedItems []pkgKafka.OrderItemPayload
	var deductErr error

	// 1. Trừ tồn kho trong PostgreSQL (Database ecom_product_db)
	for _, item := range payload.Items {
		err := w.repo.DeductStock(item.ProductID, item.Quantity)
		if err != nil {
			deductErr = fmt.Errorf("sản phẩm #%d '%s' không đủ tồn kho: %w", item.ProductID, item.ProductName, err)
			break
		}
		deductedItems = append(deductedItems, item)

		// Xóa cache chi tiết sản phẩm trên Redis
		if w.redisClient != nil {
			_ = w.redisClient.Del(reqCtx, fmt.Sprintf("cache:product:%d", item.ProductID))
		}
	}

	// 2. Xử lý kết quả (Compensating Transaction nếu có lỗi)
	if deductErr != nil {
		logger.WarnContext(reqCtx, "⚠️ [SAGA] Trừ tồn kho thất bại, tiến hành Rollback các món đã trừ",
			"order_id", payload.OrderID,
			"error", deductErr.Error(),
		)

		// Hoàn trả lại các món đã trừ trước đó
		for _, item := range deductedItems {
			_ = w.repo.RevertStock(item.ProductID, item.Quantity)
			if w.redisClient != nil {
				_ = w.redisClient.Del(reqCtx, fmt.Sprintf("cache:product:%d", item.ProductID))
			}
		}

		// Bắn kết quả FAILED về Kafka topic stock.events cho Order Service cập nhật
		if w.producer != nil {
			_ = w.producer.PublishStockResult(reqCtx, pkgKafka.StockResultPayload{
				EventType: pkgKafka.EventStockDeductedFailed,
				OrderID:   payload.OrderID,
				OrderCode: payload.OrderCode,
				Success:   false,
				Reason:    deductErr.Error(),
				Items:     payload.Items,
				TraceID:   traceID,
				Timestamp: time.Now(),
			})
		}
		return
	}

	// 3. Trừ tồn kho thành công 100% -> Bắn kết quả SUCCESS về Kafka topic stock.events
	logger.InfoContext(reqCtx, "✅ [SAGA] Trừ tồn kho Database thành công cho toàn bộ sản phẩm",
		"order_id", payload.OrderID,
		"order_code", payload.OrderCode,
	)

	if w.producer != nil {
		_ = w.producer.PublishStockResult(reqCtx, pkgKafka.StockResultPayload{
			EventType: pkgKafka.EventStockDeductedSuccess,
			OrderID:   payload.OrderID,
			OrderCode: payload.OrderCode,
			Success:   true,
			Items:     payload.Items,
			TraceID:   traceID,
			Timestamp: time.Now(),
		})
	}
}

func (w *ProductStockWorker) Close() error {
	if w.reader != nil {
		return w.reader.Close()
	}
	return nil
}
