package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/services/product-service/internal/domain"
	"ecomerce-service/services/product-service/internal/repository"

	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

type FlashSaleConfirmationConsumer struct {
	reader             *kafka.Reader
	db                 *gorm.DB
	stockAllocRepo     domain.StockAllocationRepository
	processedEventRepo domain.ProcessedEventRepository
	producer           pkgKafka.OrderKafkaProducer
}

func NewFlashSaleConfirmationConsumer(
	brokers []string,
	db *gorm.DB,
	stockAllocRepo domain.StockAllocationRepository,
	processedEventRepo domain.ProcessedEventRepository,
	producer pkgKafka.OrderKafkaProducer,
) *FlashSaleConfirmationConsumer {
	if len(brokers) == 0 {
		return nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          pkgKafka.TopicFlashSaleConfirmed,
		GroupID:        pkgKafka.ConsumerGroupProductFSConfirmation,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})

	return &FlashSaleConfirmationConsumer{
		reader:             reader,
		db:                 db,
		stockAllocRepo:     stockAllocRepo,
		processedEventRepo: processedEventRepo,
		producer:           producer,
	}
}

func (c *FlashSaleConfirmationConsumer) Start(ctx context.Context) {
	if c.reader == nil {
		return
	}

	logger.Info("⚡ [KAFKA FLASH SALE] Product Confirmation Consumer đang lắng nghe topic flashsale.confirmed...",
		"group_id", pkgKafka.ConsumerGroupProductFSConfirmation,
	)

	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("🛑 FlashSaleConfirmationConsumer nhận tín hiệu dừng")
				_ = c.reader.Close()
				return
			default:
				m, err := c.reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					time.Sleep(500 * time.Millisecond)
					continue
				}

				if err := c.processMessage(ctx, m); err != nil {
					// Nếu lỗi retryable (DB, network), không commit offset, chờ backoff
					logger.Error("❌ FlashSaleConfirmationConsumer lỗi xử lý message, retry sau", "error", err.Error())
					time.Sleep(1 * time.Second)
					continue
				}

				// Xử lý thành công hoặc đã đưa sang DLT -> commit offset
				_ = c.reader.CommitMessages(ctx, m)
			}
		}
	}()
}

func (c *FlashSaleConfirmationConsumer) processMessage(ctx context.Context, m kafka.Message) error {
	var payload pkgKafka.FlashSaleOrderConfirmedPayload
	if err := json.Unmarshal(m.Value, &payload); err != nil {
		logger.Error("❌ FlashSaleConfirmationConsumer: Lỗi parse JSON payload", "error", err.Error())
		if c.producer != nil {
			_ = c.producer.PublishDeadLetter(ctx, m.Topic, string(m.Key), m.Value, "Invalid JSON payload: "+err.Error(), "")
		}
		return nil // Commit poison message sau khi DLT
	}

	traceID := payload.TraceID
	if traceID == "" {
		traceID = fmt.Sprintf("fs-conf-%s", payload.EventID)
	}
	reqCtx := logger.SetTraceID(ctx, traceID)

	// Validate payload
	if payload.EventID == "" || payload.CampaignID == 0 || payload.ProductID == 0 || payload.Quantity <= 0 {
		errMsg := fmt.Sprintf("Invalid confirmation payload: event_id=%s, camp=%d, prod=%d, qty=%d",
			payload.EventID, payload.CampaignID, payload.ProductID, payload.Quantity)
		logger.ErrorContext(reqCtx, "❌ FlashSaleConfirmationConsumer: Dữ liệu payload không hợp lệ", "error", errMsg)
		if c.producer != nil {
			_ = c.producer.PublishDeadLetter(reqCtx, m.Topic, string(m.Key), m.Value, errMsg, traceID)
		}
		return nil // Poison message, bỏ qua sau khi DLT
	}

	// Thực hiện cập nhật sổ cái trong 1 Transaction của Product DB
	dbErr := c.db.Transaction(func(tx *gorm.DB) error {
		// 1. Chèn vào processed_events để đảm bảo Effectively-Once
		_, insErr := c.processedEventRepo.InsertIfNew(tx, "product-flashsale-confirmation", payload.EventID)
		if insErr != nil {
			if errors.Is(insErr, repository.ErrDuplicateEvent) {
				logger.InfoContext(reqCtx, "🔁 [IDEMPOTENT] Sự kiện FLASH_SALE_ORDER_CONFIRMED đã được xử lý trước đó",
					"event_id", payload.EventID,
					"campaign_id", payload.CampaignID,
					"product_id", payload.ProductID,
				)
				return insErr
			}
			return insErr
		}

		// 2. Tăng sold_quantity trên sổ cái phân bổ
		if err := c.stockAllocRepo.IncrementSoldQuantityTx(tx, payload.CampaignID, payload.ProductID, payload.Quantity); err != nil {
			return err
		}

		return nil
	})

	if dbErr != nil {
		if errors.Is(dbErr, repository.ErrDuplicateEvent) {
			// Đã xử lý rồi -> Coi như thành công để commit offset
			return nil
		}

		// Kiểm tra nếu là lỗi vi phạm Invariant (vượt quá allocation) -> Ghi DLT và cảnh báo
		logger.ErrorContext(reqCtx, "❌ FlashSaleConfirmationConsumer: Gặp lỗi khi cập nhật sổ cái",
			"event_id", payload.EventID,
			"campaign_id", payload.CampaignID,
			"product_id", payload.ProductID,
			"error", dbErr.Error(),
		)

		// Nếu lỗi do logic nghiệp vụ (vượt quá tồn kho phân bổ) -> Fatal -> Ghi DLT để tránh kẹt partition
		if isInvariantViolation(dbErr) {
			if c.producer != nil {
				_ = c.producer.PublishDeadLetter(reqCtx, m.Topic, string(m.Key), m.Value, dbErr.Error(), traceID)
			}
			return nil
		}

		// Còn nếu là lỗi DB kết nối/hạ tầng -> trả về error để vòng lặp ngoài retry
		return dbErr
	}

	logger.InfoContext(reqCtx, "✅ [FLASH SALE LEDGER] Đã cập nhật sold_quantity thành công trên Product DB",
		"event_id", payload.EventID,
		"campaign_id", payload.CampaignID,
		"product_id", payload.ProductID,
		"quantity", payload.Quantity,
		"order_id", payload.OrderID,
	)

	return nil
}

func isInvariantViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return msg == "không thể tăng số lượng đã bán: vượt quá số lượng phân bổ hoặc không tìm thấy allocation"
}

func (c *FlashSaleConfirmationConsumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}
