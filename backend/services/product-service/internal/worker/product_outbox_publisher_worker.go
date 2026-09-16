package worker

import (
	"context"
	"fmt"
	"time"

	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/services/product-service/internal/domain"
)

// ProductOutboxPublisherWorker quét định kỳ product_outbox_events và publish lên Kafka với cơ chế Lease Lock (18.5)
type ProductOutboxPublisherWorker struct {
	outboxRepo    domain.ProductOutboxRepository
	kafkaProducer pkgKafka.OrderKafkaProducer
	workerID      string
	interval      time.Duration
}

func NewProductOutboxPublisherWorker(
	outboxRepo domain.ProductOutboxRepository,
	kafkaProducer pkgKafka.OrderKafkaProducer,
	workerID string,
) *ProductOutboxPublisherWorker {
	if workerID == "" {
		workerID = fmt.Sprintf("prod-outbox-pub-%d", time.Now().UnixNano())
	}
	return &ProductOutboxPublisherWorker{
		outboxRepo:    outboxRepo,
		kafkaProducer: kafkaProducer,
		workerID:      workerID,
		interval:      200 * time.Millisecond,
	}
}

func (w *ProductOutboxPublisherWorker) Start(ctx context.Context) {
	if w.kafkaProducer == nil || w.outboxRepo == nil {
		return
	}

	logger.Info("📬 [PRODUCT OUTBOX PUBLISHER] Bắt đầu quét product_outbox_events với Lease Lock", "worker_id", w.workerID)

	go func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		reclaimTicker := time.NewTicker(1 * time.Minute)
		defer reclaimTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Info("🛑 ProductOutboxPublisherWorker nhận tín hiệu dừng")
				return
			case <-reclaimTicker.C:
				_ = w.outboxRepo.ReclaimStaleProcessing(time.Now())
			case <-ticker.C:
				w.processBatch(ctx)
			}
		}
	}()
}

func (w *ProductOutboxPublisherWorker) processBatch(ctx context.Context) {
	events, err := w.outboxRepo.ClaimPendingBatch(50, w.workerID, 30*time.Second)
	if err != nil || len(events) == 0 {
		return
	}

	for _, event := range events {
		pubErr := w.kafkaProducer.PublishRaw(ctx, event.Topic, event.PartitionKey, []byte(event.Payload))
		if pubErr != nil {
			logger.ErrorContext(ctx, "❌ [PRODUCT OUTBOX] Lỗi publish sự kiện lên Kafka",
				"event_id", event.ID,
				"topic", event.Topic,
				"error", pubErr.Error(),
			)
			// Exponential backoff
			attempts := event.Attempts
			if attempts > 10 {
				attempts = 10
			}
			retryIn := time.Duration(1<<uint(attempts)) * time.Second
			if retryIn > 5*time.Minute {
				retryIn = 5 * time.Minute
			}
			_ = w.outboxRepo.MarkFailed(event.ID, w.workerID, retryIn)
			continue
		}

		_ = w.outboxRepo.MarkPublished(event.ID, w.workerID)
	}
}
