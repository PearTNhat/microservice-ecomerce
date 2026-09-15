package worker

import (
	"context"
	"fmt"
	"time"

	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/services/order-service/internal/domain"
)

type OutboxPublisherWorker struct {
	outboxRepo    domain.OutboxRepository
	kafkaProducer pkgKafka.OrderKafkaProducer
	workerID      string
	interval      time.Duration
}

func NewOutboxPublisherWorker(
	outboxRepo domain.OutboxRepository,
	kafkaProducer pkgKafka.OrderKafkaProducer,
	workerID string,
) *OutboxPublisherWorker {
	if workerID == "" {
		workerID = fmt.Sprintf("outbox-pub-%d", time.Now().UnixNano())
	}
	return &OutboxPublisherWorker{
		outboxRepo:    outboxRepo,
		kafkaProducer: kafkaProducer,
		workerID:      workerID,
		interval:      200 * time.Millisecond,
	}
}

func (w *OutboxPublisherWorker) Start(ctx context.Context) {
	if w.kafkaProducer == nil || w.outboxRepo == nil {
		return
	}

	logger.Info("📬 [OUTBOX PUBLISHER] Bắt đầu quét outbox_events với Lease Lock", "worker_id", w.workerID)

	go func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		reclaimTicker := time.NewTicker(1 * time.Minute)
		defer reclaimTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Info("🛑 OutboxPublisherWorker nhận tín hiệu dừng")
				return
			case <-reclaimTicker.C:
				_ = w.outboxRepo.ReclaimStaleProcessing(time.Now())
			case <-ticker.C:
				w.processBatch(ctx)
			}
		}
	}()
}

func (w *OutboxPublisherWorker) processBatch(ctx context.Context) {
	events, err := w.outboxRepo.ClaimPendingBatch(50, w.workerID, 30*time.Second)
	if err != nil || len(events) == 0 {
		return
	}

	for _, event := range events {
		pubErr := w.kafkaProducer.PublishRaw(ctx, event.Topic, event.PartitionKey, []byte(event.Payload))
		if pubErr != nil {
			logger.ErrorContext(ctx, "❌ [OUTBOX] Lỗi publish sự kiện lên Kafka",
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
