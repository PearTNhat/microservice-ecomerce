package worker

import (
	"context"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/pkg/logger"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type ProductViewPayload struct {
	ProductID uint      `json:"product_id"`
	UserID    string    `json:"user_id,omitempty"`
	TraceID   string    `json:"trace_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

type ProductViewWorker struct {
	reader      *kafka.Reader
	repo        domain.ProductRepository
	redisClient *redis.Client
	buffer      map[uint]int64
	mu          sync.Mutex
	flushTicker *time.Ticker
	batchSize   int
	flushPeriod time.Duration
}

func NewProductViewWorker(brokers []string, topic string, repo domain.ProductRepository, rClient *redis.Client) *ProductViewWorker {
	if len(brokers) == 0 {
		return nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        "product-view-counter-group",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second, // Auto commit sau 1s
	})

	return &ProductViewWorker{
		reader:      reader,
		repo:        repo,
		redisClient: rClient,
		buffer:      make(map[uint]int64),
		flushPeriod: 5 * time.Second, // Gom batch 5 giây 1 lần
		batchSize:   20,              // Hoặc gom đủ 20 events là xả
	}
}

// Start khởi chạy tiến trình Consumer gom batch ngầm
func (w *ProductViewWorker) Start(ctx context.Context) {
	if w.reader == nil {
		logger.Warn("⚠️ Kafka reader rỗng, không khởi chạy ProductViewWorker")
		return
	}

	logger.Info("🚀 Khởi chạy Kafka View Consumer Worker (Gom batch lượt xem mỗi 5s)...")

	w.flushTicker = time.NewTicker(w.flushPeriod)

	// Goroutine 1: Lắng nghe và gom dữ liệu từ Kafka
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := w.reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					time.Sleep(200 * time.Millisecond)
					continue
				}

				var payload ProductViewPayload
				if err := json.Unmarshal(msg.Value, &payload); err == nil && payload.ProductID > 0 {
					w.mu.Lock()
					w.buffer[payload.ProductID]++
					currentCount := len(w.buffer)
					w.mu.Unlock()

					// Nếu gom đủ batchSize thì xả ngay
					if currentCount >= w.batchSize {
						w.flush(ctx)
					}
				}

				// Commit message
				_ = w.reader.CommitMessages(ctx, msg)
			}
		}
	}()

	// Goroutine 2: Tự động xả định kỳ theo chu kỳ 5 giây
	go func() {
		for {
			select {
			case <-ctx.Done():
				w.flush(context.Background()) // Xả nốt dữ liệu còn sót lại trước khi tắt
				return
			case <-w.flushTicker.C:
				w.flush(ctx)
			}
		}
	}()
}

// flush thực hiện ghi toàn bộ lượt xem gom được vào PostgreSQL & Xóa Cache Redis
func (w *ProductViewWorker) flush(ctx context.Context) {
	w.mu.Lock()
	if len(w.buffer) == 0 {
		w.mu.Unlock()
		return
	}

	// Copy dữ liệu ra để giải phóng lock nhanh
	toFlush := make(map[uint]int64, len(w.buffer))
	for k, v := range w.buffer {
		toFlush[k] = v
	}
	w.buffer = make(map[uint]int64)
	w.mu.Unlock()

	var totalViews int64 = 0
	for _, count := range toFlush {
		totalViews += count
	}

	// 1. Cập nhật thẳng vào PostgreSQL theo lô trong 1 Transaction
	err := w.repo.BatchIncrementViews(toFlush)
	if err != nil {
		logger.Error("❌ Lỗi khi cập nhật batch views vào Database", "error", err.Error())
		return
	}

	// 2. Xóa Cache Redis của các sản phẩm này để lấy lượt xem mới nhất
	if w.redisClient != nil {
		for prodID := range toFlush {
			cacheKey := fmt.Sprintf("product:detail:%d", prodID)
			_ = w.redisClient.Del(ctx, cacheKey).Err()
		}
	}

	logger.Info("⚡ [KAFKA WORKER] Đã đồng bộ batch views vào PostgreSQL",
		"products_updated", len(toFlush),
		"total_views_added", totalViews,
	)
}

func (w *ProductViewWorker) Close() error {
	if w.flushTicker != nil {
		w.flushTicker.Stop()
	}
	if w.reader != nil {
		return w.reader.Close()
	}
	return nil
}
