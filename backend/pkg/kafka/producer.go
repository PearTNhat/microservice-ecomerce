package kafka

import (
	"context"
	"ecomerce-service/pkg/logger"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// ProductViewedPayload chứa thông tin sự kiện người dùng xem chi tiết sản phẩm
type ProductViewedPayload struct {
	ProductID uint      `json:"product_id"`
	UserID    string    `json:"user_id,omitempty"`
	TraceID   string    `json:"trace_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// EventProducer interface để các service bắn event
type EventProducer interface {
	PublishProductViewed(ctx context.Context, productID uint, userID string) error
	Close() error
}

type kafkaProducer struct {
	writer *kafka.Writer
	topic  string
}

func NewKafkaProducer(brokers []string, topic string) EventProducer {
	if len(brokers) == 0 {
		logger.Warn("⚠️ Kafka brokers rỗng, sử dụng Noop Producer")
		return &noopProducer{}
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		BatchTimeout:           50 * time.Millisecond,
		Async:                  true, // Bắn bất đồng bộ không chặn request
		AllowAutoTopicCreation: true, // Tự động tạo topic nếu broker cho phép
	}

	logger.Info("✅ Đã khởi tạo Kafka Producer", "topic", topic, "brokers", brokers)

	return &kafkaProducer{
		writer: writer,
		topic:  topic,
	}
}

func (p *kafkaProducer) PublishProductViewed(ctx context.Context, productID uint, userID string) error {
	traceID := logger.GetTraceID(ctx)

	payload := ProductViewedPayload{
		ProductID: productID,
		UserID:    userID,
		TraceID:   traceID,
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("product_%d", productID)),
		Value: data,
		Time:  time.Now(),
	}

	// Gửi ngầm không chặn luồng chính
	go func() {
		err := p.writer.WriteMessages(context.Background(), msg)
		if err != nil {
			logger.Warn("⚠️ Không thể gửi event tới Kafka (Kafka có thể chưa sẵn sàng)",
				"topic", p.topic,
				"product_id", productID,
				"error", err.Error(),
			)
		}
	}()

	return nil
}

func (p *kafkaProducer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}

// noopProducer dùng khi không cấu hình Kafka hoặc chạy test
type noopProducer struct{}

func (n *noopProducer) PublishProductViewed(ctx context.Context, productID uint, userID string) error {
	return nil
}

func (n *noopProducer) Close() error {
	return nil
}

func NewNoopProducer() EventProducer {
	return &noopProducer{}
}
