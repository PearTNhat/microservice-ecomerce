package kafka

import (
	"context"
	"ecomerce-service/pkg/logger"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// OrderKafkaProducer interface định nghĩa toàn bộ hành động phát event cho Order và Saga
type OrderKafkaProducer interface {
	PublishOrderCreated(ctx context.Context, payload OrderCreatedPayload) error
	PublishOrderPaid(ctx context.Context, orderID uint, amount float64) error
	PublishOrderCancelled(ctx context.Context, orderID uint, reason string) error
	PublishFlashSaleOrderTask(ctx context.Context, payload FlashSaleOrderTaskPayload) error
	PublishStockResult(ctx context.Context, payload StockResultPayload) error
	PublishMixedOrderStockRequest(ctx context.Context, payload MixedOrderStockRequestPayload) error
	PublishMixedOrderStockResult(ctx context.Context, payload MixedOrderStockResultPayload) error
	PublishMixedOrderStockCompensate(ctx context.Context, payload MixedOrderStockCompensatePayload) error
	PublishMixedOrderStockCompensateResult(ctx context.Context, payload MixedOrderStockCompensateResultPayload) error
	PublishDeadLetter(ctx context.Context, originalTopic string, key string, rawPayload []byte, errReason string, traceID string) error
	PublishRaw(ctx context.Context, topic string, key string, payload []byte) error
	Close() error
}

type orderKafkaProducer struct {
	writers map[string]*kafka.Writer
	brokers []string
}

// NewOrderKafkaProducer khởi tạo Producer với kết nối tới Kafka Broker
func NewOrderKafkaProducer(brokers []string) OrderKafkaProducer {
	if len(brokers) == 0 {
		logger.Warn("⚠️ Kafka brokers rỗng, sử dụng Noop Order Producer")
		return &noopOrderKafkaProducer{}
	}

	createWriter := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{}, // Phân phối partition theo key (ví dụ OrderID)
			BatchTimeout:           10 * time.Millisecond,
			Async:                  false, // Sync write để đảm bảo tin nhắn đã vào Kafka
			AllowAutoTopicCreation: true,
			RequiredAcks:           kafka.RequireOne, // Leader ghi thành công là xong
		}
	}

	writers := map[string]*kafka.Writer{
		TopicOrderEvents:               createWriter(TopicOrderEvents),
		TopicStockEvents:               createWriter(TopicStockEvents),
		TopicFlashSaleOrders:           createWriter(TopicFlashSaleOrders),
		TopicFlashSaleConfirmed:        createWriter(TopicFlashSaleConfirmed),
		TopicMixedOrderStockRequest:    createWriter(TopicMixedOrderStockRequest),
		TopicMixedOrderStockResult:     createWriter(TopicMixedOrderStockResult),
		TopicMixedOrderStockCompensate:       createWriter(TopicMixedOrderStockCompensate),
		TopicMixedOrderStockCompensateResult: createWriter(TopicMixedOrderStockCompensateResult),
		TopicOrdersDLT:                       createWriter(TopicOrdersDLT),
	}

	logger.Info("✅ [KAFKA] Đã khởi tạo Order Kafka Producer", "brokers", brokers)

	return &orderKafkaProducer{
		writers: writers,
		brokers: brokers,
	}
}

func (p *orderKafkaProducer) PublishOrderCreated(ctx context.Context, payload OrderCreatedPayload) error {
	payload.EventType = EventOrderCreated
	if payload.CreatedAt.IsZero() {
		payload.CreatedAt = time.Now()
	}
	if payload.TraceID == "" {
		payload.TraceID = logger.GetTraceID(ctx)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("lỗi serialize OrderCreatedPayload: %w", err)
	}

	w, ok := p.writers[TopicOrderEvents]
	if !ok {
		return fmt.Errorf("writer cho topic %s chưa khởi tạo", TopicOrderEvents)
	}

	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("order_%d", payload.OrderID)),
		Value: data,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(EventOrderCreated)},
			{Key: "trace_id", Value: []byte(payload.TraceID)},
		},
	}

	err = w.WriteMessages(ctx, msg)
	if err != nil {
		logger.ErrorContext(ctx, "❌ [KAFKA] Lỗi publish order.created", "order_id", payload.OrderID, "error", err.Error())
		return err
	}

	logger.InfoContext(ctx, "📢 [KAFKA] Đã publish event order.created thành công",
		"order_id", payload.OrderID,
		"order_code", payload.OrderCode,
		"total_amount", payload.TotalAmount,
	)

	return nil
}

func (p *orderKafkaProducer) PublishOrderPaid(ctx context.Context, orderID uint, amount float64) error {
	w, ok := p.writers[TopicOrderEvents]
	if !ok {
		return fmt.Errorf("writer cho topic %s chưa khởi tạo", TopicOrderEvents)
	}

	payload := map[string]interface{}{
		"event_type": EventOrderPaid,
		"order_id":   orderID,
		"amount":     amount,
		"timestamp":  time.Now(),
		"trace_id":   logger.GetTraceID(ctx),
	}

	data, _ := json.Marshal(payload)
	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("order_%d", orderID)),
		Value: data,
		Time:  time.Now(),
	}

	return w.WriteMessages(ctx, msg)
}

func (p *orderKafkaProducer) PublishOrderCancelled(ctx context.Context, orderID uint, reason string) error {
	w, ok := p.writers[TopicOrderEvents]
	if !ok {
		return fmt.Errorf("writer cho topic %s chưa khởi tạo", TopicOrderEvents)
	}

	payload := map[string]interface{}{
		"event_type": EventOrderCancelled,
		"order_id":   orderID,
		"reason":     reason,
		"timestamp":  time.Now(),
		"trace_id":   logger.GetTraceID(ctx),
	}

	data, _ := json.Marshal(payload)
	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("order_%d", orderID)),
		Value: data,
		Time:  time.Now(),
	}

	return w.WriteMessages(ctx, msg)
}

func (p *orderKafkaProducer) PublishFlashSaleOrderTask(ctx context.Context, payload FlashSaleOrderTaskPayload) error {
	if payload.CreatedAt.IsZero() {
		payload.CreatedAt = time.Now()
	}
	if payload.TraceID == "" {
		payload.TraceID = logger.GetTraceID(ctx)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("lỗi serialize FlashSaleOrderTaskPayload: %w", err)
	}

	w, ok := p.writers[TopicFlashSaleOrders]
	if !ok {
		return fmt.Errorf("writer cho topic %s chưa khởi tạo", TopicFlashSaleOrders)
	}

	msg := kafka.Message{
		Key:   []byte(payload.OrderToken),
		Value: data,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "trace_id", Value: []byte(payload.TraceID)},
		},
	}

	err = w.WriteMessages(ctx, msg)
	if err != nil {
		logger.ErrorContext(ctx, "❌ [KAFKA] Lỗi publish flashsale order task", "token", payload.OrderToken, "error", err.Error())
		return err
	}

	logger.InfoContext(ctx, "⚡ [KAFKA] Đã publish FlashSale order task vào Kafka",
		"token", payload.OrderToken,
		"product_id", payload.ProductID,
	)

	return nil
}

func (p *orderKafkaProducer) PublishStockResult(ctx context.Context, payload StockResultPayload) error {
	if payload.Success {
		payload.EventType = EventStockDeductedSuccess
	} else {
		payload.EventType = EventStockDeductedFailed
	}
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now()
	}
	if payload.TraceID == "" {
		payload.TraceID = logger.GetTraceID(ctx)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("lỗi serialize StockResultPayload: %w", err)
	}

	w, ok := p.writers[TopicStockEvents]
	if !ok {
		return fmt.Errorf("writer cho topic %s chưa khởi tạo", TopicStockEvents)
	}

	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("order_%d", payload.OrderID)),
		Value: data,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(payload.EventType)},
			{Key: "trace_id", Value: []byte(payload.TraceID)},
		},
	}

	err = w.WriteMessages(ctx, msg)
	if err != nil {
		logger.ErrorContext(ctx, "❌ [KAFKA] Lỗi publish stock result", "order_id", payload.OrderID, "success", payload.Success, "error", err.Error())
		return err
	}

	logger.InfoContext(ctx, "📦 [KAFKA] Đã publish StockResult sang topic stock.events",
		"order_id", payload.OrderID,
		"success", payload.Success,
		"reason", payload.Reason,
	)

	return nil
}

func (p *orderKafkaProducer) PublishMixedOrderStockRequest(ctx context.Context, payload MixedOrderStockRequestPayload) error {
	payload.EventType = EventMixedStockDeductRequest
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now()
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("lỗi serialize MixedOrderStockRequestPayload: %w", err)
	}

	key := fmt.Sprintf("mixed-req-%d", payload.OrderID)
	return p.PublishRaw(ctx, TopicMixedOrderStockRequest, key, data)
}

func (p *orderKafkaProducer) PublishMixedOrderStockResult(ctx context.Context, payload MixedOrderStockResultPayload) error {
	payload.EventType = EventMixedStockDeductResult
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now()
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("lỗi serialize MixedOrderStockResultPayload: %w", err)
	}

	key := fmt.Sprintf("mixed-res-%d", payload.OrderID)
	return p.PublishRaw(ctx, TopicMixedOrderStockResult, key, data)
}

func (p *orderKafkaProducer) PublishMixedOrderStockCompensate(ctx context.Context, payload MixedOrderStockCompensatePayload) error {
	payload.EventType = EventMixedStockCompensate
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now()
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("lỗi serialize MixedOrderStockCompensatePayload: %w", err)
	}

	key := fmt.Sprintf("mixed-comp-%d", payload.OrderID)
	return p.PublishRaw(ctx, TopicMixedOrderStockCompensate, key, data)
}

func (p *orderKafkaProducer) PublishMixedOrderStockCompensateResult(ctx context.Context, payload MixedOrderStockCompensateResultPayload) error {
	payload.EventType = EventMixedStockCompensateResult
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now()
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("lỗi serialize MixedOrderStockCompensateResultPayload: %w", err)
	}

	key := fmt.Sprintf("mixed-comp-res-%d", payload.OrderID)
	return p.PublishRaw(ctx, TopicMixedOrderStockCompensateResult, key, data)
}

func (p *orderKafkaProducer) PublishDeadLetter(ctx context.Context, originalTopic string, key string, rawPayload []byte, errReason string, traceID string) error {
	w, ok := p.writers[TopicOrdersDLT]
	if !ok {
		return fmt.Errorf("writer cho topic %s chưa khởi tạo", TopicOrdersDLT)
	}

	dlp := DeadLetterPayload{
		OriginalTopic: originalTopic,
		PartitionKey:  key,
		RawPayload:    string(rawPayload),
		ErrorMessage:  errReason,
		TraceID:       traceID,
		FailedAt:      time.Now(),
	}

	data, _ := json.Marshal(dlp)
	msg := kafka.Message{
		Key:   []byte(key),
		Value: data,
		Time:  time.Now(),
	}

	logger.WarnContext(ctx, "⚠️ [KAFKA DLT] Chuyển tin nhắn lỗi vào Dead Letter Topic",
		"topic", TopicOrdersDLT,
		"original_topic", originalTopic,
		"key", key,
		"error", errReason,
	)

	return w.WriteMessages(ctx, msg)
}

func (p *orderKafkaProducer) PublishRaw(ctx context.Context, topic string, key string, payload []byte) error {
	w, ok := p.writers[topic]
	if !ok {
		// Tạo dynamically writer nếu topic mới
		w = &kafka.Writer{
			Addr:                   kafka.TCP(p.brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			BatchTimeout:           10 * time.Millisecond,
			Async:                  false,
			AllowAutoTopicCreation: true,
			RequiredAcks:           kafka.RequireOne,
		}
		p.writers[topic] = w
	}

	msg := kafka.Message{
		Key:   []byte(key),
		Value: payload,
		Time:  time.Now(),
	}

	return w.WriteMessages(ctx, msg)
}

func (p *orderKafkaProducer) Close() error {
	for _, w := range p.writers {
		if w != nil {
			_ = w.Close()
		}
	}
	return nil
}

// noopOrderKafkaProducer cho trường hợp chạy unit test hoặc không có Kafka
type noopOrderKafkaProducer struct{}

func (n *noopOrderKafkaProducer) PublishOrderCreated(ctx context.Context, payload OrderCreatedPayload) error {
	return nil
}
func (n *noopOrderKafkaProducer) PublishOrderPaid(ctx context.Context, orderID uint, amount float64) error {
	return nil
}
func (n *noopOrderKafkaProducer) PublishOrderCancelled(ctx context.Context, orderID uint, reason string) error {
	return nil
}
func (n *noopOrderKafkaProducer) PublishFlashSaleOrderTask(ctx context.Context, payload FlashSaleOrderTaskPayload) error {
	return nil
}
func (n *noopOrderKafkaProducer) PublishStockResult(ctx context.Context, payload StockResultPayload) error {
	return nil
}
func (n *noopOrderKafkaProducer) PublishMixedOrderStockRequest(ctx context.Context, payload MixedOrderStockRequestPayload) error {
	return nil
}
func (n *noopOrderKafkaProducer) PublishMixedOrderStockResult(ctx context.Context, payload MixedOrderStockResultPayload) error {
	return nil
}
func (n *noopOrderKafkaProducer) PublishMixedOrderStockCompensate(ctx context.Context, payload MixedOrderStockCompensatePayload) error {
	return nil
}
func (n *noopOrderKafkaProducer) PublishMixedOrderStockCompensateResult(ctx context.Context, payload MixedOrderStockCompensateResultPayload) error {
	return nil
}
func (n *noopOrderKafkaProducer) PublishDeadLetter(ctx context.Context, originalTopic string, key string, rawPayload []byte, errReason string, traceID string) error {
	return nil
}
func (n *noopOrderKafkaProducer) PublishRaw(ctx context.Context, topic string, key string, payload []byte) error {
	return nil
}
func (n *noopOrderKafkaProducer) Close() error {
	return nil
}

func NewNoopOrderKafkaProducer() OrderKafkaProducer {
	return &noopOrderKafkaProducer{}
}
