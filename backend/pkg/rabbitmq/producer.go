package rabbitmq

import (
	"context"
	"ecomerce-service/pkg/logger"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeOrdersTopic = "ecom.orders.topic"
	ExchangeOrdersDLX   = "ecom.orders.dlx"
	QueueOrderEmail     = "order.email.queue"
	QueueOrderDLQ       = "order.dead_letter.queue"

	RoutingOrderCreated   = "order.created"
	RoutingOrderPaid      = "order.paid"
	RoutingOrderCancelled = "order.cancelled"
	RoutingOrderDLQ       = "order.dead_letter"
)

// OrderItemEventPayload chứa thông tin từng món hàng trong sự kiện
type OrderItemEventPayload struct {
	ProductID   uint    `json:"product_id"`
	ProductName string  `json:"product_name"`
	Price       float64 `json:"price"`
	Quantity    int     `json:"quantity"`
	Subtotal    float64 `json:"subtotal"`
}

// OrderCreatedPayload chứa thông tin chi tiết đơn hàng được tạo thành công
type OrderCreatedPayload struct {
	OrderID         uint                    `json:"order_id"`
	UserID          string                  `json:"user_id"`
	CustomerEmail   string                  `json:"customer_email"`
	CustomerName    string                  `json:"customer_name"`
	CustomerPhone   string                  `json:"customer_phone"`
	ShippingAddress string                  `json:"shipping_address"`
	TotalAmount     float64                 `json:"total_amount"`
	PaymentMethod   string                  `json:"payment_method"`
	Items           []OrderItemEventPayload `json:"items"`
	TraceID         string                  `json:"trace_id,omitempty"`
	CreatedAt       time.Time               `json:"created_at"`
}

// OrderEventProducer interface phát các sự kiện liên quan đến đơn hàng
type OrderEventProducer interface {
	PublishOrderCreated(ctx context.Context, payload OrderCreatedPayload) error
	PublishOrderPaid(ctx context.Context, orderID uint, amount float64) error
	PublishOrderCancelled(ctx context.Context, orderID uint, reason string) error
	Close() error
}

type rabbitMQProducer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	amqpURL string
}

// NewRabbitMQProducer khởi tạo kết nối RabbitMQ và khai báo Topic Exchange + Queues + DLX
func NewRabbitMQProducer(amqpURL string) OrderEventProducer {
	if amqpURL == "" {
		logger.Warn("⚠️ RabbitMQ URL rỗng, sử dụng Noop Producer")
		return &noopRabbitMQProducer{}
	}

	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		logger.Warn("⚠️ Không thể kết nối RabbitMQ (sử dụng Noop Producer fallback)", "url", amqpURL, "error", err.Error())
		return &noopRabbitMQProducer{}
	}

	ch, err := conn.Channel()
	if err != nil {
		logger.Error("❌ Không thể mở RabbitMQ channel", "error", err.Error())
		conn.Close()
		return &noopRabbitMQProducer{}
	}

	// 1. Khai báo Dead Letter Exchange (DLX) & Queue
	err = ch.ExchangeDeclare(
		ExchangeOrdersDLX,
		"direct",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	)
	if err != nil {
		logger.Error("❌ Không thể khai báo DLX Exchange", "error", err.Error())
	}

	_, err = ch.QueueDeclare(
		QueueOrderDLQ,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		logger.Error("❌ Không thể khai báo DLQ Queue", "error", err.Error())
	}

	_ = ch.QueueBind(QueueOrderDLQ, RoutingOrderDLQ, ExchangeOrdersDLX, false, nil)

	// 2. Khai báo Main Topic Exchange
	err = ch.ExchangeDeclare(
		ExchangeOrdersTopic,
		"topic",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	)
	if err != nil {
		logger.Error("❌ Không thể khai báo Orders Topic Exchange", "error", err.Error())
	}

	// 3. Khai báo Email Queue gắn kèm Dead Letter Exchange
	queueArgs := amqp.Table{
		"x-dead-letter-exchange":    ExchangeOrdersDLX,
		"x-dead-letter-routing-key": RoutingOrderDLQ,
		"x-message-ttl":             int32(86400000), // 24 hours TTL
	}

	_, err = ch.QueueDeclare(
		QueueOrderEmail,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		queueArgs,
	)
	if err != nil {
		logger.Error("❌ Không thể khai báo Email Queue", "error", err.Error())
	}

	_ = ch.QueueBind(QueueOrderEmail, RoutingOrderCreated, ExchangeOrdersTopic, false, nil)

	logger.Info("✅ Đã khởi tạo RabbitMQ Producer & Exchanges/Queues thành công", "url", amqpURL)

	return &rabbitMQProducer{
		conn:    conn,
		channel: ch,
		amqpURL: amqpURL,
	}
}

func (p *rabbitMQProducer) PublishOrderCreated(ctx context.Context, payload OrderCreatedPayload) error {
	if payload.CreatedAt.IsZero() {
		payload.CreatedAt = time.Now()
	}
	if payload.TraceID == "" {
		payload.TraceID = logger.GetTraceID(ctx)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("lỗi serialize payload: %w", err)
	}

	msg := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent, // Tin nhắn lưu trên đĩa (không mất khi broker restart)
		Timestamp:    time.Now(),
		Body:         data,
		Headers: amqp.Table{
			"trace_id": payload.TraceID,
			"event":    RoutingOrderCreated,
		},
	}

	err = p.channel.PublishWithContext(
		ctx,
		ExchangeOrdersTopic,
		RoutingOrderCreated,
		false, // mandatory
		false, // immediate
		msg,
	)
	if err != nil {
		logger.Error("❌ Lỗi publish event order.created tới RabbitMQ",
			"order_id", payload.OrderID,
			"error", err.Error(),
		)
		return err
	}

	logger.Info("🐰 [RABBITMQ] Đã phát sự kiện order.created",
		"order_id", payload.OrderID,
		"total_amount", payload.TotalAmount,
		"customer_email", payload.CustomerEmail,
	)

	return nil
}

func (p *rabbitMQProducer) PublishOrderPaid(ctx context.Context, orderID uint, amount float64) error {
	payload := map[string]interface{}{
		"order_id":  orderID,
		"amount":    amount,
		"trace_id":  logger.GetTraceID(ctx),
		"timestamp": time.Now(),
	}
	data, _ := json.Marshal(payload)

	return p.channel.PublishWithContext(
		ctx,
		ExchangeOrdersTopic,
		RoutingOrderPaid,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         data,
		},
	)
}

func (p *rabbitMQProducer) PublishOrderCancelled(ctx context.Context, orderID uint, reason string) error {
	payload := map[string]interface{}{
		"order_id":  orderID,
		"reason":    reason,
		"trace_id":  logger.GetTraceID(ctx),
		"timestamp": time.Now(),
	}
	data, _ := json.Marshal(payload)

	return p.channel.PublishWithContext(
		ctx,
		ExchangeOrdersTopic,
		RoutingOrderCancelled,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         data,
		},
	)
}

func (p *rabbitMQProducer) Close() error {
	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

// noopRabbitMQProducer dùng khi test hoặc không có RabbitMQ
type noopRabbitMQProducer struct{}

func (n *noopRabbitMQProducer) PublishOrderCreated(ctx context.Context, payload OrderCreatedPayload) error {
	return nil
}
func (n *noopRabbitMQProducer) PublishOrderPaid(ctx context.Context, orderID uint, amount float64) error {
	return nil
}
func (n *noopRabbitMQProducer) PublishOrderCancelled(ctx context.Context, orderID uint, reason string) error {
	return nil
}
func (n *noopRabbitMQProducer) Close() error {
	return nil
}

func NewNoopRabbitMQProducer() OrderEventProducer {
	return &noopRabbitMQProducer{}
}
