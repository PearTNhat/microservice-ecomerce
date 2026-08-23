package worker

import (
	"context"
	"ecomerce-service/config"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/rabbitmq"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/gomail.v2"
)

type OrderEmailWorker struct {
	amqpURL   string
	conn      *amqp.Connection
	channel   *amqp.Channel
	appConfig config.AppConfig
}

func NewOrderEmailWorker(cfg config.AppConfig) *OrderEmailWorker {
	if cfg.RabbitMQURL == "" {
		return nil
	}

	return &OrderEmailWorker{
		amqpURL:   cfg.RabbitMQURL,
		appConfig: cfg,
	}
}

// Start khởi chạy tiến trình Worker lắng nghe queue gửi email hóa đơn
func (w *OrderEmailWorker) Start(ctx context.Context) error {
	conn, err := amqp.Dial(w.amqpURL)
	if err != nil {
		logger.Warn("⚠️ OrderEmailWorker: Không thể kết nối RabbitMQ (Worker tạm hoãn)", "error", err.Error())
		return err
	}
	w.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		logger.Error("❌ OrderEmailWorker: Không thể mở channel", "error", err.Error())
		conn.Close()
		return err
	}
	w.channel = ch

	// Giới hạn prefetch 5 tin nhắn 1 lúc để tránh nghẽn
	_ = ch.Qos(5, 0, false)

	msgs, err := ch.Consume(
		rabbitmq.QueueOrderEmail,
		"order-email-consumer",
		false, // auto-ack = false (xác nhận thủ công)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		logger.Error("❌ OrderEmailWorker: Không thể đăng ký Consume", "error", err.Error())
		return err
	}

	logger.Info("📧 [RABBITMQ WORKER] Đang lắng nghe hàng đợi order.email.queue để gửi email hóa đơn...")

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}

				var payload rabbitmq.OrderCreatedPayload
				if err := json.Unmarshal(msg.Body, &payload); err != nil {
					logger.Error("❌ OrderEmailWorker: Lỗi deserialize message, đẩy sang DLQ", "error", err.Error())
					_ = msg.Nack(false, false) // Không requeue -> Đẩy vào Dead Letter Queue
					continue
				}

				// Xử lý gửi email xác nhận
				err := w.sendInvoiceEmail(payload)
				if err != nil {
					logger.Error("⚠️ Gửi email hóa đơn thất bại",
						"order_id", payload.OrderID,
						"customer_email", payload.CustomerEmail,
						"error", err.Error(),
					)
					// Nếu lỗi gửi, có thể Nack requeue hoặc Nack vào DLQ tùy chính sách
					_ = msg.Ack(false)
				} else {
					logger.Info("✅ Đã gửi email xác nhận đơn hàng thành công",
						"order_id", payload.OrderID,
						"customer_email", payload.CustomerEmail,
					)
					_ = msg.Ack(false)
				}
			}
		}
	}()

	return nil
}

func (w *OrderEmailWorker) sendInvoiceEmail(payload rabbitmq.OrderCreatedPayload) error {
	if w.appConfig.SMTPHost == "" || w.appConfig.SMTPUser == "" {
		logger.Info("📧 [SIMULATED EMAIL] Không cấu hình SMTP thực tế, giả lập gửi email thành công",
			"to", payload.CustomerEmail,
			"order_id", payload.OrderID,
			"amount", payload.TotalAmount,
		)
		return nil
	}

	itemsHTML := ""
	for _, item := range payload.Items {
		itemsHTML += fmt.Sprintf(`
			<tr>
				<td style="padding: 8px; border-bottom: 1px solid #ddd;">%s</td>
				<td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: center;">%d</td>
				<td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: right;">%.0f đ</td>
				<td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: right; font-weight: bold;">%.0f đ</td>
			</tr>
		`, item.ProductName, item.Quantity, item.Price, item.Subtotal)
	}

	body := fmt.Sprintf(`
		<div style="font-family: Arial, sans-serif; max-width: 600px; margin: auto; padding: 20px; border: 1px solid #eee; border-radius: 8px;">
			<h2 style="color: #2563eb; text-align: center;">XÁC NHẬN ĐƠN HÀNG THÀNH CÔNG</h2>
			<p>Xin chào <strong>%s</strong>,</p>
			<p>Cảm ơn bạn đã đặt hàng tại Điện Máy E-Commerce. Dưới đây là thông tin đơn hàng <strong>#%d</strong> của bạn:</p>
			
			<table style="width: 100%%; border-collapse: collapse; margin: 20px 0;">
				<thead>
					<tr style="background-color: #f3f4f6;">
						<th style="padding: 8px; text-align: left;">Sản phẩm</th>
						<th style="padding: 8px; text-align: center;">SL</th>
						<th style="padding: 8px; text-align: right;">Đơn giá</th>
						<th style="padding: 8px; text-align: right;">Tạm tính</th>
					</tr>
				</thead>
				<tbody>
					%s
				</tbody>
			</table>

			<p style="text-align: right; font-size: 18px; color: #dc2626;"><strong>Tổng thanh toán: %.0f đ</strong></p>
			<hr style="border: none; border-top: 1px solid #eee;" />
			<p><strong>Địa chỉ nhận hàng:</strong> %s</p>
			<p><strong>Số điện thoại:</strong> %s</p>
			<p><strong>Hình thức thanh toán:</strong> %s</p>
			<p style="color: #6b7280; font-size: 13px; text-align: center; margin-top: 30px;">
				Đơn hàng được xử lý tự động bởi hệ thống Microservices & RabbitMQ.
			</p>
		</div>
	`, payload.CustomerName, payload.OrderID, itemsHTML, payload.TotalAmount, payload.ShippingAddress, payload.CustomerPhone, payload.PaymentMethod)

	m := gomail.NewMessage()
	m.SetHeader("From", w.appConfig.SMTPUser)
	m.SetHeader("To", payload.CustomerEmail)
	m.SetHeader("Subject", fmt.Sprintf("🎉 [E-COMMERCE] Xác nhận đơn hàng #%d thành công", payload.OrderID))
	m.SetBody("text/html", body)

	d := gomail.NewDialer(w.appConfig.SMTPHost, w.appConfig.SMTPPort, w.appConfig.SMTPUser, w.appConfig.SMTPPass)

	return d.DialAndSend(m)
}

func (w *OrderEmailWorker) Close() error {
	if w.channel != nil {
		_ = w.channel.Close()
	}
	if w.conn != nil {
		return w.conn.Close()
	}
	return nil
}
