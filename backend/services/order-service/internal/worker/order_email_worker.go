package worker

import (
	"context"
	"ecomerce-service/pkg/config"
	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"gopkg.in/gomail.v2"
)

type OrderEmailWorker struct {
	reader    *kafka.Reader
	producer  pkgKafka.OrderKafkaProducer
	appConfig config.AppConfig
}

func NewOrderEmailWorker(brokers []string, cfg config.AppConfig, producer pkgKafka.OrderKafkaProducer) *OrderEmailWorker {
	if len(brokers) == 0 {
		return nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          pkgKafka.TopicOrderEvents,
		GroupID:        pkgKafka.ConsumerGroupOrderEmail,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})

	return &OrderEmailWorker{
		reader:    reader,
		producer:  producer,
		appConfig: cfg,
	}
}

// Start khởi chạy tiến trình Worker lắng nghe Kafka topic order.events để gửi email hóa đơn
func (w *OrderEmailWorker) Start(ctx context.Context) {
	if w.reader == nil {
		return
	}

	logger.Info("📧 [KAFKA WORKER] Đang lắng nghe topic order.events để gửi email hóa đơn...",
		"group_id", pkgKafka.ConsumerGroupOrderEmail,
	)

	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("🛑 OrderEmailWorker nhận tín hiệu dừng")
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

				var event struct {
					EventType string `json:"event_type"`
				}
				if err := json.Unmarshal(m.Value, &event); err == nil && event.EventType == pkgKafka.EventOrderCreated {
					var payload pkgKafka.OrderCreatedPayload
					if err := json.Unmarshal(m.Value, &payload); err == nil {
						_ = w.sendInvoiceEmail(payload)
					} else if w.producer != nil {
						_ = w.producer.PublishDeadLetter(ctx, m.Topic, string(m.Key), m.Value, err.Error(), "")
					}
				}

				_ = w.reader.CommitMessages(ctx, m)
			}
		}
	}()
}

func (w *OrderEmailWorker) sendInvoiceEmail(payload pkgKafka.OrderCreatedPayload) error {
	if w.appConfig.SMTPHost == "" || w.appConfig.SMTPUser == "" {
		logger.Info("📧 [SIMULATED EMAIL] Không cấu hình SMTP thực tế, giả lập gửi email thành công qua Kafka",
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
				Đơn hàng được xử lý tự động bởi hệ thống Microservices & Apache Kafka.
			</p>
		</div>
	`, payload.CustomerName, payload.OrderID, itemsHTML, payload.TotalAmount, payload.ShippingAddress, payload.CustomerPhone, payload.PaymentMethod)

	m := gomail.NewMessage()
	m.SetHeader("From", w.appConfig.SMTPUser)
	m.SetHeader("To", payload.CustomerEmail)
	m.SetHeader("Subject", fmt.Sprintf("🎉 [E-COMMERCE] Xác nhận đơn hàng #%d thành công", payload.OrderID))
	m.SetBody("text/html", body)

	d := gomail.NewDialer(w.appConfig.SMTPHost, w.appConfig.SMTPPort, w.appConfig.SMTPUser, w.appConfig.SMTPPass)

	err := d.DialAndSend(m)
	if err != nil {
		logger.Error("❌ Gửi email hóa đơn thất bại", "error", err.Error(), "to", payload.CustomerEmail)
		return err
	}

	logger.Info("✅ Đã gửi email xác nhận đơn hàng thành công qua Kafka", "order_id", payload.OrderID, "to", payload.CustomerEmail)
	return nil
}

func (w *OrderEmailWorker) Close() error {
	if w.reader != nil {
		return w.reader.Close()
	}
	return nil
}
