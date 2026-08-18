package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	amqpURL        = "amqp://guest:guest@localhost:5672/"
	exchangeDirect = "ecom.orders.direct"
	exchangeFanout = "ecom.broadcast.fanout"
	exchangeDLX    = "ecom.orders.dlx"
	queueInventory = "order.inventory.queue"
	queueEmail     = "order.email.queue"
	queuePayment   = "order.payment.queue"
	queueDLQ       = "order.dead_letter.queue"
	routingCreated = "order.created"
	routingPaid    = "order.paid"
	routingFailed  = "order.failed"
)

// OrderEvent đại diện cho một đơn hàng trong hệ sinh thái E-Commerce
type OrderEvent struct {
	OrderID    string  `json:"order_id"`
	Customer   string  `json:"customer"`
	Product    string  `json:"product"`
	Amount     float64 `json:"amount"`
	Currency   string  `json:"currency"`
	Action     string  `json:"action"`
	ShouldFail bool    `json:"should_fail,omitempty"`
	CreatedAt  string  `json:"created_at"`
}

func main() {
	args := os.Args[1:]
	mode := "all"
	if len(args) > 0 {
		mode = args[0]
	}

	fmt.Println("==================================================================")
	fmt.Println("🐰 HỌC VIỆN RABBITMQ: TUTORIAL & THỰC HÀNH TƯƠNG TÁC")
	fmt.Println("==================================================================")

	// Khởi tạo hạ tầng Exchange, Queue, Binding và Dead Letter Queue
	setupRabbitMQInfrastructure()

	switch mode {
	case "producer":
		fmt.Println("🚀 Chế độ: [PRODUCER CHUYÊN BIỆT] - Bắn sự kiện đơn hàng vào RabbitMQ")
		runDedicatedProducer()

	case "consumer":
		fmt.Println("🎧 Chế độ: [CONSUMER CHUYÊN BIỆT] - Lắng nghe và xử lý (kèm ACK / NACK)")
		runDedicatedConsumer()

	case "fanout":
		fmt.Println("📢 Chế độ: [FANOUT DEMO] - Phát thanh tin nhắn đến toàn bộ hàng đợi")
		runFanoutDemo()

	default:
		fmt.Println("💡 Chế độ: [FULL DEMO] - Tự động minh họa Producer, Consumer & ACK/DLQ")
		fmt.Println("👉 Mẹo: Bạn có thể mở 2 terminal riêng biệt:")
		fmt.Println("   - Terminal 1: go run cmd/demo-rabbitmq/main.go consumer")
		fmt.Println("   - Terminal 2: go run cmd/demo-rabbitmq/main.go producer")
		fmt.Println("   - Web UI:     http://localhost:15672 (guest/guest)")
		fmt.Println("------------------------------------------------------------------")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Chạy Consumer ngầm
		go runConsumersInternal(ctx)

		time.Sleep(1 * time.Second)

		// Bắn các đơn hàng mẫu
		publishSampleOrders()

		time.Sleep(4 * time.Second)
		cancel()
		fmt.Println("\n✅ Đã hoàn thành Full Demo RabbitMQ!")
	}
}

// 1. THIẾT LẬP HẠ TẦNG (Exchanges, Queues, Bindings & DLQ)
func setupRabbitMQInfrastructure() {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Fatalf("❌ Không thể kết nối RabbitMQ tại %s: %v\n👉 Hãy chắc chắn bạn đã chạy 'docker compose up -d rabbitmq'", amqpURL, err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ Lỗi mở Channel: %v", err)
	}
	defer ch.Close()

	// A. Khởi tạo Dead Letter Exchange (DLX) & Dead Letter Queue (DLQ) để hứng tin nhắn lỗi
	_ = ch.ExchangeDeclare(exchangeDLX, "direct", true, false, false, false, nil)
	_, _ = ch.QueueDeclare(queueDLQ, true, false, false, false, nil)
	_ = ch.QueueBind(queueDLQ, "failed.order", exchangeDLX, false, nil)

	// B. Khởi tạo Direct Exchange cho Đơn hàng
	err = ch.ExchangeDeclare(
		exchangeDirect, // Tên Exchange
		"direct",       // Loại Exchange (Định tuyến chính xác theo Routing Key)
		true,           // Durable (Lưu trên đĩa, không mất khi restart)
		false,          // Auto-deleted
		false,          // Internal
		false,          // No-wait
		nil,
	)
	if err != nil {
		log.Printf("⚠️ Lỗi khai báo Direct Exchange: %v\n", err)
	}

	// C. Khai báo các Hàng Đợi (Queues)
	// 1. Queue Kho hàng (Có gắn DLX: Nếu tin bị NACK sẽ tự động chuyển sang DLQ)
	queueArgs := amqp.Table{
		"x-dead-letter-exchange":    exchangeDLX,
		"x-dead-letter-routing-key": "failed.order",
	}

	_, _ = ch.QueueDeclare(queueInventory, true, false, false, false, queueArgs)
	_, _ = ch.QueueDeclare(queueEmail, true, false, false, false, nil)
	_, _ = ch.QueueDeclare(queuePayment, true, false, false, false, nil)

	// D. Ràng buộc (Binding) Hàng đợi với Exchange qua Routing Key
	// - Khi có sự kiện 'order.created' -> Gửi cho cả Kho Hàng (Inventory) và Gửi Email
	_ = ch.QueueBind(queueInventory, routingCreated, exchangeDirect, false, nil)
	_ = ch.QueueBind(queueEmail, routingCreated, exchangeDirect, false, nil)

	// - Khi có sự kiện 'order.paid' -> Gửi cho Bộ phận Thanh toán (Payment)
	_ = ch.QueueBind(queuePayment, routingPaid, exchangeDirect, false, nil)

	// E. Khởi tạo Fanout Exchange (Phát thanh quảng bá)
	_ = ch.ExchangeDeclare(exchangeFanout, "fanout", true, false, false, false, nil)
	_ = ch.QueueBind(queueInventory, "", exchangeFanout, false, nil)
	_ = ch.QueueBind(queueEmail, "", exchangeFanout, false, nil)
}

// 2. PRODUCER CHUYÊN BIỆT: Bắn sự kiện đơn hàng vào Exchange
func runDedicatedProducer() {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Fatalf("❌ Lỗi kết nối RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ Lỗi mở Channel: %v", err)
	}
	defer ch.Close()

	orders := []struct {
		Event      OrderEvent
		RoutingKey string
	}{
		{
			Event: OrderEvent{
				OrderID:   "ORD-1001",
				Customer:  "Nguyen Van A",
				Product:   "Máy lạnh Daikin Inverter 1.5 HP",
				Amount:    13500000,
				Currency:  "VND",
				Action:    "TAO_DON_HANG",
				CreatedAt: time.Now().Format("15:04:05"),
			},
			RoutingKey: routingCreated,
		},
		{
			Event: OrderEvent{
				OrderID:   "ORD-1002",
				Customer:  "Tran Thi B",
				Product:   "Tủ lạnh Samsung 4 cánh 647L",
				Amount:    24990000,
				Currency:  "VND",
				Action:    "THANH_TOAN_THANH_CONG",
				CreatedAt: time.Now().Format("15:04:05"),
			},
			RoutingKey: routingPaid,
		},
		{
			Event: OrderEvent{
				OrderID:    "ORD-1003-ERROR",
				Customer:   "Le Van C (Thẻ lỗi)",
				Product:    "Bếp từ đôi Bosch PPI82560MS",
				Amount:     11200000,
				Currency:   "VND",
				Action:     "TAO_DON_HANG_LOI_TEST_DLQ",
				ShouldFail: true, // Đơn này sẽ bị Consumer NACK và đẩy vào Dead Letter Queue
				CreatedAt:  time.Now().Format("15:04:05"),
			},
			RoutingKey: routingCreated,
		},
	}

	fmt.Printf("📤 Đang gửi %d đơn hàng mẫu vào Exchange '%s'...\n\n", len(orders), exchangeDirect)

	for i, item := range orders {
		body, _ := json.Marshal(item.Event)

		err := ch.PublishWithContext(
			context.Background(),
			exchangeDirect,  // Tên Exchange
			item.RoutingKey, // Routing Key (Định tuyến)
			false,           // Mandatory
			false,           // Immediate
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent, // Lưu tin nhắn vào ổ đĩa để chống mất mát
				Body:         body,
			},
		)

		if err != nil {
			log.Printf("❌ Lỗi gửi đơn hàng %s: %v\n", item.Event.OrderID, err)
		} else {
			fmt.Printf(" [PRODUCER ĐÃ GỬI] 📦 [%d/%d] Đơn: %s | RoutingKey='%s' | Sản phẩm: %s\n",
				i+1, len(orders), item.Event.OrderID, item.RoutingKey, item.Event.Product)
		}
		time.Sleep(800 * time.Millisecond)
	}

	fmt.Println("\n🎉 Đã gửi xong tất cả đơn hàng vào RabbitMQ!")
}

// 3. CONSUMER CHUYÊN BIỆT: Lắng nghe và xử lý tin nhắn
func runDedicatedConsumer() {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Fatalf("❌ Lỗi kết nối RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ Lỗi mở Channel: %v", err)
	}
	defer ch.Close()

	// Thiết lập QoS (Quality of Service) - PrefetchCount = 1
	// Giúp phân phối đều tải: Mỗi worker chỉ nhận tối đa 1 tin nhắn tại 1 thời điểm, xử lý xong mới lấy tiếp
	_ = ch.Qos(1, 0, false)

	// Lắng nghe trên 3 Hàng đợi chính + 1 Hàng đợi lỗi DLQ
	msgsInventory, _ := ch.Consume(queueInventory, "consumer-inventory", false, false, false, false, nil)
	msgsEmail, _ := ch.Consume(queueEmail, "consumer-email", false, false, false, false, nil)
	msgsPayment, _ := ch.Consume(queuePayment, "consumer-payment", false, false, false, false, nil)
	msgsDLQ, _ := ch.Consume(queueDLQ, "consumer-dlq", false, false, false, false, nil)

	fmt.Printf("🎧 [CONSUMER] Đang lắng nghe trên các Queue:\n")
	fmt.Printf("   ├─ 📦 '%s' (Trừ tồn kho)\n", queueInventory)
	fmt.Printf("   ├─ ✉️  '%s' (Gửi email khách hàng)\n", queueEmail)
	fmt.Printf("   ├─ 💳 '%s' (Xử lý thanh toán)\n", queuePayment)
	fmt.Printf("   └─ ☠️  '%s' (Dead Letter Queue - Đơn hàng lỗi)\n", queueDLQ)
	fmt.Println("\n👉 Hãy mở Tab Terminal khác và chạy: go run cmd/demo-rabbitmq/main.go producer")
	fmt.Println("👉 Bấm Ctrl+C tại terminal này để dừng.")
	fmt.Println("------------------------------------------------------------------")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n👋 Đã tắt Consumer an toàn!")
			return

		case d, ok := <-msgsInventory:
			if ok {
				handleInventoryMessage(d)
			}

		case d, ok := <-msgsEmail:
			if ok {
				handleEmailMessage(d)
			}

		case d, ok := <-msgsPayment:
			if ok {
				handlePaymentMessage(d)
			}

		case d, ok := <-msgsDLQ:
			if ok {
				handleDLQMessage(d)
			}
		}
	}
}

// Xử lý đơn hàng tại Kho (Minh họa ACK vs NACK/DLQ)
func handleInventoryMessage(d amqp.Delivery) {
	var event OrderEvent
	_ = json.Unmarshal(d.Body, &event)

	fmt.Printf("\n📦 [WORKER KHO HÀNG] Nhận đơn '%s': %s (Khách: %s)\n", event.OrderID, event.Product, event.Customer)

	if event.ShouldFail {
		// Giả lập lỗi: Hết hàng hoặc dữ liệu sai
		fmt.Printf("   ❌ [KHO HÀNG LỖI] Đơn '%s' hết hàng trong kho!\n", event.OrderID)
		fmt.Printf("   👉 Gửi lệnh NACK (requeue=false) -> RabbitMQ tự động đẩy vào Dead Letter Queue (DLQ)!\n")
		// Nack với requeue = false sẽ đẩy tin nhắn vào DLX/DLQ
		_ = d.Nack(false, false)
		return
	}

	fmt.Printf("   ✅ [KHO HÀNG THÀNH CÔNG] Đã trừ 1 '%s' trong kho -> Gửi ACK xác nhận!\n", event.Product)
	_ = d.Ack(false) // Xác nhận đã xử lý xong
}

// Xử lý gửi email
func handleEmailMessage(d amqp.Delivery) {
	var event OrderEvent
	_ = json.Unmarshal(d.Body, &event)

	fmt.Printf("\n✉️  [WORKER EMAIL] Gửi email xác nhận đơn '%s' tới khách hàng '%s' (Tổng tiền: %.0f %s)\n",
		event.OrderID, event.Customer, event.Amount, event.Currency)
	_ = d.Ack(false)
}

// Xử lý thanh toán
func handlePaymentMessage(d amqp.Delivery) {
	var event OrderEvent
	_ = json.Unmarshal(d.Body, &event)

	fmt.Printf("\n💳 [WORKER THANH TOÁN] Đã thu tiền thành công đơn '%s': %.0f %s\n",
		event.OrderID, event.Amount, event.Currency)
	_ = d.Ack(false)
}

// Xử lý hàng đợi lỗi Dead Letter Queue
func handleDLQMessage(d amqp.Delivery) {
	var event OrderEvent
	_ = json.Unmarshal(d.Body, &event)

	fmt.Printf("\n☠️  [DEAD LETTER QUEUE (DLQ)] Phát hiện đơn hàng lỗi được chuyển vào:\n")
	fmt.Printf("   ├─ Mã đơn: %s\n", event.OrderID)
	fmt.Printf("   ├─ Khách hàng: %s\n", event.Customer)
	fmt.Printf("   └─ Hành động: Cần Admin / Kế toán can thiệp xử lý thủ công!\n")
	_ = d.Ack(false)
}

// 4. DEMO PHÁT THANH QUẢNG BÁ (FANOUT EXCHANGE)
func runFanoutDemo() {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Fatalf("❌ Lỗi kết nối RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ Lỗi mở Channel: %v", err)
	}
	defer ch.Close()

	msg := map[string]string{
		"type":    "KHUYEN_MAI_TOAN_SAN",
		"message": "🔥 SIÊU SALE 9.9 - GIẢM 50% TOÀN BỘ MÁY LẠNH & TỦ LẠNH!",
		"time":    time.Now().Format("15:04:05"),
	}
	body, _ := json.Marshal(msg)

	fmt.Println("📢 Đang phát thanh thông báo khuyến mãi qua Fanout Exchange 'ecom.broadcast.fanout'...")
	err = ch.PublishWithContext(
		context.Background(),
		exchangeFanout,
		"", // Fanout bỏ qua Routing Key, tự động nhân bản tin cho TOÀN BỘ Queue đã bind
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)

	if err != nil {
		log.Printf("❌ Lỗi phát thanh: %v", err)
	} else {
		fmt.Println("🎉 Đã phát thanh thành công! Mọi Queue đăng ký đều nhận được bản sao tin nhắn này.")
	}
}

func runConsumersInternal(ctx context.Context) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return
	}
	defer ch.Close()

	msgsInventory, _ := ch.Consume(queueInventory, "auto-inventory", false, false, false, false, nil)
	msgsEmail, _ := ch.Consume(queueEmail, "auto-email", false, false, false, false, nil)
	msgsPayment, _ := ch.Consume(queuePayment, "auto-payment", false, false, false, false, nil)
	msgsDLQ, _ := ch.Consume(queueDLQ, "auto-dlq", false, false, false, false, nil)

	for {
		select {
		case <-ctx.Done():
			return
		case d := <-msgsInventory:
			handleInventoryMessage(d)
		case d := <-msgsEmail:
			handleEmailMessage(d)
		case d := <-msgsPayment:
			handlePaymentMessage(d)
		case d := <-msgsDLQ:
			handleDLQMessage(d)
		}
	}
}

func publishSampleOrders() {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return
	}
	defer ch.Close()

	order1 := OrderEvent{
		OrderID:   "ORD-AUTO-01",
		Customer:  "Hoang Minh Tuan",
		Product:   "Smart Tivi Sony 65 Inch 4K",
		Amount:    18900000,
		Currency:  "VND",
		Action:    "TAO_DON_HANG",
		CreatedAt: time.Now().Format("15:04:05"),
	}
	b1, _ := json.Marshal(order1)
	_ = ch.PublishWithContext(context.Background(), exchangeDirect, routingCreated, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        b1,
	})
	fmt.Printf("📤 [PRODUCER] Gửi đơn hàng hợp lệ: %s - %s\n", order1.OrderID, order1.Product)

	time.Sleep(500 * time.Millisecond)

	order2 := OrderEvent{
		OrderID:    "ORD-AUTO-02-FAIL",
		Customer:   "Pham Thi Hang (Kho hết)",
		Product:    "Robot Hút Bụi Dreame L10s",
		Amount:     12500000,
		Currency:   "VND",
		Action:     "TAO_DON_HANG",
		ShouldFail: true,
		CreatedAt:  time.Now().Format("15:04:05"),
	}
	b2, _ := json.Marshal(order2)
	_ = ch.PublishWithContext(context.Background(), exchangeDirect, routingCreated, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        b2,
	})
	fmt.Printf("📤 [PRODUCER] Gửi đơn hàng test lỗi DLQ: %s - %s\n", order2.OrderID, order2.Product)
}
