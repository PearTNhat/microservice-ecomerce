package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker = "localhost:9092"
	topicName   = "demo-ecommerce-events"
)

func main() {
	args := os.Args[1:]
	mode := "all"
	if len(args) > 0 {
		mode = args[0]
	}

	fmt.Println("==================================================================")
	fmt.Println("🎓 HỌC VIỆN APACHE KAFKA: TUTORIAL & THỰC HÀNH TƯƠNG TÁC")
	fmt.Println("==================================================================")

	// Đảm bảo Topic đã sẵn sàng trên Kafka Broker
	ensureTopicExists(topicName, 2) // Tạo topic có 2 Partitions để học

	switch mode {
	case "producer":
		fmt.Println("🚀 Chế độ: [PRODUCER CHUYÊN BIỆT] - Bắn sự kiện mua sắm vào Kafka")
		runInteractiveProducer()

	case "consumer":
		fmt.Println("🎧 Chế độ: [CONSUMER CHUYÊN BIỆT] - Lắng nghe sự kiện thời gian thực")
		runDedicatedConsumer("team-analytics")

	default:
		// Chế độ mặc định: Chạy cả Producer và Consumer cùng lúc
		fmt.Println("💡 Chế độ: [FULL DEMO] - Chạy đồng thời Producer & Consumer")
		fmt.Println("👉 Mẹo: Bạn có thể mở 2 terminal riêng biệt:")
		fmt.Println("   - Terminal 1: go run cmd/demo-kafka/main.go consumer")
		fmt.Println("   - Terminal 2: go run cmd/demo-kafka/main.go producer")
		fmt.Println("------------------------------------------------------------------")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Chạy Consumer ngầm
		go runConsumerWithContext(ctx, "full-demo-group")

		time.Sleep(1 * time.Second)

		// Chạy Producer gửi 3 tin mẫu
		sendSampleEvents()

		// Chờ 3 giây để Consumer in hết dữ liệu
		time.Sleep(3 * time.Second)
		cancel()
		fmt.Println("\n✅ Đã hoàn thành Full Demo!")
	}
}

// 1. TẠO TOPIC NẾU CHƯA CÓ (Topic có 2 Partitions để hiểu cách chia tải)
func ensureTopicExists(topic string, numPartitions int) {
	conn, err := kafka.Dial("tcp", kafkaBroker)
	if err != nil {
		log.Printf("⚠️ Chưa thể kết nối Kafka Broker tại %s: %v\n", kafkaBroker, err)
		return
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return
	}
	defer controllerConn.Close()

	_ = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: 1,
	})
}

// 2. PRODUCER: Gửi dữ liệu sự kiện vào Kafka
func runInteractiveProducer() {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        topicName,
		Balancer:     &kafka.Hash{}, // Dùng Hash theo Key để định tuyến vào Partition
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	sampleOrders := []struct {
		Customer string
		Action   string
		Product  string
		Amount   string
	}{
		{"nguyen_van_a", "XEM_SAN_PHAM", "Máy lạnh Daikin Inverter 1.5 HP", "13.500.000 ₫"},
		{"tran_thi_b", "THEM_GIO_HANG", "Tủ lạnh Samsung 4 cánh 647L", "24.990.000 ₫"},
		{"nguyen_van_a", "DAT_HANG_THANH_CONG", "Máy lạnh Daikin Inverter 1.5 HP", "13.500.000 ₫"},
		{"le_van_c", "XEM_SAN_PHAM", "Bếp từ đôi Bosch PPI82560MS", "11.200.000 ₫"},
	}

	fmt.Printf("📤 Đang gửi %d sự kiện mẫu vào Topic '%s'...\n\n", len(sampleOrders), topicName)

	for i, item := range sampleOrders {
		payload := fmt.Sprintf(`{"stt": %d, "khach_hang": "%s", "hanh_dong": "%s", "san_pham": "%s", "gia_tien": "%s", "thoi_gian": "%s"}`,
			i+1, item.Customer, item.Action, item.Product, item.Amount, time.Now().Format("15:04:05"))

		err := writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(item.Customer), // Key giúp gom sự kiện của cùng 1 khách hàng vào cùng 1 Partition
			Value: []byte(payload),
		})

		if err != nil {
			log.Printf("❌ Lỗi gửi: %v\n", err)
		} else {
			fmt.Printf(" [PRODUCER ĐÃ BẮN] 📦 Key='%s' | Sự kiện: %s - %s\n", item.Customer, item.Action, item.Product)
		}
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("\n🎉 Đã gửi xong tất cả sự kiện!")
}

// 3. CONSUMER: Lắng nghe và đọc dữ liệu từ Kafka
func runDedicatedConsumer(groupID string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       topicName,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset, // Đọc từ tin nhắn đầu tiên trong lịch sử
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer reader.Close()

	fmt.Printf("🎧 [CONSUMER] Đang lắng nghe trên Topic '%s' (Group: '%s')...\n", topicName, groupID)
	fmt.Println("👉 Hãy mở một Tab/Cửa sổ Terminal RIÊNG BIỆT khác và chạy: go run cmd/demo-kafka/main.go producer")
	fmt.Println("👉 Bấm Ctrl+C tại terminal này để dừng.")
	fmt.Println("------------------------------------------------------------------")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("\n👋 Đã tắt Consumer an toàn!")
				return
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}

		// In chi tiết các trường cốt lõi của Kafka Message
		fmt.Printf("\n📩 [CONSUMER NHẬN ĐƯỢC TIN MỚI]:\n")
		fmt.Printf("   ├─ 📍 Partition  : %d (Phân vùng lưu trữ)\n", msg.Partition)
		fmt.Printf("   ├─ 🔢 Offset     : %d (Vị trí số thứ tự trong hàng đợi)\n", msg.Offset)
		fmt.Printf("   ├─ 🔑 Message Key: %s\n", string(msg.Key))
		fmt.Printf("   ├─ 📦 Nội dung   : %s\n", string(msg.Value))
		fmt.Printf("   └─ ⏰ Thời gian  : %s\n", msg.Time.Format("15:04:05"))

		// Commit để Kafka biết Consumer này đã xử lý xong tin nhắn này
		_ = reader.CommitMessages(ctx, msg)
	}
}

func sendSampleEvents() {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	events := []string{
		"Khách A xem Máy lạnh Daikin",
		"Khách B thêm Tủ lạnh Samsung vào giỏ hàng",
		"Khách C đặt mua Smart Tivi Sony 65 Inch",
	}

	fmt.Printf("\n📤 [PRODUCER] Gửi 3 sự kiện mẫu...\n")
	for i, e := range events {
		msg := fmt.Sprintf(`{"event_id": %d, "detail": "%s"}`, i+1, e)
		_ = writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(fmt.Sprintf("key_%d", i)),
			Value: []byte(msg),
		})
		fmt.Printf("   👉 [Đã gửi]: %s\n", e)
		time.Sleep(300 * time.Millisecond)
	}
}

func runConsumerWithContext(ctx context.Context, groupID string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       topicName,
		GroupID:     groupID,
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return
		}
		fmt.Printf("   ⚡ [CONSUMER NHẬN ĐƯỢC] (Partition %d | Offset %d): %s\n",
			msg.Partition, msg.Offset, string(msg.Value))
	}
}
