package main

import (
	"context"
	"ecomerce-service/pkg/config"
	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/server"
	"ecomerce-service/services/order-service/internal/client"
	http_handlers "ecomerce-service/services/order-service/internal/delivery/http"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/repository"
	"ecomerce-service/services/order-service/internal/service"
	"ecomerce-service/services/order-service/internal/worker"
	"os"

	"github.com/redis/go-redis/v9"
)

func main() {
	appConfig := config.LoadConfig()

	// Cổng mặc định cho Order Service: REST 8003
	restPort := os.Getenv("ORDER_SERVICE_PORT")
	if restPort == "" {
		restPort = "8003"
	}
	appConfig.ServerPort = restPort
	appConfig.Dns = appConfig.OrderDbDns

	productServiceURL := os.Getenv("PRODUCT_SERVICE_URL")
	if productServiceURL == "" {
		productServiceURL = "http://localhost:8002"
	}

	// 1. Khởi tạo Structured Logger
	logger.InitLogger("order-service", appConfig.Environment, appConfig.GraylogAddress)
	logger.Info("🛒 Khởi động Order Microservice (Database-per-Service & Kafka Event-Driven)",
		"rest_port", restPort,
		"env", appConfig.Environment,
		"kafka_brokers", appConfig.KafkaBrokers,
		"redis_addr", appConfig.RedisAddress,
		"product_service_url", productServiceURL,
	)

	// 2. Khởi tạo Redis Client cho Idempotency & Flash Sale Atomic Lock
	importRedis := redis.NewClient(&redis.Options{
		Addr: appConfig.RedisAddress,
	})

	// 3. Khởi tạo REST Server & DB (CHỈ DÙNG DUY NHẤT database ecom_order_db)
	srv := server.NewServer(appConfig)

	// AutoMigrate bảng Cart, CartItem, Order, OrderItem
	err := srv.DB.AutoMigrate(
		&domain.Cart{},
		&domain.CartItem{},
		&domain.Order{},
		&domain.OrderItem{},
	)
	if err != nil {
		logger.Error("❌ Lỗi AutoMigrate Cart/CartItem/Order/OrderItem", "error", err.Error())
	}

	// 4. Khởi tạo Apache Kafka Order Event Producer
	kafkaBrokers := []string{appConfig.KafkaBrokers}
	orderKafkaProducer := pkgKafka.NewOrderKafkaProducer(kafkaBrokers)
	defer orderKafkaProducer.Close()

	// 5. Khởi tạo Internal Product Client (Giao tiếp HTTP/Cache với Product Service, KHÔNG đụng Product DB)
	productClient := client.NewProductClient(productServiceURL, importRedis)

	// 6. Khởi tạo Repositories & Services
	cartRepo := repository.NewCartRepository(srv.DB)
	orderRepo := repository.NewOrderRepository(srv.DB)

	cartService := service.NewCartService(cartRepo, productClient)
	orderService := service.NewOrderService(orderRepo, cartRepo, productClient, importRedis, orderKafkaProducer)

	// 7. Khởi chạy các Kafka Consumer Workers (Event-Driven Concurrency)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 7.1. Email Worker (Lắng nghe order.events để gửi hóa đơn)
	emailWorker := worker.NewOrderEmailWorker(kafkaBrokers, appConfig, orderKafkaProducer)
	if emailWorker != nil {
		emailWorker.Start(ctx)
		defer emailWorker.Close()
	}

	// 7.2. Flash Sale Worker (Lắng nghe flashsale.orders để cắt đỉnh tải tạo đơn)
	flashSaleWorker := worker.NewFlashSaleWorker(kafkaBrokers, appConfig, orderRepo, productClient, importRedis, orderKafkaProducer)
	if flashSaleWorker != nil {
		flashSaleWorker.Start(ctx)
		defer flashSaleWorker.Close()
	}

	// 7.3. Order Saga Worker (Lắng nghe stock.events từ Product Service để Confirm/Cancel đơn hàng)
	orderSagaWorker := worker.NewOrderSagaWorker(kafkaBrokers, orderRepo, importRedis, orderKafkaProducer)
	if orderSagaWorker != nil {
		orderSagaWorker.Start(ctx)
		defer orderSagaWorker.Close()
	}

	// 8. Đăng ký REST Routes theo từng module
	rh := &server.RestHandler{
		App:    srv.App,
		Config: appConfig,
	}
	http_handlers.SetupCartRoutes(rh, cartService)
	http_handlers.SetupOrderRoutes(rh, orderService, importRedis)

	// 9. Chạy REST Server ở luồng chính
	srv.Start()
}
