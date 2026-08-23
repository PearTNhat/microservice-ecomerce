package main

import (
	"context"
	"ecomerce-service/config"
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/api/rest/handlers"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/repository/postgres"
	"ecomerce-service/internal/worker"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/rabbitmq"
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

	// 1. Khởi tạo Structured Logger
	logger.InitLogger("order-service", appConfig.Environment, appConfig.GraylogAddress)
	logger.Info("🛒 Khởi động Order & Cart Microservice (DDD Modular)",
		"rest_port", restPort,
		"env", appConfig.Environment,
		"rabbitmq_url", appConfig.RabbitMQURL,
		"redis_addr", appConfig.RedisAddress,
	)

	// 2. Khởi tạo Redis Client cho Idempotency & Flash Sale Atomic Lock
	importRedis := redis.NewClient(&redis.Options{
		Addr: appConfig.RedisAddress,
	})

	// 3. Khởi tạo REST Server & DB (Dùng riêng database ecom_order_db)
	server := rest.NewServer(appConfig)

	// AutoMigrate bảng Cart, CartItem, Order, OrderItem
	err := server.DB.AutoMigrate(
		&domain.Cart{},
		&domain.CartItem{},
		&domain.Order{},
		&domain.OrderItem{},
	)
	if err != nil {
		logger.Error("❌ Lỗi AutoMigrate Cart/CartItem/Order/OrderItem", "error", err.Error())
	}

	// 4. Khởi tạo RabbitMQ Event Producer
	rabbitMQProducer := rabbitmq.NewRabbitMQProducer(appConfig.RabbitMQURL)
	defer rabbitMQProducer.Close()

	// 5. Khởi tạo Repositories & Services tách biệt theo Domain
	cartRepo := postgres.NewCartRepository(server.DB)
	orderRepo := postgres.NewOrderRepository(server.DB)

	cartService := service.NewCartService(cartRepo, nil)
	orderService := service.NewOrderService(orderRepo, cartRepo, nil, importRedis, rabbitMQProducer)

	// 6. Khởi chạy RabbitMQ Worker gửi Email hóa đơn ngầm
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	emailWorker := worker.NewOrderEmailWorker(appConfig)
	if emailWorker != nil {
		_ = emailWorker.Start(ctx)
		defer emailWorker.Close()
	}

	// 7. Đăng ký REST Routes theo từng module
	rh := &rest.RestHandler{
		App:    server.App,
		Config: appConfig,
	}
	handlers.SetupCartRoutes(rh, cartService)
	handlers.SetupOrderRoutes(rh, orderService, importRedis)

	// 8. Chạy REST Server ở luồng chính
	server.Start()
}
