package main

import (
	"ecomerce-service/config"
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/api/rest/handlers"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/repository/postgres"
	"ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"os"

	"github.com/redis/go-redis/v9"
)

func main() {
	appConfig := config.LoadConfig()

	// Cổng mặc định cho Product Service: REST 8002
	restPort := os.Getenv("PRODUCT_SERVICE_PORT")
	if restPort == "" {
		restPort = "8002"
	}
	appConfig.ServerPort = restPort

	// 1. Khởi tạo Structured Logger
	logger.InitLogger("product-service", appConfig.Environment, appConfig.GraylogAddress)
	logger.Info("📦 Khởi động Product Microservice",
		"rest_port", restPort,
		"env", appConfig.Environment,
		"kafka_brokers", appConfig.KafkaBrokers,
	)

	// 2. Khởi tạo Redis Client cho Caching
	importRedis := redis.NewClient(&redis.Options{
		Addr: appConfig.RedisAddress,
	})

	// 3. Khởi tạo REST Server & DB
	server := rest.NewServer(appConfig)

	// AutoMigrate bảng Category, Brand, Product
	err := server.DB.AutoMigrate(
		&domain.Category{},
		&domain.Brand{},
		&domain.Product{},
	)
	if err != nil {
		logger.Error("❌ Lỗi AutoMigrate Product/Category/Brand", "error", err.Error())
	} else {
		// Tự động seed dữ liệu mẫu đồ điện máy nếu chưa có
		postgres.SeedSampleData(server.DB)
	}

	// 4. Khởi tạo Kafka Producer
	kafkaProducer := kafka.NewKafkaProducer([]string{appConfig.KafkaBrokers}, "product-views")

	// 5. Khởi tạo Repositories & Services
	productRepo := postgres.NewProductRepository(server.DB)
	productService := service.NewProductService(productRepo, importRedis, kafkaProducer)

	// 6. Đăng ký Product REST Routes
	rh := &rest.RestHandler{
		App:    server.App,
		Config: appConfig,
	}
	handlers.SetupProductRoutes(rh, productService)

	// 7. Chạy REST Server ở luồng chính
	server.Start()
}
