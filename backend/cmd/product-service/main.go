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
	"ecomerce-service/pkg/elasticsearch"
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
	appConfig.Dns = appConfig.ProductDbDns

	// 1. Khởi tạo Structured Logger
	logger.InitLogger("product-service", appConfig.Environment, appConfig.GraylogAddress)
	logger.Info("📦 Khởi động Product Microservice",
		"rest_port", restPort,
		"env", appConfig.Environment,
		"kafka_brokers", appConfig.KafkaBrokers,
		"elasticsearch_addr", appConfig.ElasticsearchAddress,
	)

	// 2. Khởi tạo Redis Client cho Caching
	importRedis := redis.NewClient(&redis.Options{
		Addr: appConfig.RedisAddress,
	})

	// 3. Khởi tạo Elasticsearch 8 Client
	esClient := elasticsearch.NewElasticsearchClient(appConfig.ElasticsearchAddress)

	// 4. Khởi tạo REST Server & DB (Dùng riêng database ecom_product_db)
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
		// Tự động seed dữ liệu mẫu đồ điện máy và index vào Elasticsearch nếu DB trống
		postgres.SeedSampleData(server.DB, esClient)
	}

	// 5. Khởi tạo Kafka Producer
	kafkaProducer := kafka.NewKafkaProducer([]string{appConfig.KafkaBrokers}, "product-views")

	// 6. Khởi tạo Repositories & Services
	productRepo := postgres.NewProductRepository(server.DB)
	productService := service.NewProductService(productRepo, importRedis, kafkaProducer, esClient)

	// 7. Khởi tạo và kích hoạt Kafka View Consumer Worker (Gom batch lượt xem ngầm)
	ctx := context.Background()
	viewWorker := worker.NewProductViewWorker([]string{appConfig.KafkaBrokers}, "product-views", productRepo, importRedis)
	if viewWorker != nil {
		viewWorker.Start(ctx)
		defer viewWorker.Close()
	}

	// 8. Đăng ký Product REST Routes
	rh := &rest.RestHandler{
		App:    server.App,
		Config: appConfig,
	}
	handlers.SetupProductRoutes(rh, productService)

	// 9. Chạy REST Server ở luồng chính
	server.Start()
}
