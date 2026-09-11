package main

import (
	"context"
	"ecomerce-service/pkg/config"
	"ecomerce-service/pkg/elasticsearch"
	"ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/pkg/server"
	http_handlers "ecomerce-service/services/product-service/internal/delivery/http"
	"ecomerce-service/services/product-service/internal/domain"
	"ecomerce-service/services/product-service/internal/repository"
	"ecomerce-service/services/product-service/internal/service"
	"ecomerce-service/services/product-service/internal/worker"
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
	logger.Info("📦 Khởi động Product Microservice (Database-per-Service & Kafka Saga Worker)",
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
	srv := server.NewServer(appConfig)

	// AutoMigrate bảng Category, Brand, Product
	err := srv.DB.AutoMigrate(
		&domain.Category{},
		&domain.Brand{},
		&domain.Product{},
	)
	if err != nil {
		logger.Error("❌ Lỗi AutoMigrate Product/Category/Brand", "error", err.Error())
	} else {
		// Tự động seed dữ liệu mẫu đồ điện máy và index vào Elasticsearch nếu DB trống
		repository.SeedSampleData(srv.DB, esClient)
	}

	// 5. Khởi tạo Kafka Producer cho lượt xem và sự kiện Stock Saga
	kafkaBrokers := []string{appConfig.KafkaBrokers}
	kafkaViewProducer := kafka.NewKafkaProducer(kafkaBrokers, "product-views")
	orderKafkaProducer := kafka.NewOrderKafkaProducer(kafkaBrokers)
	defer orderKafkaProducer.Close()

	// 6. Khởi tạo Repositories & Services
	productRepo := repository.NewProductRepository(srv.DB)
	productService := service.NewProductService(productRepo, importRedis, kafkaViewProducer, esClient)

	// 7. Khởi chạy các Kafka Consumer Workers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 7.1. Kafka View Consumer Worker (Gom batch lượt xem ngầm)
	viewWorker := worker.NewProductViewWorker(kafkaBrokers, "product-views", productRepo, importRedis)
	if viewWorker != nil {
		viewWorker.Start(ctx)
		defer viewWorker.Close()
	}

	// 7.2. Kafka Stock Consumer Worker (Saga Choreography: lắng nghe order.events và tự trừ kho trong ecom_product_db)
	stockWorker := worker.NewProductStockWorker(kafkaBrokers, productRepo, importRedis, orderKafkaProducer)
	if stockWorker != nil {
		stockWorker.Start(ctx)
		defer stockWorker.Close()
	}

	// 8. Đăng ký Product REST Routes
	rh := &server.RestHandler{
		App:    srv.App,
		Config: appConfig,
	}
	http_handlers.SetupProductRoutes(rh, productService)

	// 9. Chạy REST Server ở luồng chính
	srv.Start()
}
