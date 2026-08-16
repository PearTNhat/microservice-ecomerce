package main

import (
	"ecomerce-service/config"
	grpc_handlers "ecomerce-service/internal/api/grpc/handlers"
	"ecomerce-service/internal/api/grpc/pb"
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/api/rest/handlers"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/repository/postgres"
	"ecomerce-service/internal/worker"
	"ecomerce-service/pkg/logger"
	"net"

	"github.com/gofiber/fiber/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func setupRoutes(app *fiber.App, appConfig config.AppConfig, userService *service.UserService) {
	rh := &rest.RestHandler{
		App:    app,
		Config: appConfig,
	}
	// Khởi tạo các routes cho User (REST)
	handlers.SetupUserRoutes(rh, userService)

	// Catch-all route cho 404 Not Found
	app.Use(func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"status":  "error",
			"message": "Đường dẫn không tồn tại (404 Not Found). Vui lòng kiểm tra lại URL.",
		})
	})
}

func startGrpcServer(userService *service.UserService) {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		logger.Error("❌ gRPC failed to listen", "error", err.Error())
		return
	}

	grpcServer := grpc.NewServer()

	// Khởi tạo gRPC Handler và đăng ký với gRPC Server
	userGrpcHandler := grpc_handlers.NewUserGrpcHandler(userService)
	pb.RegisterUserServiceServer(grpcServer, userGrpcHandler)

	logger.Info("🚀 gRPC Server đang chạy tại port 50051...")
	if err := grpcServer.Serve(lis); err != nil {
		logger.Error("❌ gRPC failed to serve", "error", err.Error())
	}
}

func runTaskProcessor(redisOpt asynq.RedisClientOpt, appConfig config.AppConfig) {
	processor := worker.NewRedisTaskProcessor(redisOpt, appConfig)
	logger.Info("⚙️ Bắt đầu khởi động Task Processor (Asynq Worker)...")
	err := processor.Start()
	if err != nil {
		logger.Error("❌ Không thể khởi động Task Processor", "error", err.Error())
	}
}

func main() {
	// 1. Load cấu hình từ file .env
	appConfig := config.LoadConfig()

	// 2. Khởi tạo Structured Logger & Kết nối Graylog GELF
	logger.InitLogger("user-service", appConfig.Environment, appConfig.GraylogAddress)
	logger.Info("🚀 Khởi động User Microservice",
		"env", appConfig.Environment,
		"graylog_addr", appConfig.GraylogAddress,
	)

	// 3. Cấu hình Redis cho Asynq
	redisOpt := asynq.RedisClientOpt{
		Addr: appConfig.RedisAddress,
	}

	// Khởi tạo Task Distributor (Client)
	taskDistributor := worker.NewRedisTaskDistributor(redisOpt)

	// Khởi tạo Redis Client tiêu chuẩn (dùng chung cho lưu trữ tạm)
	importRedis := redis.NewClient(&redis.Options{
		Addr: appConfig.RedisAddress,
	})

	// Chạy Task Processor (Server/Worker) ngầm
	go runTaskProcessor(redisOpt, appConfig)

	// 4. Khởi tạo REST Server (Bao gồm Fiber App, kết nối DB)
	server := rest.NewServer(appConfig)

	// 5. Khởi tạo Dependencies (DI) - Inject TaskDistributor và Redis vào UserService
	userRepo := postgres.NewUserRepository(server.DB)
	userService := service.NewUserService(userRepo, appConfig, taskDistributor, importRedis)

	// 6. Khởi tạo các routes cho REST
	setupRoutes(server.App, appConfig, userService)

	// 7. Chạy gRPC Server ở một luồng (Goroutine) riêng biệt chạy ngầm
	go startGrpcServer(userService)

	// 8. Chạy REST Server ở luồng chính
	server.Start()
}
