package main

import (
	"ecomerce-service/config"
	grpc_handlers "ecomerce-service/internal/api/grpc/handlers"
	"ecomerce-service/internal/api/grpc/pb"
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/api/rest/handlers"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/repository/postgres"
	"ecomerce-service/internal/worker"
	"ecomerce-service/pkg/logger"
	"fmt"
	"net"
	"os"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func startGrpcServer(port string, userService *service.UserService) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		logger.Error("❌ User gRPC failed to listen", "port", port, "error", err.Error())
		return
	}

	grpcServer := grpc.NewServer()
	userGrpcHandler := grpc_handlers.NewUserGrpcHandler(userService)
	pb.RegisterUserServiceServer(grpcServer, userGrpcHandler)

	logger.Info("🚀 User gRPC Server đang chạy", "port", port)
	if err := grpcServer.Serve(lis); err != nil {
		logger.Error("❌ User gRPC failed to serve", "error", err.Error())
	}
}

func runTaskProcessor(redisOpt asynq.RedisClientOpt, appConfig config.AppConfig) {
	processor := worker.NewRedisTaskProcessor(redisOpt, appConfig)
	logger.Info("⚙️ Khởi động Asynq Email Worker...")
	if err := processor.Start(); err != nil {
		logger.Error("❌ Không thể khởi động Asynq Worker", "error", err.Error())
	}
}

func main() {
	appConfig := config.LoadConfig()

	// Cổng mặc định cho User Service: REST 8001, gRPC 50051
	restPort := os.Getenv("USER_SERVICE_PORT")
	if restPort == "" {
		restPort = "8001"
	}
	appConfig.ServerPort = restPort

	grpcPort := os.Getenv("USER_GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	// 1. Khởi tạo Structured Logger
	logger.InitLogger("user-service", appConfig.Environment, appConfig.GraylogAddress)
	logger.Info("👤 Khởi động User Microservice",
		"rest_port", restPort,
		"grpc_port", grpcPort,
		"env", appConfig.Environment,
	)

	// 2. Cấu hình Redis & Asynq Worker
	redisOpt := asynq.RedisClientOpt{Addr: appConfig.RedisAddress}
	taskDistributor := worker.NewRedisTaskDistributor(redisOpt)
	importRedis := redis.NewClient(&redis.Options{Addr: appConfig.RedisAddress})

	go runTaskProcessor(redisOpt, appConfig)

	// 3. Khởi tạo REST Server & DB
	server := rest.NewServer(appConfig)

	// AutoMigrate bảng User
	if err := server.DB.AutoMigrate(&domain.User{}); err != nil {
		logger.Error("❌ Lỗi AutoMigrate User", "error", err.Error())
	}

	// 4. Khởi tạo Repositories & Services
	userRepo := postgres.NewUserRepository(server.DB)
	userService := service.NewUserService(userRepo, appConfig, taskDistributor, importRedis)

	// 5. Đăng ký User REST Routes
	rh := &rest.RestHandler{
		App:    server.App,
		Config: appConfig,
	}
	handlers.SetupUserRoutes(rh, userService)

	// 6. Chạy gRPC Server ở Goroutine ngầm
	go startGrpcServer(grpcPort, userService)

	// 7. Chạy REST Server ở luồng chính
	server.Start()
}
