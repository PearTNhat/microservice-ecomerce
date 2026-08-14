package main

import (
	"ecomerce-service/config"
	"ecomerce-service/internal/api/grpc/pb"
	grpc_handlers "ecomerce-service/internal/api/grpc/handlers"
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/api/rest/handlers"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/repository/postgres"
	"ecomerce-service/internal/worker"
	"log"
	"net"

	"github.com/gofiber/fiber/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func setupRoutes(app *fiber.App, userService *service.UserService) {
	rh := &rest.RestHandler{
		App: app,
	}
	// Khởi tạo các routes cho User (REST)
	handlers.SetupUserRoutes(rh, userService)
}

func startGrpcServer(userService *service.UserService) {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("gRPC failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	
	// Khởi tạo gRPC Handler và đăng ký với gRPC Server
	userGrpcHandler := grpc_handlers.NewUserGrpcHandler(userService)
	pb.RegisterUserServiceServer(grpcServer, userGrpcHandler)

	log.Println("🚀 gRPC Server đang chạy tại port 50051...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC failed to serve: %v", err)
	}
}

func runTaskProcessor(redisOpt asynq.RedisClientOpt, appConfig config.AppConfig) {
	processor := worker.NewRedisTaskProcessor(redisOpt, appConfig)
	log.Println("Bắt đầu khởi động Task Processor (Worker)...")
	err := processor.Start()
	if err != nil {
		log.Fatal("Không thể khởi động Task Processor:", err)
	}
}

func main() {
	// 1. Load cấu hình từ file .env
	appConfig := config.LoadConfig()

	// 2. Cấu hình Redis cho Asynq
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

	// 3. Khởi tạo REST Server (Bao gồm Fiber App, kết nối DB)
	server := rest.NewServer(appConfig)

	// 4. Khởi tạo Dependencies (DI) - Inject TaskDistributor và Redis vào UserService
	userRepo := postgres.NewUserRepository(server.DB)
	userService := service.NewUserService(userRepo, appConfig, taskDistributor, importRedis)

	// 5. Khởi tạo các routes cho REST
	setupRoutes(server.App, userService)

	// 6. Chạy gRPC Server ở một luồng (Goroutine) riêng biệt chạy ngầm
	go startGrpcServer(userService)

	// 7. Chạy REST Server ở luồng chính
	server.Start()
}
