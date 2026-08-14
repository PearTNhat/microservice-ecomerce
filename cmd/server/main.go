package main

import (
	"ecomerce-service/config"
	"ecomerce-service/internal/api/grpc/pb"
	grpc_handlers "ecomerce-service/internal/api/grpc/handlers"
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/api/rest/handlers"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/repository/postgres"
	"log"
	"net"

	"github.com/gofiber/fiber/v2"
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

func main() {
	// 1. Load cấu hình từ file .env
	appConfig := config.LoadConfig()

	// 2. Khởi tạo REST Server (Bao gồm Fiber App, kết nối DB)
	server := rest.NewServer(appConfig)

	// 3. Khởi tạo Dependencies (DI) - Tái sử dụng cho cả REST và gRPC!
	userRepo := postgres.NewUserRepository(server.DB)
	userService := service.NewUserService(userRepo, appConfig)

	// 4. Khởi tạo các routes cho REST
	setupRoutes(server.App, userService)

	// 5. Chạy gRPC Server ở một luồng (Goroutine) riêng biệt chạy ngầm
	go startGrpcServer(userService)

	// 6. Chạy REST Server ở luồng chính
	server.Start()
}
