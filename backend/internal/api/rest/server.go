package rest

import (
	"ecomerce-service/config"
	"ecomerce-service/internal/api/rest/middlewares"
	"ecomerce-service/pkg/logger"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Server struct {
	App    *fiber.App
	DB     *gorm.DB
	Config config.AppConfig
}

// RestHandler giữ nguyên cấu trúc để truyền dependencies cho route handlers
type RestHandler struct {
	App    *fiber.App
	Config config.AppConfig
}

func NewServer(cfg config.AppConfig) *Server {
	// 1. Khởi tạo kết nối Database bằng GORM
	db, err := gorm.Open(postgres.Open(cfg.Dns), &gorm.Config{})
	if err != nil {
		logger.Error("❌ Lỗi kết nối tới Database", "error", err.Error())
		panic(err)
	}
	logger.Info("✅ Đã kết nối thành công tới Database PostgreSQL!")

	// 2. Khởi tạo Fiber App và tích hợp RequestID Middleware
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})
	app.Use(middlewares.RequestIDMiddleware())

	server := &Server{
		App:    app,
		DB:     db,
		Config: cfg,
	}

	return server
}

func (s *Server) Start() {
	port := s.Config.ServerPort
	if port == "" {
		port = "8000"
	}

	logger.Info("🚀 REST Server đang chạy", "port", port)
	err := s.App.Listen(fmt.Sprintf(":%s", port))
	if err != nil {
		logger.Error("❌ Khởi động server thất bại", "error", err.Error())
	}
}
