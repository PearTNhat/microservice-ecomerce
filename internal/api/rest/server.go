package rest

import (
	"ecomerce-service/config"
	"fmt"
	"log"

	"github.com/gofiber/fiber/v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Server struct {
	App    *fiber.App
	DB     *gorm.DB
	Config config.AppConfig
}

// RestHandler giữ nguyên cấu trúc cũ để không ảnh hưởng tới handlers
type RestHandler struct {
	App *fiber.App
}

func NewServer(cfg config.AppConfig) *Server {
	// 1. Khởi tạo kết nối Database bằng GORM
	db, err := gorm.Open(postgres.Open(cfg.Dns), &gorm.Config{})
	if err != nil {
		log.Fatalf("❌ Lỗi kết nối tới Database: %v", err)
	}
	log.Println("✅ Đã kết nối thành công tới Database PostgreSQL!")

	// 2. Khởi tạo Fiber App
	app := fiber.New()

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

	log.Printf("🚀 Server đang chạy tại port %s...\n", port)
	err := s.App.Listen(fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("❌ Khởi động server thất bại: %v", err)
	}
}
