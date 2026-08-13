package main

import (
	"ecomerce-service/config"
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/api/rest/handlers"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"log"
)

func main() {
	appConfig := config.LoadConfig()
	app := fiber.New()

	// Cấu hình RestHandler
	rh := &rest.RestHandler{
		App: app,
	}

	// Khởi tạo các routes cho User
	handlers.SetupUserRoutes(rh)

	port := appConfig.ServerPort
	if port == "" {
		port = "8000"
	}

	log.Printf("Server đang chạy tại port %s...\n", port)
	err := app.Listen(fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("Khởi động server thất bại: %v", err)
	}
}
