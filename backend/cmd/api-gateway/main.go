package main

import (
	"ecomerce-service/config"
	"ecomerce-service/internal/api/rest/middlewares"
	"ecomerce-service/pkg/logger"
	"fmt"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/fiber/v2/middleware/proxy"
)

func main() {
	appConfig := config.LoadConfig()

	gatewayPort := os.Getenv("GATEWAY_PORT")
	if gatewayPort == "" {
		gatewayPort = "8000"
	}

	userServiceURL := os.Getenv("USER_SERVICE_URL")
	if userServiceURL == "" {
		userServiceURL = "http://localhost:8001"
	}

	productServiceURL := os.Getenv("PRODUCT_SERVICE_URL")
	if productServiceURL == "" {
		productServiceURL = "http://localhost:8002"
	}

	orderServiceURL := os.Getenv("ORDER_SERVICE_URL")
	if orderServiceURL == "" {
		orderServiceURL = "http://localhost:8003"
	}

	// 1. Khởi tạo Structured Logger
	logger.InitLogger("api-gateway", appConfig.Environment, appConfig.GraylogAddress)
	logger.Info("🌐 Khởi động API Gateway (Reverse Proxy)",
		"port", gatewayPort,
		"user_service", userServiceURL,
		"product_service", productServiceURL,
		"order_service", orderServiceURL,
	)

	// 2. Khởi tạo Fiber Gateway App
	app := fiber.New(fiber.Config{
		AppName:               "E-Commerce Microservices API Gateway",
		DisableStartupMessage: true,
	})

	// 3. Middlewares toàn cục
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowHeaders: "Origin, Content-Type, Accept, Authorization, X-Request-ID, X-Idempotency-Key, Idempotency-Key",
		AllowMethods: "GET, POST, PUT, DELETE, OPTIONS",
	}))

	app.Use(middlewares.RequestIDMiddleware())

	// Rate Limiter: Tối đa 100 requests / 1 phút cho mỗi IP
	app.Use(limiter.New(limiter.Config{
		Max:        100,
		Expiration: 1 * time.Minute,
		LimitReached: func(c *fiber.Ctx) error {
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"status":  "error",
				"message": "Quá nhiều yêu cầu. Vui lòng thử lại sau 1 phút.",
			})
		},
	}))

	// 4. Health Check
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":    "healthy",
			"gateway":   "running",
			"timestamp": time.Now().Format(time.RFC3339),
			"services": fiber.Map{
				"user_service":    userServiceURL,
				"product_service": productServiceURL,
				"order_service":   orderServiceURL,
			},
		})
	})

	// 5. REVERSE PROXY ROUTING: Chuyển tiếp tới User Microservice (Port 8001)
	userRoutes := []string{
		"/register",
		"/login",
		"/verify-email",
		"/user",
		"/user/*",
	}
	for _, route := range userRoutes {
		targetRoute := route
		app.All(targetRoute, func(c *fiber.Ctx) error {
			targetURL := userServiceURL + c.OriginalURL()
			return proxy.Do(c, targetURL)
		})
	}

	// 6. REVERSE PROXY ROUTING: Chuyển tiếp tới Product Microservice (Port 8002)
	productRoutes := []string{
		"/categories",
		"/brands",
		"/products",
		"/products/*",
	}
	for _, route := range productRoutes {
		targetRoute := route
		app.All(targetRoute, func(c *fiber.Ctx) error {
			targetURL := productServiceURL + c.OriginalURL()
			return proxy.Do(c, targetURL)
		})
	}

	// 7. REVERSE PROXY ROUTING: Chuyển tiếp tới Order Microservice (Port 8003)
	orderRoutes := []string{
		"/cart",
		"/cart/*",
		"/orders",
		"/orders/*",
	}
	for _, route := range orderRoutes {
		targetRoute := route
		app.All(targetRoute, func(c *fiber.Ctx) error {
			targetURL := orderServiceURL + c.OriginalURL()
			return proxy.Do(c, targetURL)
		})
	}

	// 7. Fallback 404
	app.Use(func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"status":  "error",
			"message": "Đường dẫn không tồn tại trên API Gateway (404 Not Found)",
			"path":    c.Path(),
		})
	})

	// 8. Chạy Gateway Server
	logger.Info(fmt.Sprintf("🚀 API Gateway đang lắng nghe tại http://localhost:%s", gatewayPort))
	if err := app.Listen(fmt.Sprintf(":%s", gatewayPort)); err != nil {
		logger.Error("❌ Không thể khởi động API Gateway", "error", err.Error())
	}
}
