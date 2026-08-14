package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type AppConfig struct {
	ServerPort   string
	Dns          string
	AppSecret    string
	RedisAddress string
	SMTPHost     string
	SMTPPort     int
	SMTPUser     string
	SMTPPass     string
}

func LoadConfig() AppConfig {
	err := godotenv.Load()
	if err != nil {
		log.Println("Không tìm thấy file .env, sẽ sử dụng biến môi trường hệ thống")
	}

	// Đổi SMTPPort từ string sang int
	smtpPort := 587
	if portStr := os.Getenv("SMTP_PORT"); portStr != "" {
		fmt.Sscanf(portStr, "%d", &smtpPort)
	}

	return AppConfig{
		ServerPort:   os.Getenv("PORT"),
		Dns:          os.Getenv("DNS"),
		AppSecret:    os.Getenv("APP_SECRET"),
		RedisAddress: os.Getenv("REDIS_ADDRESS"),
		SMTPHost:     os.Getenv("SMTP_HOST"),
		SMTPPort:     smtpPort,
		SMTPUser:     os.Getenv("SMTP_USER"),
		SMTPPass:     os.Getenv("SMTP_PASS"),
	}
}
