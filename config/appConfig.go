package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type AppConfig struct {
	ServerPort string
	Dns        string
	AppSecret  string
}

func LoadConfig() AppConfig {
	err := godotenv.Load()
	if err != nil {
		log.Println("Không tìm thấy file .env, sẽ sử dụng biến môi trường hệ thống")
	}
	return AppConfig{
		ServerPort: os.Getenv("PORT"),
		Dns:        os.Getenv("DNS"),
		AppSecret:  os.Getenv("APP_SECRET"),
	}
}
