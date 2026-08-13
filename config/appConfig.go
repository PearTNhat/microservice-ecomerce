package config

import (
	"github.com/joho/godotenv"
	"log"
	"os"
)

type AppConfig struct {
	ServerPort string
	DBUrl      string
}

func LoadConfig() AppConfig {
	err := godotenv.Load()
	if err != nil {
		log.Println("Không tìm thấy file .env, sẽ sử dụng biến môi trường hệ thống")
	}

	return AppConfig{
		ServerPort: os.Getenv("PORT"),
		DBUrl:      os.Getenv("DB_URL"),
	}
}
