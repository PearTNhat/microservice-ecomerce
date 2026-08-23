package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type AppConfig struct {
	ServerPort           string
	Dns                  string
	UserDbDns            string
	ProductDbDns         string
	OrderDbDns           string
	AppSecret            string
	RedisAddress         string
	SMTPHost             string
	SMTPPort             int
	SMTPUser             string
	SMTPPass             string
	GraylogAddress       string
	Environment          string
	KafkaBrokers         string
	ElasticsearchAddress string
	RabbitMQURL          string
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

	env := os.Getenv("ENV")
	if env == "" {
		env = "development"
	}

	graylogAddr := os.Getenv("GRAYLOG_ADDR")
	if graylogAddr == "" {
		graylogAddr = "127.0.0.1:12201"
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}

	esAddr := os.Getenv("ELASTICSEARCH_ADDR")
	if esAddr == "" {
		esAddr = "http://localhost:9200"
	}

	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://guest:guest@localhost:5672/"
	}

	// Database per service default DNS
	defaultBaseDNS := os.Getenv("DNS")
	if defaultBaseDNS == "" {
		defaultBaseDNS = "host=localhost port=5428 user=root password=secret sslmode=disable"
	}

	userDbDns := os.Getenv("USER_DB_DNS")
	if userDbDns == "" {
		userDbDns = "host=localhost port=5428 user=root password=secret dbname=ecom_user_db sslmode=disable"
	}

	productDbDns := os.Getenv("PRODUCT_DB_DNS")
	if productDbDns == "" {
		productDbDns = "host=localhost port=5428 user=root password=secret dbname=ecom_product_db sslmode=disable"
	}

	orderDbDns := os.Getenv("ORDER_DB_DNS")
	if orderDbDns == "" {
		orderDbDns = "host=localhost port=5428 user=root password=secret dbname=ecom_order_db sslmode=disable"
	}

	return AppConfig{
		ServerPort:           os.Getenv("PORT"),
		Dns:                  defaultBaseDNS,
		UserDbDns:            userDbDns,
		ProductDbDns:         productDbDns,
		OrderDbDns:           orderDbDns,
		AppSecret:            os.Getenv("APP_SECRET"),
		RedisAddress:         os.Getenv("REDIS_ADDRESS"),
		SMTPHost:             os.Getenv("SMTP_HOST"),
		SMTPPort:             smtpPort,
		SMTPUser:             os.Getenv("SMTP_USER"),
		SMTPPass:             os.Getenv("SMTP_PASS"),
		GraylogAddress:       graylogAddr,
		Environment:          env,
		KafkaBrokers:         kafkaBrokers,
		ElasticsearchAddress: esAddr,
		RabbitMQURL:          rabbitmqURL,
	}
}
