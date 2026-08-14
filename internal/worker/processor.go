package worker

import (
	"context"
	"ecomerce-service/config"
	"log"

	"github.com/hibiken/asynq"
)

// TaskProcessor là interface chịu trách nhiệm nhận task từ Redis và xử lý
type TaskProcessor interface {
	Start() error
	ProcessTaskSendVerifyEmail(ctx context.Context, task *asynq.Task) error
}

type RedisTaskProcessor struct {
	server *asynq.Server
	config config.AppConfig
}

func NewRedisTaskProcessor(redisOpt asynq.RedisClientOpt, appConfig config.AppConfig) TaskProcessor {
	// Cấu hình Server (Worker) của Asynq
	server := asynq.NewServer(
		redisOpt,
		asynq.Config{
			// Khai báo số lượng Goroutines chạy song song để bốc task (Concurrency)
			Concurrency: 10,
			// Hàm xử lý lỗi nếu task bị fail
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
				log.Printf("❌ [Worker Lỗi] Xử lý task bị lỗi: type=%q, error=%v", task.Type(), err)
			}),
		},
	)

	return &RedisTaskProcessor{
		server: server,
		config: appConfig,
	}
}

// Start() dùng để map Task với Hàm xử lý và khởi động Worker Server
func (processor *RedisTaskProcessor) Start() error {
	mux := asynq.NewServeMux()
	// Đăng ký: Hễ gặp task có tên "TaskSendVerifyEmail" thì quăng cho hàm "ProcessTaskSendVerifyEmail" xử lý
	mux.HandleFunc(TaskSendVerifyEmail, processor.ProcessTaskSendVerifyEmail)

	log.Println("🚀 Bắt đầu khởi động Asynq Worker Server chạy ngầm...")
	return processor.server.Start(mux)
}
