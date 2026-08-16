package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hibiken/asynq"
	"gopkg.in/gomail.v2"
)

const TaskSendVerifyEmail = "task:send_verify_email"

type PayloadSendVerifyEmail struct {
	Email string `json:"email"`
	Code  int    `json:"code"`
}

// Hàm đẩy Task vào Asynq (Distributor gọi hàm này)
func (distributor *RedisTaskDistributor) DistributeTaskSendVerifyEmail(
	ctx context.Context,
	payload *PayloadSendVerifyEmail,
	opts ...asynq.Option,
) error {
	// Đóng gói JSON
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal task payload: %w", err)
	}

	// Tạo Task mới
	task := asynq.NewTask(TaskSendVerifyEmail, jsonPayload, opts...)

	// Đẩy vào Asynq Queue (Redis)
	info, err := distributor.client.EnqueueContext(ctx, task)
	if err != nil {
		return fmt.Errorf("failed to enqueue task: %w", err)
	}

	fmt.Printf("✅ [Asynq] Đã đẩy Task gửi mail vào hàng đợi: id=%s queue=%s\n", info.ID, info.Queue)
	return nil
}

// Hàm nhận Task ra xử lý (Processor gọi hàm này)
func (processor *RedisTaskProcessor) ProcessTaskSendVerifyEmail(ctx context.Context, task *asynq.Task) error {
	var payload PayloadSendVerifyEmail
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	fmt.Printf("🚀🚀🚀 [Worker Đang Chạy Ngầm] Gửi mã xác thực [%d] tới email [%s]...\n", payload.Code, payload.Email)

	m := gomail.NewMessage()
	m.SetHeader("From", processor.config.SMTPUser)
	m.SetHeader("To", payload.Email)
	m.SetHeader("Subject", "Mã xác thực tài khoản E-commerce")
	m.SetBody("text/html", fmt.Sprintf(`
		<div style="font-family: Arial, sans-serif; padding: 20px;">
			<h2 style="color: #4A90E2;">Xác thực tài khoản</h2>
			<p>Chào bạn,</p>
			<p>Mã xác thực tài khoản E-commerce của bạn là:</p>
			<h1 style="color: #D0021B; letter-spacing: 5px;">%d</h1>
			<p>Mã này có hiệu lực trong vòng <strong>15 phút</strong>. Vui lòng không chia sẻ mã này cho bất kỳ ai.</p>
		</div>
	`, payload.Code))

	d := gomail.NewDialer(processor.config.SMTPHost, processor.config.SMTPPort, processor.config.SMTPUser, processor.config.SMTPPass)

	if err := d.DialAndSend(m); err != nil {
		return fmt.Errorf("không thể gửi email: %w", err)
	}

	fmt.Printf("✅✅✅ [Worker Hoàn Thành] Đã gửi mail thành công cho [%s]\n", payload.Email)
	return nil
}
