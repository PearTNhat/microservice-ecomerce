package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"ecomerce-service/config"
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/worker"

	"github.com/alicebob/miniredis/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// ==========================================
// 1. TẠO MOCK REPOSITORY & MOCK TASK DISTRIBUTOR
// ==========================================
// Đây là kỹ thuật cực kỳ quan trọng trong Clean Architecture.
// Thay vì kết nối Postgres/Redis thật, chúng ta tạo mock để test độc lập.
type mockUserRepository struct {
	users map[string]*domain.User
}

func (m *mockUserRepository) CreateUser(user *domain.User) error {
	user.ID = 1 // Giả lập DB tự động tăng ID
	m.users[user.Email] = user
	return nil
}

func (m *mockUserRepository) FindUserByEmail(email string) (*domain.User, error) {
	if user, ok := m.users[email]; ok {
		return user, nil
	}
	return nil, nil // Trả về nil nếu không tìm thấy
}

func (m *mockUserRepository) FindUserById(id uint) (*domain.User, error) { return nil, nil }
func (m *mockUserRepository) UpdateUser(user *domain.User) error         { return nil }

type mockTaskDistributor struct{}

func (m *mockTaskDistributor) DistributeTaskSendVerifyEmail(
	ctx context.Context,
	payload *worker.PayloadSendVerifyEmail,
	opts ...asynq.Option,
) error {
	return nil
}

// ==========================================
// 2. VIẾT UNIT TEST CHO TÍNH NĂNG ĐĂNG KÝ
// ==========================================
func TestRegister_Success(t *testing.T) {
	// Khởi tạo miniredis cho môi trường test
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Không thể khởi động miniredis: %v", err)
	}
	defer mr.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer redisClient.Close()

	// Chuẩn bị môi trường Test
	app := fiber.New()
	rh := &rest.RestHandler{App: app}

	// Tiêm DB giả, Mock Task Distributor và Redis Client vào Service
	mockRepo := &mockUserRepository{users: make(map[string]*domain.User)}
	mockDistributor := &mockTaskDistributor{}
	mockConfig := config.AppConfig{AppSecret: "secret-test-key"}
	userService := service.NewUserService(mockRepo, mockConfig, mockDistributor, redisClient)

	// Khởi tạo routes
	SetupUserRoutes(rh, userService)

	// Tạo dữ liệu test
	reqBody := []byte(`{"email": "test@gmail.com", "password": "123", "phone": "098"}`)
	req := httptest.NewRequest("POST", "/register", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")

	// Chạy thử Request
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatalf("Lỗi khi chạy request test: %v", err)
	}

	// 3. KIỂM TRA KẾT QUẢ TỰ ĐỘNG
	if resp.StatusCode != 200 {
		t.Errorf("Kỳ vọng status 200 nhưng nhận được %d", resp.StatusCode)
	}

	// Đọc nội dung JSON trả về xem có chứa message phản hồi không
	body, _ := io.ReadAll(resp.Body)
	var responseData map[string]interface{}
	json.Unmarshal(body, &responseData)

	if msg, hasMsg := responseData["message"]; !hasMsg || msg == "" {
		t.Errorf("Đăng ký thành công nhưng không thấy trả về message xác thực! Body: %s", string(body))
	}
}
