package handlers

import (
	"bytes"
	"ecomerce-service/config"
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/core/service"
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
)

// ==========================================
// 1. TẠO MỘT DATABASE GIẢ (MOCK REPOSITORY)
// ==========================================
// Đây là kỹ thuật cực kỳ quan trọng trong Clean Architecture.
// Thay vì kết nối Postgres thật, chúng ta tạo một DB giả lưu trên RAM.
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

// ==========================================
// 2. VIẾT UNIT TEST CHO TÍNH NĂNG ĐĂNG KÝ
// ==========================================
func TestRegister_Success(t *testing.T) {
	// Chuẩn bị môi trường Test
	app := fiber.New()
	rh := &rest.RestHandler{App: app}

	// Tiêm DB giả vào Service thay vì DB thật
	mockRepo := &mockUserRepository{users: make(map[string]*domain.User)}
	mockConfig := config.AppConfig{AppSecret: "secret-test-key"}
	userService := service.NewUserService(mockRepo, mockConfig)

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

	// Đọc nội dung JSON trả về xem có chứa Token không
	body, _ := io.ReadAll(resp.Body)
	var responseData map[string]interface{}
	json.Unmarshal(body, &responseData)

	if _, hasToken := responseData["token"]; !hasToken {
		t.Errorf("Đăng ký thành công nhưng không thấy trả về Token! Body: %s", string(body))
	}
}
