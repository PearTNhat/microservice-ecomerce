package service

import (
	"context"
	"encoding/json"
	"ecomerce-service/config"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/dto"
	"ecomerce-service/internal/worker"
	"ecomerce-service/pkg/utils"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

type UserService struct {
	repo        domain.UserRepository
	config      config.AppConfig
	distributor worker.TaskDistributor
	redisClient *redis.Client
}

func NewUserService(repo domain.UserRepository, cfg config.AppConfig, distributor worker.TaskDistributor, rClient *redis.Client) *UserService {
	return &UserService{
		repo:        repo,
		config:      cfg,
		distributor: distributor,
		redisClient: rClient,
	}
}

type PendingUser struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Phone     string `json:"phone"`
	Email     string `json:"email"`
	Password  string `json:"password"`
	OTP       int    `json:"otp"`
}

func (s UserService) Signup(input dto.UserSignup) (string, error) {
	// 1. Kiểm tra email đã tồn tại trong DB chính chưa
	existingUser, _ := s.repo.FindUserByEmail(input.Email)
	if existingUser != nil {
		return "", errors.New("email đã được sử dụng")
	}

	// 2. Tạo mã OTP ngẫu nhiên
	otpCode, err := utils.GenerateRandomCode()
	if err != nil {
		return "", fmt.Errorf("không thể tạo mã xác thực: %w", err)
	}

	// 3. Mã hóa mật khẩu
	hashedPassword, err := utils.HashPassword(input.Password)
	if err != nil {
		return "", err
	}

	// 4. Lưu thông tin tạm vào Redis với TTL 15 phút
	pendingUser := PendingUser{
		FirstName: input.FirstName,
		LastName:  input.LastName,
		Phone:     input.Phone,
		Email:     input.Email,
		Password:  hashedPassword,
		OTP:       otpCode,
	}

	pendingBytes, _ := json.Marshal(pendingUser)
	redisKey := fmt.Sprintf("verify:user:%s", input.Email)

	err = s.redisClient.Set(context.Background(), redisKey, pendingBytes, 15*time.Minute).Err()
	if err != nil {
		return "", fmt.Errorf("lỗi server khi xử lý đăng ký: %w", err)
	}

	// 5. Tạo task gửi mail xác thực thông qua Asynq
	payload := &worker.PayloadSendVerifyEmail{
		Email: input.Email,
		Code:  otpCode,
	}
	opts := []asynq.Option{
		asynq.MaxRetry(5),
		asynq.ProcessIn(1 * time.Second),
		asynq.Queue("default"),
	}

	// Đẩy task chạy ngầm
	s.distributor.DistributeTaskSendVerifyEmail(context.Background(), payload, opts...)

	// Trả về thông báo thay vì JWT token
	return "Vui lòng kiểm tra email để nhận mã xác thực (có hiệu lực 15 phút)", nil
}

func (s UserService) VerifyEmail(email string, code int) (string, *domain.User, error) {
	redisKey := fmt.Sprintf("verify:user:%s", email)

	// 1. Lấy thông tin từ Redis
	val, err := s.redisClient.Get(context.Background(), redisKey).Result()
	if err == redis.Nil {
		return "", nil, errors.New("mã xác thực đã hết hạn hoặc email không chính xác")
	} else if err != nil {
		return "", nil, err
	}

	var pendingUser PendingUser
	if err := json.Unmarshal([]byte(val), &pendingUser); err != nil {
		return "", nil, errors.New("lỗi hệ thống khi đọc dữ liệu")
	}

	// 2. So sánh mã OTP
	if pendingUser.OTP != code {
		return "", nil, errors.New("mã xác thực không chính xác")
	}

	// 3. OTP hợp lệ -> Lưu vào PostgreSQL
	user := &domain.User{
		FirstName: pendingUser.FirstName,
		LastName:  pendingUser.LastName,
		Email:     pendingUser.Email,
		Password:  pendingUser.Password,
		UserType:  domain.RoleCustomer,
		Verified:  true,
	}

	err = s.repo.CreateUser(user)
	if err != nil {
		return "", nil, errors.New("không thể tạo tài khoản lúc này")
	}

	// 4. Xóa key trong Redis
	s.redisClient.Del(context.Background(), redisKey)

	// 5. Cấp JWT Token
	token, err := utils.GenerateTokenWithRole(user.ID, user.UserType, s.config.AppSecret)
	if err != nil {
		return "", nil, err
	}

	return token, user, nil
}

func (s UserService) findUserByEmail(email string) (*domain.User, error) {
	return s.repo.FindUserByEmail(email)
}

func (s UserService) Login(input dto.UserLogin) (string, *domain.User, error) {
	// 1. Tìm user bằng email
	user, err := s.repo.FindUserByEmail(input.Email)
	if err != nil {
		return "", nil, errors.New("tài khoản không tồn tại")
	}

	// 2. Kiểm tra mật khẩu
	if !utils.CheckPasswordHash(input.Password, user.Password) {
		return "", nil, errors.New("mật khẩu không chính xác")
	}

	// 3. Tạo JWT Token với Role
	userRole := user.UserType
	if userRole == "" {
		userRole = domain.RoleCustomer
	}
	token, err := utils.GenerateTokenWithRole(user.ID, userRole, s.config.AppSecret)
	if err != nil {
		return "", nil, err
	}

	return token, user, nil
}

func (s UserService) GetVerificationCode(e domain.User) (int, error) {

	return 0, nil
}

func (s UserService) VerifyCode(id uint, code int) error {

	return nil
}

func (s UserService) CreateProfile(id uint, input any) error {

	return nil
}

func (s UserService) GetProfile(id uint) (*domain.User, error) {

	return nil, nil
}

func (s UserService) UpdateProfile(id uint, input any) error {

	return nil
}

func (s UserService) BecomeSeller(id uint, input any) (string, error) {

	return "", nil
}

func (s UserService) FindCart(id uint) ([]interface{}, error) {

	return nil, nil
}

func (s UserService) CreateCart(input any, u domain.User) ([]interface{}, error) {

	return nil, nil
}

func (s UserService) CreateOrder(u domain.User) (int, error) {

	return 0, nil
}

func (s UserService) GetOrders(u domain.User) ([]interface{}, error) {

	return nil, nil
}

func (s UserService) GetOrderById(id uint, uId uint) (interface{}, error) {

	return nil, nil
}
