package service

import (
	"ecomerce-service/config"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/dto"
	"ecomerce-service/pkg/utils"
	"errors"
)

type UserService struct {
	repo   domain.UserRepository
	config config.AppConfig
}

func NewUserService(repo domain.UserRepository, cfg config.AppConfig) *UserService {
	return &UserService{
		repo:   repo,
		config: cfg,
	}
}

func (s UserService) Signup(input dto.UserSignup) (string, error) {
	// 1. Kiểm tra email đã tồn tại chưa
	existingUser, _ := s.repo.FindUserByEmail(input.Email)
	if existingUser != nil {
		return "", errors.New("email đã được sử dụng")
	}

	// 2. Mã hóa mật khẩu
	hashedPassword, err := utils.HashPassword(input.Password)
	if err != nil {
		return "", err
	}

	// 3. Tạo User mới
	user := &domain.User{
		Email:    input.Email,
		Password: hashedPassword,
		Phone:    input.Phone,
	}

	err = s.repo.CreateUser(user)
	if err != nil {
		return "", err
	}

	// 4. Tạo JWT Token
	token, err := utils.GenerateToken(user.ID, s.config.AppSecret)
	if err != nil {
		return "", err
	}

	return token, nil
}

func (s UserService) findUserByEmail(email string) (*domain.User, error) {
	return s.repo.FindUserByEmail(email)
}

func (s UserService) Login(input dto.UserLogin) (string, error) {
	// 1. Tìm user bằng email
	user, err := s.repo.FindUserByEmail(input.Email)
	if err != nil {
		return "", errors.New("tài khoản không tồn tại")
	}

	// 2. Kiểm tra mật khẩu
	if !utils.CheckPasswordHash(input.Password, user.Password) {
		return "", errors.New("mật khẩu không chính xác")
	}

	// 3. Tạo JWT Token
	token, err := utils.GenerateToken(user.ID, s.config.AppSecret)
	if err != nil {
		return "", err
	}

	return token, nil
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
