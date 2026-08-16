package handlers

import (
	"context"
	"ecomerce-service/internal/api/grpc/pb"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/dto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UserGrpcHandler triển khai interface pb.UserServiceServer
type UserGrpcHandler struct {
	pb.UnimplementedUserServiceServer
	svc *service.UserService
}

func NewUserGrpcHandler(svc *service.UserService) *UserGrpcHandler {
	return &UserGrpcHandler{
		svc: svc,
	}
}

func (h *UserGrpcHandler) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	// 1. Map Protobuf Message -> DTO
	input := dto.UserSignup{
		UserLogin: dto.UserLogin{
			Email:    req.GetEmail(),
			Password: req.GetPassword(),
		},
	}

	// 2. Gọi Service cốt lõi (Tái sử dụng 100% logic đã viết cho REST)
	token, err := h.svc.Signup(input)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Lỗi đăng ký: %v", err)
	}

	// 3. Trả về Protobuf Message
	return &pb.RegisterResponse{
		Token: token,
	}, nil
}

func (h *UserGrpcHandler) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
	// 1. Map Protobuf Message -> DTO
	input := dto.UserLogin{
		Email:    req.GetEmail(),
		Password: req.GetPassword(),
	}

	// 2. Gọi Service cốt lõi
	token, err := h.svc.Login(input)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "Đăng nhập thất bại: %v", err)
	}

	// 3. Trả về Protobuf Message
	return &pb.LoginResponse{
		Token: token,
	}, nil
}
