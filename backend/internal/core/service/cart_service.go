package service

import (
	"context"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/dto"
	"errors"
	"fmt"
)

type CartService interface {
	GetCart(ctx context.Context, userID string) (*dto.CartResponse, error)
	AddToCart(ctx context.Context, userID string, req *dto.AddToCartRequest) (*dto.CartResponse, error)
	UpdateCartItem(ctx context.Context, userID string, itemID uint, quantity int) (*dto.CartResponse, error)
	RemoveCartItem(ctx context.Context, userID string, itemID uint) (*dto.CartResponse, error)
	ClearCart(ctx context.Context, userID string) error
}

type cartService struct {
	cartRepo    domain.CartRepository
	productRepo domain.ProductRepository
}

func NewCartService(cartRepo domain.CartRepository, productRepo domain.ProductRepository) CartService {
	return &cartService{
		cartRepo:    cartRepo,
		productRepo: productRepo,
	}
}

func (s *cartService) GetCart(ctx context.Context, userID string) (*dto.CartResponse, error) {
	cart, err := s.cartRepo.GetCartByUserID(userID)
	if err != nil {
		return nil, fmt.Errorf("lỗi lấy giỏ hàng: %w", err)
	}

	return s.mapCartToResponse(cart), nil
}

func (s *cartService) AddToCart(ctx context.Context, userID string, req *dto.AddToCartRequest) (*dto.CartResponse, error) {
	if req.ProductID == 0 || req.Quantity <= 0 {
		return nil, errors.New("thông tin sản phẩm hoặc số lượng không hợp lệ")
	}

	cart, err := s.cartRepo.GetCartByUserID(userID)
	if err != nil {
		return nil, err
	}

	var name, slug, thumbnail string
	var price float64

	if s.productRepo != nil {
		prod, err := s.productRepo.FindById(req.ProductID)
		if err != nil || prod == nil {
			return nil, fmt.Errorf("sản phẩm không tồn tại hoặc đã bị xóa")
		}
		if prod.Stock < req.Quantity {
			return nil, fmt.Errorf("sản phẩm chỉ còn %d món trong kho", prod.Stock)
		}
		name = prod.Name
		slug = prod.Slug
		thumbnail = prod.Thumbnail
		price = prod.Price
		if prod.DiscountPrice > 0 {
			price = prod.DiscountPrice
		}
	} else {
		name = fmt.Sprintf("Sản phẩm #%d", req.ProductID)
		price = 100000
	}

	cartItem := &domain.CartItem{
		CartID:      cart.ID,
		ProductID:   req.ProductID,
		ProductName: name,
		ProductSlug: slug,
		Thumbnail:   thumbnail,
		Price:       price,
		Quantity:    req.Quantity,
	}

	if err := s.cartRepo.AddItem(cart.ID, cartItem); err != nil {
		return nil, fmt.Errorf("lỗi thêm vào giỏ hàng: %w", err)
	}

	return s.GetCart(ctx, userID)
}

func (s *cartService) UpdateCartItem(ctx context.Context, userID string, itemID uint, quantity int) (*dto.CartResponse, error) {
	cart, err := s.cartRepo.GetCartByUserID(userID)
	if err != nil {
		return nil, err
	}

	if err := s.cartRepo.UpdateItemQuantity(cart.ID, itemID, quantity); err != nil {
		return nil, fmt.Errorf("lỗi cập nhật số lượng: %w", err)
	}

	return s.GetCart(ctx, userID)
}

func (s *cartService) RemoveCartItem(ctx context.Context, userID string, itemID uint) (*dto.CartResponse, error) {
	cart, err := s.cartRepo.GetCartByUserID(userID)
	if err != nil {
		return nil, err
	}

	if err := s.cartRepo.RemoveItem(cart.ID, itemID); err != nil {
		return nil, fmt.Errorf("lỗi xóa sản phẩm khỏi giỏ: %w", err)
	}

	return s.GetCart(ctx, userID)
}

func (s *cartService) ClearCart(ctx context.Context, userID string) error {
	cart, err := s.cartRepo.GetCartByUserID(userID)
	if err != nil {
		return err
	}
	return s.cartRepo.ClearCart(cart.ID)
}

func (s *cartService) mapCartToResponse(cart *domain.Cart) *dto.CartResponse {
	if cart == nil {
		return &dto.CartResponse{Items: []dto.CartItemResponse{}}
	}

	var items []dto.CartItemResponse
	var totalPrice float64
	var totalItems int

	for _, item := range cart.Items {
		subtotal := item.Price * float64(item.Quantity)
		totalPrice += subtotal
		totalItems += item.Quantity

		items = append(items, dto.CartItemResponse{
			ID:          item.ID,
			ProductID:   item.ProductID,
			ProductName: item.ProductName,
			ProductSlug: item.ProductSlug,
			Thumbnail:   item.Thumbnail,
			Price:       item.Price,
			Quantity:    item.Quantity,
			Subtotal:    subtotal,
		})
	}

	return &dto.CartResponse{
		ID:         cart.ID,
		UserID:     cart.UserID,
		Items:      items,
		TotalItems: totalItems,
		TotalPrice: totalPrice,
	}
}
