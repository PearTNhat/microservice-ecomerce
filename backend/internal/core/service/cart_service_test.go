package service

import (
	"context"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/dto"
	"testing"
)

type mockCartRepositoryForCartService struct {
	carts map[string]*domain.Cart
	items map[uint][]domain.CartItem
	seq   uint
}

func newMockCartRepositoryForCartService() *mockCartRepositoryForCartService {
	return &mockCartRepositoryForCartService{
		carts: make(map[string]*domain.Cart),
		items: make(map[uint][]domain.CartItem),
		seq:   1,
	}
}

func (m *mockCartRepositoryForCartService) GetCartByUserID(userID string) (*domain.Cart, error) {
	cart, exists := m.carts[userID]
	if !exists {
		cart = &domain.Cart{
			ID:     m.seq,
			UserID: userID,
			Items:  []domain.CartItem{},
		}
		m.carts[userID] = cart
		m.items[cart.ID] = []domain.CartItem{}
		m.seq++
	}
	cart.Items = m.items[cart.ID]
	return cart, nil
}

func (m *mockCartRepositoryForCartService) AddItem(cartID uint, item *domain.CartItem) error {
	items := m.items[cartID]
	for i, it := range items {
		if it.ProductID == item.ProductID {
			items[i].Quantity += item.Quantity
			m.items[cartID] = items
			return nil
		}
	}
	item.ID = uint(len(items) + 1)
	item.CartID = cartID
	m.items[cartID] = append(items, *item)
	return nil
}

func (m *mockCartRepositoryForCartService) UpdateItemQuantity(cartID uint, itemID uint, quantity int) error {
	items := m.items[cartID]
	for i, it := range items {
		if it.ID == itemID {
			if quantity <= 0 {
				return m.RemoveItem(cartID, itemID)
			}
			items[i].Quantity = quantity
			m.items[cartID] = items
			return nil
		}
	}
	return nil
}

func (m *mockCartRepositoryForCartService) RemoveItem(cartID uint, itemID uint) error {
	items := m.items[cartID]
	var newItems []domain.CartItem
	for _, it := range items {
		if it.ID != itemID {
			newItems = append(newItems, it)
		}
	}
	m.items[cartID] = newItems
	return nil
}

func (m *mockCartRepositoryForCartService) ClearCart(cartID uint) error {
	m.items[cartID] = []domain.CartItem{}
	return nil
}

func TestCartService_CRUD(t *testing.T) {
	repo := newMockCartRepositoryForCartService()
	svc := NewCartService(repo, nil)
	ctx := context.Background()
	userID := "user-cart-test"

	// 1. GetCart ban đầu rỗng
	cart, err := svc.GetCart(ctx, userID)
	if err != nil {
		t.Fatalf("Lỗi GetCart: %v", err)
	}
	if len(cart.Items) != 0 {
		t.Errorf("Kỳ vọng giỏ hàng rỗng nhưng có %d món", len(cart.Items))
	}

	// 2. AddToCart sản phẩm #1
	cart, err = svc.AddToCart(ctx, userID, &dto.AddToCartRequest{
		ProductID: 1,
		Quantity:  2,
	})
	if err != nil {
		t.Fatalf("Lỗi AddToCart: %v", err)
	}
	if len(cart.Items) != 1 || cart.TotalItems != 2 {
		t.Errorf("Kỳ vọng 1 món với tổng số lượng 2 nhưng nhận total_items=%d", cart.TotalItems)
	}

	// 3. UpdateCartItem
	itemID := cart.Items[0].ID
	cart, err = svc.UpdateCartItem(ctx, userID, itemID, 5)
	if err != nil {
		t.Fatalf("Lỗi UpdateCartItem: %v", err)
	}
	if cart.TotalItems != 5 {
		t.Errorf("Kỳ vọng total_items=5 sau khi update nhưng nhận %d", cart.TotalItems)
	}

	// 4. RemoveCartItem
	cart, err = svc.RemoveCartItem(ctx, userID, itemID)
	if err != nil {
		t.Fatalf("Lỗi RemoveCartItem: %v", err)
	}
	if len(cart.Items) != 0 {
		t.Errorf("Kỳ vọng giỏ rỗng sau khi remove nhưng còn %d món", len(cart.Items))
	}
}
