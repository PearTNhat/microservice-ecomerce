package service

import (
	"context"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/dto"
	"ecomerce-service/pkg/rabbitmq"
	"ecomerce-service/pkg/redislock"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type mockCartRepositoryForOrderService struct {
	carts map[string]*domain.Cart
	items map[uint][]domain.CartItem
	seq   uint
}

func newMockCartRepositoryForOrderService() *mockCartRepositoryForOrderService {
	return &mockCartRepositoryForOrderService{
		carts: make(map[string]*domain.Cart),
		items: make(map[uint][]domain.CartItem),
		seq:   1,
	}
}

func (m *mockCartRepositoryForOrderService) GetCartByUserID(userID string) (*domain.Cart, error) {
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

func (m *mockCartRepositoryForOrderService) AddItem(cartID uint, item *domain.CartItem) error {
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

func (m *mockCartRepositoryForOrderService) UpdateItemQuantity(cartID uint, itemID uint, quantity int) error {
	return nil
}

func (m *mockCartRepositoryForOrderService) RemoveItem(cartID uint, itemID uint) error {
	return nil
}

func (m *mockCartRepositoryForOrderService) ClearCart(cartID uint) error {
	m.items[cartID] = []domain.CartItem{}
	return nil
}

type mockOrderRepositoryForOrderService struct {
	orders []*domain.Order
	seq    uint
}

func newMockOrderRepositoryForOrderService() *mockOrderRepositoryForOrderService {
	return &mockOrderRepositoryForOrderService{
		orders: []*domain.Order{},
		seq:    1,
	}
}

func (m *mockOrderRepositoryForOrderService) CreateOrder(order *domain.Order) error {
	order.ID = m.seq
	m.seq++
	for i := range order.Items {
		order.Items[i].ID = uint(i + 1)
		order.Items[i].OrderID = order.ID
	}
	m.orders = append(m.orders, order)
	return nil
}

func (m *mockOrderRepositoryForOrderService) FindByID(id uint) (*domain.Order, error) {
	for _, o := range m.orders {
		if o.ID == id {
			return o, nil
		}
	}
	return nil, nil
}

func (m *mockOrderRepositoryForOrderService) FindByOrderCode(orderCode string) (*domain.Order, error) {
	for _, o := range m.orders {
		if o.OrderCode == orderCode {
			return o, nil
		}
	}
	return nil, nil
}

func (m *mockOrderRepositoryForOrderService) FindByUserID(userID string, page int, limit int) ([]*domain.Order, int64, error) {
	var result []*domain.Order
	for _, o := range m.orders {
		if o.UserID == userID {
			result = append(result, o)
		}
	}
	return result, int64(len(result)), nil
}

func (m *mockOrderRepositoryForOrderService) UpdateStatus(orderID uint, status string) error {
	for _, o := range m.orders {
		if o.ID == orderID {
			o.OrderStatus = status
			return nil
		}
	}
	return nil
}

func (m *mockOrderRepositoryForOrderService) UpdatePaymentStatus(orderID uint, status string) error {
	for _, o := range m.orders {
		if o.ID == orderID {
			o.PaymentStatus = status
			return nil
		}
	}
	return nil
}

func TestOrderService_CreateOrder_DirectAndFromCart(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Không thể khởi chạy miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	cartRepo := newMockCartRepositoryForOrderService()
	orderRepo := newMockOrderRepositoryForOrderService()
	producer := rabbitmq.NewNoopRabbitMQProducer()

	svc := NewOrderService(orderRepo, cartRepo, nil, rdb, producer)
	ctx := context.Background()
	userID := "user-abc"

	// 1. Test tạo đơn trực tiếp thành công
	_ = redislock.SetStock(ctx, rdb, 100, 50)

	createReq := &dto.CreateOrderRequest{
		CustomerName:    "Lê Tuấn Nhật",
		CustomerEmail:   "nhat@example.com",
		CustomerPhone:   "0987654321",
		ShippingAddress: "123 Đường Công Nghệ, TP.HCM",
		PaymentMethod:   "COD",
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 100, Quantity: 2},
		},
	}

	orderResp, err := svc.CreateOrder(ctx, userID, createReq)
	if err != nil {
		t.Fatalf("Lỗi CreateOrder trực tiếp: %v", err)
	}
	if orderResp.OrderCode == "" || len(orderResp.Items) != 1 {
		t.Errorf("Tạo đơn hàng không hợp lệ: %+v", orderResp)
	}

	// 2. Test mua vượt quá tồn kho (Flash Sale Lock)
	overReq := &dto.CreateOrderRequest{
		CustomerName:    "Khách Mua Sỉ",
		CustomerEmail:   "si@example.com",
		CustomerPhone:   "0987654321",
		ShippingAddress: "Kho Tổng",
		PaymentMethod:   "COD",
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 100, Quantity: 999},
		},
	}

	_, err = svc.CreateOrder(ctx, userID, overReq)
	if err == nil {
		t.Error("Kỳ vọng lỗi hết hàng / tồn kho không đủ nhưng thành công")
	}

	// 3. Test tạo đơn từ Giỏ hàng
	_ = redislock.SetStock(ctx, rdb, 200, 10)
	cartSvc := NewCartService(cartRepo, nil)
	_, _ = cartSvc.AddToCart(ctx, userID, &dto.AddToCartRequest{
		ProductID: 200,
		Quantity:  3,
	})

	cartOrderReq := &dto.CreateOrderRequest{
		CustomerName:    "Lê Tuấn Nhật",
		CustomerEmail:   "nhat@example.com",
		CustomerPhone:   "0987654321",
		ShippingAddress: "123 Đường Công Nghệ, TP.HCM",
		PaymentMethod:   "VNPAY",
		FromCart:        true,
	}

	cartOrderResp, err := svc.CreateOrder(ctx, userID, cartOrderReq)
	if err != nil {
		t.Fatalf("Lỗi CreateOrder từ giỏ hàng: %v", err)
	}
	if cartOrderResp.TotalAmount <= 0 {
		t.Errorf("Tổng tiền không hợp lệ: %.2f", cartOrderResp.TotalAmount)
	}

	// Kiểm tra giỏ hàng sau khi đặt xong đã được dọn sạch
	cart, _ := cartSvc.GetCart(ctx, userID)
	if len(cart.Items) != 0 {
		t.Errorf("Kỳ vọng giỏ hàng rỗng sau khi checkout nhưng còn %d món", len(cart.Items))
	}
}

func TestOrderService_GetOrderAndStatus(t *testing.T) {
	cartRepo := newMockCartRepositoryForOrderService()
	orderRepo := newMockOrderRepositoryForOrderService()
	producer := rabbitmq.NewNoopRabbitMQProducer()

	svc := NewOrderService(orderRepo, cartRepo, nil, nil, producer)
	ctx := context.Background()
	userID := "user-xyz"

	// Tạo 1 đơn hàng
	orderReq := &dto.CreateOrderRequest{
		CustomerName:    "Người Mua",
		CustomerEmail:   "buyer@example.com",
		CustomerPhone:   "0123456789",
		ShippingAddress: "Địa chỉ nhận",
		PaymentMethod:   "COD",
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 1},
		},
	}
	created, _ := svc.CreateOrder(ctx, userID, orderReq)

	// 1. GetOrderByID - Chính chủ xem -> OK
	order, err := svc.GetOrderByID(ctx, created.ID, userID, "CUSTOMER")
	if err != nil {
		t.Fatalf("Lỗi GetOrderByID: %v", err)
	}
	if order.OrderCode != created.OrderCode {
		t.Errorf("Mã đơn hàng không khớp: %s != %s", order.OrderCode, created.OrderCode)
	}

	// 2. GetOrderByID - Người lạ xem -> Lỗi quyền
	_, err = svc.GetOrderByID(ctx, created.ID, "user-stranger", "CUSTOMER")
	if err == nil {
		t.Error("Kỳ vọng lỗi không có quyền xem đơn nhưng lại thành công")
	}

	// 3. GetOrderByID - Admin xem -> OK
	_, err = svc.GetOrderByID(ctx, created.ID, "user-admin", "ADMIN")
	if err != nil {
		t.Errorf("Admin phải được phép xem mọi đơn hàng: %v", err)
	}

	// 4. UpdateOrderStatus
	err = svc.UpdateOrderStatus(ctx, created.ID, &dto.UpdateOrderStatusRequest{
		Status: domain.OrderStatusConfirmed,
	})
	if err != nil {
		t.Fatalf("Lỗi UpdateOrderStatus: %v", err)
	}

	updated, _ := svc.GetOrderByID(ctx, created.ID, userID, "CUSTOMER")
	if updated.OrderStatus != domain.OrderStatusConfirmed {
		t.Errorf("Kỳ vọng trạng thái CONFIRMED nhưng nhận %s", updated.OrderStatus)
	}
}

type mockProductRepositoryForOrderService struct {
	products map[uint]*domain.Product
}

func newMockProductRepositoryForOrderService() *mockProductRepositoryForOrderService {
	return &mockProductRepositoryForOrderService{
		products: make(map[uint]*domain.Product),
	}
}

func (m *mockProductRepositoryForOrderService) FindAll(filter domain.ProductFilter) ([]*domain.Product, int64, error) {
	return nil, 0, nil
}
func (m *mockProductRepositoryForOrderService) FindById(id uint) (*domain.Product, error) {
	p, exists := m.products[id]
	if !exists {
		return nil, nil
	}
	return p, nil
}
func (m *mockProductRepositoryForOrderService) FindByIds(ids []uint) ([]*domain.Product, error) {
	return nil, nil
}
func (m *mockProductRepositoryForOrderService) FindBySlug(slug string) (*domain.Product, error) {
	return nil, nil
}
func (m *mockProductRepositoryForOrderService) FindCategories() ([]*domain.Category, error) {
	return nil, nil
}
func (m *mockProductRepositoryForOrderService) FindBrands() ([]*domain.Brand, error) {
	return nil, nil
}
func (m *mockProductRepositoryForOrderService) CreateProduct(product *domain.Product) error {
	return nil
}
func (m *mockProductRepositoryForOrderService) CreateCategory(category *domain.Category) error {
	return nil
}
func (m *mockProductRepositoryForOrderService) CreateBrand(brand *domain.Brand) error {
	return nil
}
func (m *mockProductRepositoryForOrderService) IncrementViews(id uint) error {
	return nil
}
func (m *mockProductRepositoryForOrderService) BatchIncrementViews(viewCounts map[uint]int64) error {
	return nil
}
func (m *mockProductRepositoryForOrderService) Count() (int64, error) {
	return int64(len(m.products)), nil
}

func TestOrderService_MultiItemRollback_WhenOneItemFails(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Không thể khởi chạy miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cartRepo := newMockCartRepositoryForOrderService()
	orderRepo := newMockOrderRepositoryForOrderService()
	producer := rabbitmq.NewNoopRabbitMQProducer()

	svc := NewOrderService(orderRepo, cartRepo, nil, rdb, producer)
	ctx := context.Background()
	userID := "user-rollback-test"

	// Sản phẩm 1 có tồn kho = 10 trên Redis
	_ = redislock.SetStock(ctx, rdb, 1, 10)
	// Sản phẩm 2 có tồn kho = 0 trên Redis (hết hàng)
	_ = redislock.SetStock(ctx, rdb, 2, 0)

	// Đặt mua cả sản phẩm 1 (số lượng 2) và sản phẩm 2 (số lượng 1)
	req := &dto.CreateOrderRequest{
		CustomerName:    "Khách Hàng Test",
		CustomerEmail:   "test@example.com",
		CustomerPhone:   "0123456789",
		ShippingAddress: "Địa chỉ nhận hàng",
		PaymentMethod:   "COD",
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 2},
			{ProductID: 2, Quantity: 1},
		},
	}

	_, err = svc.CreateOrder(ctx, userID, req)
	if err == nil {
		t.Fatal("Kỳ vọng đơn hàng thất bại do sản phẩm 2 hết hàng, nhưng lại thành công")
	}

	// KIỂM TRA ROLLBACK: Tồn kho của sản phẩm 1 trên Redis PHẢI ĐƯỢC HOÀN LẠI VỀ 10
	stock1, err := redislock.GetStock(ctx, rdb, 1)
	if err != nil {
		t.Fatalf("Lỗi GetStock: %v", err)
	}
	if stock1 != 10 {
		t.Errorf("Kỳ vọng tồn kho sản phẩm 1 được hoàn lại là 10 sau khi rollback, nhưng thực tế còn: %d", stock1)
	}
}

func TestOrderService_LazyLoadingStock_FromDB(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Không thể khởi chạy miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cartRepo := newMockCartRepositoryForOrderService()
	orderRepo := newMockOrderRepositoryForOrderService()
	prodRepo := newMockProductRepositoryForOrderService()
	producer := rabbitmq.NewNoopRabbitMQProducer()

	// Sản phẩm ID = 300 có tồn kho = 25 trong PostgreSQL DB, nhưng CHƯA CÓ TRÊN REDIS (Cache Miss)
	prodRepo.products[300] = &domain.Product{
		ID:    300,
		Name:  "Máy Giặt Bosch Inverter",
		Slug:  "may-giat-bosch-inverter",
		Price: 15000000,
		Stock: 25,
	}

	svc := NewOrderService(orderRepo, cartRepo, prodRepo, rdb, producer)
	ctx := context.Background()
	userID := "user-lazy-test"

	req := &dto.CreateOrderRequest{
		CustomerName:    "Khách Hàng Lazy",
		CustomerEmail:   "lazy@example.com",
		CustomerPhone:   "0123456789",
		ShippingAddress: "Địa chỉ giao hàng",
		PaymentMethod:   "COD",
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 300, Quantity: 3},
		},
	}

	orderResp, err := svc.CreateOrder(ctx, userID, req)
	if err != nil {
		t.Fatalf("Lỗi tạo đơn hàng có cơ chế Lazy Loading: %v", err)
	}
	if orderResp.OrderCode == "" {
		t.Errorf("Tạo đơn hàng không thành công: %+v", orderResp)
	}

	// KIỂM TRA: Tồn kho trên Redis bây giờ phải tự động nạp từ DB (25) và trừ đi 3 = 22
	stockOnRedis, err := redislock.GetStock(ctx, rdb, 300)
	if err != nil {
		t.Fatalf("Lỗi GetStock trên Redis sau Lazy Load: %v", err)
	}
	if stockOnRedis != 22 {
		t.Errorf("Kỳ vọng tồn kho trên Redis sau khi Lazy Load & Deduct là 22, nhưng thực tế là: %d", stockOnRedis)
	}
}

