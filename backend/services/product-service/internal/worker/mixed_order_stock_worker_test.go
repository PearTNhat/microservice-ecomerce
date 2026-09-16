package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	pkgKafka "ecomerce-service/pkg/kafka"
	"ecomerce-service/services/product-service/internal/domain"
	"ecomerce-service/services/product-service/internal/repository"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type mockKafkaProducerForWorker struct {
	mu                sync.Mutex
	results           []pkgKafka.MixedOrderStockResultPayload
	compensateResults []pkgKafka.MixedOrderStockCompensateResultPayload
	deadLetters       []string
	rawPublished      []string
	publishError      error
}

func (m *mockKafkaProducerForWorker) PublishOrderCreated(ctx context.Context, payload pkgKafka.OrderCreatedPayload) error {
	return nil
}
func (m *mockKafkaProducerForWorker) PublishOrderPaid(ctx context.Context, orderID uint, amount float64) error {
	return nil
}
func (m *mockKafkaProducerForWorker) PublishOrderCancelled(ctx context.Context, orderID uint, reason string) error {
	return nil
}
func (m *mockKafkaProducerForWorker) PublishFlashSaleOrderTask(ctx context.Context, payload pkgKafka.FlashSaleOrderTaskPayload) error {
	return nil
}
func (m *mockKafkaProducerForWorker) PublishStockResult(ctx context.Context, payload pkgKafka.StockResultPayload) error {
	return nil
}
func (m *mockKafkaProducerForWorker) PublishMixedOrderStockRequest(ctx context.Context, payload pkgKafka.MixedOrderStockRequestPayload) error {
	return nil
}
func (m *mockKafkaProducerForWorker) PublishMixedOrderStockCompensate(ctx context.Context, payload pkgKafka.MixedOrderStockCompensatePayload) error {
	return nil
}
func (m *mockKafkaProducerForWorker) PublishMixedOrderStockCompensateResult(ctx context.Context, payload pkgKafka.MixedOrderStockCompensateResultPayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishError != nil {
		return m.publishError
	}
	m.compensateResults = append(m.compensateResults, payload)
	return nil
}
func (m *mockKafkaProducerForWorker) PublishMixedOrderStockResult(ctx context.Context, payload pkgKafka.MixedOrderStockResultPayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishError != nil {
		return m.publishError
	}
	m.results = append(m.results, payload)
	return nil
}
func (m *mockKafkaProducerForWorker) PublishDeadLetter(ctx context.Context, originalTopic, key string, rawPayload []byte, errReason string, traceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadLetters = append(m.deadLetters, errReason)
	return nil
}
func (m *mockKafkaProducerForWorker) PublishRaw(ctx context.Context, topic string, key string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishError != nil {
		return m.publishError
	}
	m.rawPublished = append(m.rawPublished, string(payload))
	return nil
}
func (m *mockKafkaProducerForWorker) Close() error {
	return nil
}

func setupTestWorkerDB(t *testing.T, dbName string) (*gorm.DB, *redis.Client, *miniredis.Miniredis) {
	// 20.2 (P1): Phân lập tuyệt đối SQLite in-memory bằng nano timestamp + UUID ngẫu nhiên để tránh đụng độ slug giữa các lần chạy lặp (-count=N)
	uniqueDBName := fmt.Sprintf("%s_%d_%s", dbName, time.Now().UnixNano(), uuid.New().String()[:8])
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared&_busy_timeout=10000", uniqueDBName)), &gorm.Config{})
	if err != nil {
		t.Fatalf("Không thể khởi tạo SQLite test DB: %v", err)
	}
	db.Exec("PRAGMA busy_timeout = 10000;")

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("Không thể lấy sql.DB: %v", err)
	}
	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	err = db.AutoMigrate(
		&domain.Product{},
		&domain.MixedOrderStockOperation{},
		&domain.ProcessedEvent{},
		&domain.ProductOutboxEvent{},
	)
	if err != nil {
		t.Fatalf("Lỗi AutoMigrate: %v", err)
	}

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Lỗi miniredis: %v", err)
	}
	t.Cleanup(func() {
		mr.Close()
	})
	rClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = rClient.Close()
	})

	return db, rClient, mr
}

// 16.2 Test: Compensate đến TRƯỚC Request (Out-of-order race condition)
// Đảm bảo không cộng khống tồn kho và request đến sau bị từ chối
func TestMixedOrderStockWorker_CompensateBeforeRequest(t *testing.T) {
	db, rClient, mr := setupTestWorkerDB(t, "test_comp_before_req")
	defer mr.Close()

	// Tạo sản phẩm ban đầu với stock = 10
	prod := domain.Product{
		Name:  "Test TV",
		Slug:  "test-tv",
		Price: 5000000,
		Stock: 10,
	}
	db.Create(&prod)

	mockProducer := &mockKafkaProducerForWorker{}
	opRepo := repository.NewMixedOrderStockOperationRepository(db)

	w := &MixedOrderStockWorker{
		db:          db,
		opRepo:      opRepo,
		redisClient: rClient,
		producer:    mockProducer,
	}

	orderID := uint(101)

	// BƯỚC 1: Compensate message đến trước!
	compPayload := pkgKafka.MixedOrderStockCompensatePayload{
		EventID:   "comp-evt-101",
		EventType: pkgKafka.EventMixedStockCompensate,
		OrderID:   orderID,
		OrderCode: "ORD-101",
		Items: []pkgKafka.OrderItemPayload{
			{ProductID: prod.ID, Quantity: 2},
		},
		Timestamp: time.Now(),
	}
	compBytes, _ := json.Marshal(compPayload)

	err := w.processCompensateMessage(context.Background(), kafka.Message{Value: compBytes})
	if err != nil {
		t.Fatalf("processCompensateMessage thất bại: %v", err)
	}

	// Kiểm tra: Trạng thái trong DB phải là CANCELLED_BEFORE_DEDUCT
	op, err := opRepo.GetByOrderID(nil, orderID)
	if err != nil || op == nil {
		t.Fatalf("Không tìm thấy operation sau khi compensate trước: %v", err)
	}
	if op.Status != domain.MixedStockOpCancelledBeforeDeduct {
		t.Errorf("Kỳ vọng status CANCELLED_BEFORE_DEDUCT, nhận: %s", op.Status)
	}

	// Tồn kho KHÔNG ĐƯỢC CỘNG KHỐNG! Vẫn phải là 10!
	var currentProd domain.Product
	db.First(&currentProd, prod.ID)
	if currentProd.Stock != 10 {
		t.Errorf("LỖI GHOST STOCK: Tồn kho bị cộng khống thành %d (kỳ vọng vẫn là 10)", currentProd.Stock)
	}

	// BƯỚC 2: Request message đến sau!
	reqPayload := pkgKafka.MixedOrderStockRequestPayload{
		EventID:   "req-evt-101",
		EventType: pkgKafka.EventMixedStockDeductRequest,
		OrderID:   orderID,
		OrderCode: "ORD-101",
		RegularItems: []pkgKafka.OrderItemPayload{
			{ProductID: prod.ID, ProductName: prod.Name, Quantity: 2},
		},
		Timestamp: time.Now(),
	}
	reqBytes, _ := json.Marshal(reqPayload)

	err = w.processMessage(context.Background(), kafka.Message{Value: reqBytes})
	if err != nil {
		t.Fatalf("processMessage thất bại: %v", err)
	}

	// Kiểm tra: Tồn kho KHÔNG ĐƯỢC TRỪ! Vẫn phải là 10!
	db.First(&currentProd, prod.ID)
	if currentProd.Stock != 10 {
		t.Errorf("Tồn kho bị trừ sai khi đơn đã CANCELLED_BEFORE_DEDUCT: %d", currentProd.Stock)
	}

	// Kết quả gửi ra Kafka phải là Success: false
	mockProducer.mu.Lock()
	defer mockProducer.mu.Unlock()
	if len(mockProducer.results) == 0 {
		t.Fatalf("Không nhận được kết quả phát ra Kafka")
	}
	lastResult := mockProducer.results[len(mockProducer.results)-1]
	if lastResult.Success {
		t.Errorf("Kỳ vọng kết quả thất bại vì đã hủy trước khi trừ kho, nhưng lại thành công")
	}
}

// 16.1 & 16.2 Test: Request trước, sau đó Compensate bình thường
func TestMixedOrderStockWorker_RequestThenCompensate(t *testing.T) {
	db, rClient, mr := setupTestWorkerDB(t, "test_req_then_comp")
	defer mr.Close()

	prod := domain.Product{
		Name:  "Test Laptop",
		Slug:  "test-laptop",
		Price: 15000000,
		Stock: 20,
	}
	db.Create(&prod)

	mockProducer := &mockKafkaProducerForWorker{}
	opRepo := repository.NewMixedOrderStockOperationRepository(db)

	w := &MixedOrderStockWorker{
		db:          db,
		opRepo:      opRepo,
		redisClient: rClient,
		producer:    mockProducer,
	}

	orderID := uint(102)

	// 1. Request trừ 3 cái
	reqPayload := pkgKafka.MixedOrderStockRequestPayload{
		EventID:   "req-evt-102",
		EventType: pkgKafka.EventMixedStockDeductRequest,
		OrderID:   orderID,
		OrderCode: "ORD-102",
		RegularItems: []pkgKafka.OrderItemPayload{
			{ProductID: prod.ID, ProductName: prod.Name, Quantity: 3},
		},
		Timestamp: time.Now(),
	}
	reqBytes, _ := json.Marshal(reqPayload)

	if err := w.processMessage(context.Background(), kafka.Message{Value: reqBytes}); err != nil {
		t.Fatalf("processMessage lỗi: %v", err)
	}

	var currentProd domain.Product
	db.First(&currentProd, prod.ID)
	if currentProd.Stock != 17 {
		t.Fatalf("Tồn kho sau khi trừ phải là 17, nhận: %d", currentProd.Stock)
	}

	op, _ := opRepo.GetByOrderID(nil, orderID)
	if op.Status != domain.MixedStockOpDeducted {
		t.Fatalf("Kỳ vọng status DEDUCTED, nhận: %s", op.Status)
	}

	// 2. Compensate hoàn trả
	compPayload := pkgKafka.MixedOrderStockCompensatePayload{
		EventID:   "comp-evt-102",
		EventType: pkgKafka.EventMixedStockCompensate,
		OrderID:   orderID,
		OrderCode: "ORD-102",
		Items: []pkgKafka.OrderItemPayload{
			{ProductID: prod.ID, Quantity: 3},
		},
		Timestamp: time.Now(),
	}
	compBytes, _ := json.Marshal(compPayload)

	if err := w.processCompensateMessage(context.Background(), kafka.Message{Value: compBytes}); err != nil {
		t.Fatalf("processCompensateMessage lỗi: %v", err)
	}

	db.First(&currentProd, prod.ID)
	if currentProd.Stock != 20 {
		t.Errorf("Tồn kho sau khi bồi hoàn phải về 20, nhận: %d", currentProd.Stock)
	}

	op, _ = opRepo.GetByOrderID(nil, orderID)
	if op.Status != domain.MixedStockOpCompensated {
		t.Errorf("Kỳ vọng status COMPENSATED, nhận: %s", op.Status)
	}

	// 3. Compensate lần 2 (Bản tin trùng lặp) -> Phải No-op, không được cộng thêm!
	if err := w.processCompensateMessage(context.Background(), kafka.Message{Value: compBytes}); err != nil {
		t.Fatalf("processCompensateMessage duplicate lỗi: %v", err)
	}

	db.First(&currentProd, prod.ID)
	if currentProd.Stock != 20 {
		t.Errorf("LỖI CỘNG LẶP: Tồn kho sau compensate lần 2 phải giữ nguyên 20, nhận: %d", currentProd.Stock)
	}
}

// 16.1 Test: Hai bản sao request chạy đồng thời (Concurrency / Double deduction test)
func TestMixedOrderStockWorker_DuplicateRequest_Concurrent(t *testing.T) {
	db, rClient, mr := setupTestWorkerDB(t, "test_concurrent_req")
	defer mr.Close()

	prod := domain.Product{
		Name:  "Test Keyboard",
		Slug:  fmt.Sprintf("test-keyboard-%d", time.Now().UnixNano()),
		Price: 1000000,
		Stock: 10,
	}
	if err := db.Create(&prod).Error; err != nil {
		t.Fatalf("Lỗi tạo sản phẩm ban đầu: %v", err)
	}

	mockProducer := &mockKafkaProducerForWorker{}
	opRepo := repository.NewMixedOrderStockOperationRepository(db)

	w := &MixedOrderStockWorker{
		db:          db,
		opRepo:      opRepo,
		redisClient: rClient,
		producer:    mockProducer,
	}

	orderID := uint(103)

	reqPayload1 := pkgKafka.MixedOrderStockRequestPayload{
		EventID:   "req-evt-103-A",
		EventType: pkgKafka.EventMixedStockDeductRequest,
		OrderID:   orderID,
		OrderCode: "ORD-103",
		RegularItems: []pkgKafka.OrderItemPayload{
			{ProductID: prod.ID, ProductName: prod.Name, Quantity: 2},
		},
		Timestamp: time.Now(),
	}
	reqBytes1, _ := json.Marshal(reqPayload1)

	reqPayload2 := pkgKafka.MixedOrderStockRequestPayload{
		EventID:   "req-evt-103-B", // Khác EventID nhưng cùng OrderID
		EventType: pkgKafka.EventMixedStockDeductRequest,
		OrderID:   orderID,
		OrderCode: "ORD-103",
		RegularItems: []pkgKafka.OrderItemPayload{
			{ProductID: prod.ID, ProductName: prod.Name, Quantity: 2},
		},
		Timestamp: time.Now(),
	}
	reqBytes2, _ := json.Marshal(reqPayload2)

	// 20.2 (P1): Thu thập lỗi từ 2 goroutine đồng thời qua channel.
	// Nếu gặp lỗi tranh chấp khóa bảng tạm thời của SQLite (database table is locked / SQLITE_LOCKED),
	// tiến hành retry ngắn tương tự cơ chế backoff của Kafka worker consumer loop.
	processWithKafkaRetry := func(val []byte) error {
		var lastErr error
		for attempt := 0; attempt < 10; attempt++ {
			err := w.processMessage(context.Background(), kafka.Message{Value: val})
			if err == nil {
				return nil
			}
			lastErr = err
			// Nếu gặp lỗi table lock tạm thời của SQLite shared-memory, chờ ngắn và retry
			if strings.Contains(err.Error(), "locked") || strings.Contains(err.Error(), "busy") {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return err
		}
		return lastErr
	}

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		errCh <- processWithKafkaRetry(reqBytes1)
	}()

	go func() {
		defer wg.Done()
		errCh <- processWithKafkaRetry(reqBytes2)
	}()

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}

	// Cả 2 goroutine phải thành công (1 thực hiện trừ kho, 1 idempotent replay)
	if len(errs) > 0 {
		t.Fatalf("Goroutine đồng thời gặp lỗi: %v", errs)
	}

	// Tồn kho chỉ được giảm đúng 1 lần: 10 - 2 = 8! Không được là 6!
	var currentProd domain.Product
	if err := db.First(&currentProd, prod.ID).Error; err != nil {
		t.Fatalf("Lỗi truy vấn sản phẩm sau 2 request song song: %v", err)
	}
	if currentProd.Stock != 8 {
		t.Fatalf("LỖI TRỪ KHO HAI LẦN (DOUBLE DEDUCTION): Tồn kho sau 2 request song song là %d, kỳ vọng 8", currentProd.Stock)
	}

	op, err := opRepo.GetByOrderID(nil, orderID)
	if err != nil || op == nil {
		t.Fatalf("Không tìm thấy operation sau test concurrent: %v", err)
	}
	if op.Status != domain.MixedStockOpDeducted {
		t.Fatalf("Trạng thái operation kỳ vọng DEDUCTED, nhận: %s", op.Status)
	}
}

// 16.1 Point 4 Test: Thiếu hàng trong giao dịch -> Rollback toàn bộ và lưu FAILED bền vững
func TestMixedOrderStockWorker_InsufficientStock_Rollback(t *testing.T) {
	db, rClient, mr := setupTestWorkerDB(t, "test_insufficient_stock")
	defer mr.Close()

	prod1 := domain.Product{Name: "Item 1", Slug: "item-1", Price: 100000, Stock: 5}
	prod2 := domain.Product{Name: "Item 2", Slug: "item-2", Price: 200000, Stock: 1} // Chỉ có 1
	db.Create(&prod1)
	db.Create(&prod2)

	mockProducer := &mockKafkaProducerForWorker{}
	opRepo := repository.NewMixedOrderStockOperationRepository(db)

	w := &MixedOrderStockWorker{
		db:          db,
		opRepo:      opRepo,
		redisClient: rClient,
		producer:    mockProducer,
	}

	orderID := uint(104)

	// Yêu cầu mua 3 cái mỗi món -> prod2 sẽ không đủ (cần 3 mà chỉ có 1)
	reqPayload := pkgKafka.MixedOrderStockRequestPayload{
		EventID:   "req-evt-104",
		EventType: pkgKafka.EventMixedStockDeductRequest,
		OrderID:   orderID,
		OrderCode: "ORD-104",
		RegularItems: []pkgKafka.OrderItemPayload{
			{ProductID: prod1.ID, ProductName: prod1.Name, Quantity: 3},
			{ProductID: prod2.ID, ProductName: prod2.Name, Quantity: 3},
		},
		Timestamp: time.Now(),
	}
	reqBytes, _ := json.Marshal(reqPayload)

	err := w.processMessage(context.Background(), kafka.Message{Value: reqBytes})
	if err != nil {
		t.Fatalf("processMessage không được trả error khi business failure (cần trả nil để commit và bắn result fail): %v", err)
	}

	// Kiểm tra: Cả 2 sản phẩm đều KHÔNG bị trừ kho (Item 1 phải được rollback về 5)
	var p1, p2 domain.Product
	db.First(&p1, prod1.ID)
	db.First(&p2, prod2.ID)
	if p1.Stock != 5 {
		t.Errorf("LỖI TRANSACTION: Item 1 không được rollback, còn lại %d (kỳ vọng 5)", p1.Stock)
	}
	if p2.Stock != 1 {
		t.Errorf("Item 2 bị thay đổi stock: %d", p2.Stock)
	}

	// Operation phải lưu trạng thái FAILED
	op, err := opRepo.GetByOrderID(nil, orderID)
	if err != nil || op == nil {
		t.Fatalf("Không tìm thấy operation sau failure: %v", err)
	}
	if op.Status != domain.MixedStockOpFailed {
		t.Errorf("Kỳ vọng status FAILED, nhận: %s", op.Status)
	}
}

// 17.3 Test: DeductedItems bị hỏng hoặc rỗng -> Rollback transaction, không được fallback sang payload để tránh tạo hàng ảo
func TestMixedOrderStockWorker_Compensate_CorruptDeductedItems_RefusesRefund(t *testing.T) {
	db, rClient, mr := setupTestWorkerDB(t, "test_corrupt_deducted_items")
	defer mr.Close()

	prod := domain.Product{Name: "Item Corrupt", Slug: "item-corrupt", Price: 100000, Stock: 10}
	db.Create(&prod)

	mockProducer := &mockKafkaProducerForWorker{}
	opRepo := repository.NewMixedOrderStockOperationRepository(db)

	w := &MixedOrderStockWorker{
		db:          db,
		opRepo:      opRepo,
		redisClient: rClient,
		producer:    mockProducer,
	}

	orderID := uint(105)

	// Tạo trước operation ở trạng thái DEDUCTED nhưng DeductedItems bị lỗi/rỗng
	corruptOp := &domain.MixedOrderStockOperation{
		OrderID:       orderID,
		OrderCode:     "ORD-105",
		Status:        domain.MixedStockOpDeducted,
		DeductedItems: "invalid-json-content",
	}
	db.Create(corruptOp)

	// Gửi compensate request chứa 5 items trong payload
	compPayload := pkgKafka.MixedOrderStockCompensatePayload{
		EventID:   "comp-evt-105",
		EventType: pkgKafka.EventMixedStockCompensate,
		OrderID:   orderID,
		OrderCode: "ORD-105",
		Items: []pkgKafka.OrderItemPayload{
			{ProductID: prod.ID, Quantity: 5},
		},
		Timestamp: time.Now(),
	}
	compBytes, _ := json.Marshal(compPayload)

	err := w.processCompensateMessage(context.Background(), kafka.Message{Value: compBytes})
	if err == nil {
		t.Fatalf("Kỳ vọng lỗi khi DeductedItems bị hỏng, nhưng lại trả về nil")
	}

	// Kiểm tra tồn kho KHÔNG được cộng khống (vẫn là 10)
	var currentProd domain.Product
	db.First(&currentProd, prod.ID)
	if currentProd.Stock != 10 {
		t.Errorf("LỖI GHOST STOCK: Tồn kho đã bị cộng khống thành %d (kỳ vọng vẫn là 10)", currentProd.Stock)
	}

	// Operation status vẫn giữ nguyên không chuyển sang COMPENSATED
	op, _ := opRepo.GetByOrderID(nil, orderID)
	if op.Status != domain.MixedStockOpDeducted {
		t.Errorf("Kỳ vọng status vẫn là DEDUCTED do transaction đã rollback, nhận: %s", op.Status)
	}
}

// 17.4 Test: Bồi hoàn thành công hoặc nhận trước -> Bắn TopicMixedOrderStockCompensateResult qua Kafka
func TestMixedOrderStockWorker_Compensate_PublishResult(t *testing.T) {
	db, rClient, mr := setupTestWorkerDB(t, "test_compensate_publish_result")
	defer mr.Close()

	prod := domain.Product{Name: "Item Test", Slug: "item-test", Price: 100000, Stock: 10}
	db.Create(&prod)

	mockProducer := &mockKafkaProducerForWorker{}
	opRepo := repository.NewMixedOrderStockOperationRepository(db)

	w := &MixedOrderStockWorker{
		db:          db,
		opRepo:      opRepo,
		redisClient: rClient,
		producer:    mockProducer,
	}

	orderID := uint(106)

	// Chuẩn bị operation DEDUCTED hợp lệ
	items := []pkgKafka.OrderItemPayload{
		{ProductID: prod.ID, Quantity: 2},
	}
	itemsJSON, _ := json.Marshal(items)
	validOp := &domain.MixedOrderStockOperation{
		OrderID:       orderID,
		OrderCode:     "ORD-106",
		Status:        domain.MixedStockOpDeducted,
		DeductedItems: string(itemsJSON),
	}
	db.Create(validOp)

	compPayload := pkgKafka.MixedOrderStockCompensatePayload{
		EventID:   "comp-evt-106",
		EventType: pkgKafka.EventMixedStockCompensate,
		OrderID:   orderID,
		OrderCode: "ORD-106",
		Items:     items,
		Timestamp: time.Now(),
	}
	compBytes, _ := json.Marshal(compPayload)

	err := w.processCompensateMessage(context.Background(), kafka.Message{Value: compBytes})
	if err != nil {
		t.Fatalf("processCompensateMessage lỗi: %v", err)
	}

	// Kiểm tra tồn kho được hoàn từ 10 -> 12
	var currentProd domain.Product
	db.First(&currentProd, prod.ID)
	if currentProd.Stock != 12 {
		t.Errorf("Kỳ vọng tồn kho là 12, nhận: %d", currentProd.Stock)
	}

	// 17.4: Kiểm tra kết quả bồi hoàn được phát ra Kafka
	if len(mockProducer.compensateResults) != 1 {
		t.Fatalf("Kỳ vọng 1 compensate result được phát, nhận %d", len(mockProducer.compensateResults))
	}
	res := mockProducer.compensateResults[0]
	if !res.Success || res.Status != "COMPENSATED" || res.OrderID != orderID {
		t.Errorf("Kết quả bồi hoàn không đúng: %+v", res)
	}
}

// 18.5 Test: Transactional Outbox ghi nhận sự kiện nguyên tử và OutboxPublisherWorker phát Kafka thành công
func TestProductOutbox_TransactionalPersistence_And_PublisherWorker(t *testing.T) {
	db, rClient, mr := setupTestWorkerDB(t, "test_product_outbox_worker")
	defer mr.Close()

	prodRepo := repository.NewProductRepository(db)
	outboxRepo := repository.NewProductOutboxRepository(db)
	mockProducer := &mockKafkaProducerForWorker{}

	// Tạo sản phẩm
	prod := domain.Product{
		Name:  "Smart Refrigerator",
		Slug:  "smart-refrigerator",
		Price: 15000000,
		Stock: 5,
	}
	db.Create(&prod)

	w := NewMixedOrderStockWorker([]string{"mock:9092"}, db, prodRepo, nil, rClient, mockProducer)

	orderID := uint(888)
	orderCode := "ORD-888"
	eventID := "evt-outbox-888"

	reqPayload := pkgKafka.MixedOrderStockRequestPayload{
		EventID:   eventID,
		OrderID:   orderID,
		OrderCode: orderCode,
		RegularItems: []pkgKafka.OrderItemPayload{
			{ProductID: prod.ID, ProductName: prod.Name, Quantity: 2},
		},
		TraceID:   "trace-outbox-888",
		Timestamp: time.Now(),
	}
	reqBytes, _ := json.Marshal(reqPayload)

	// Xử lý trừ kho
	err := w.processMessage(context.Background(), kafka.Message{Value: reqBytes})
	if err != nil {
		t.Fatalf("processStockRequest lỗi: %v", err)
	}

	// 1. Kiểm tra tồn kho đã bị trừ từ 5 -> 3
	var updatedProd domain.Product
	db.First(&updatedProd, prod.ID)
	if updatedProd.Stock != 3 {
		t.Fatalf("Kỳ vọng tồn kho là 3, nhận %d", updatedProd.Stock)
	}

	// 2. Kiểm tra bản ghi ProductOutboxEvent đã được ghi nhận trong DB
	var outboxEvent domain.ProductOutboxEvent
	outboxID := fmt.Sprintf("ob-deduct-%d-%s", orderID, eventID)
	err = db.Where("id = ?", outboxID).First(&outboxEvent).Error
	if err != nil {
		t.Fatalf("Kỳ vọng ProductOutboxEvent tồn tại trong DB nhưng lỗi: %v", err)
	}
	if outboxEvent.Topic != pkgKafka.TopicMixedOrderStockResult {
		t.Errorf("Kỳ vọng topic %s, nhận %s", pkgKafka.TopicMixedOrderStockResult, outboxEvent.Topic)
	}

	// 3. Test ProductOutboxPublisherWorker phát sự kiện từ DB lên Kafka
	// Đặt lại status về PENDING để worker claim và publish
	db.Model(&domain.ProductOutboxEvent{}).Where("id = ?", outboxID).Update("status", domain.ProductOutboxStatusPending)

	pubWorker := NewProductOutboxPublisherWorker(outboxRepo, mockProducer, "test-worker-1")
	pubWorker.processBatch(context.Background())

	// Xác minh worker đã gọi PublishRaw
	mockProducer.mu.Lock()
	rawCount := len(mockProducer.rawPublished)
	mockProducer.mu.Unlock()
	if rawCount == 0 {
		t.Errorf("Kỳ vọng ProductOutboxPublisherWorker đã gọi PublishRaw")
	}

	// Xác minh event chuyển sang PUBLISHED
	var publishedEvent domain.ProductOutboxEvent
	db.First(&publishedEvent, "id = ?", outboxID)
	if publishedEvent.Status != domain.ProductOutboxStatusPublished {
		t.Errorf("Kỳ vọng trạng thái PUBLISHED, nhận %s", publishedEvent.Status)
	}
}


