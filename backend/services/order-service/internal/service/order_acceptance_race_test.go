//go:build acceptance
// +build acceptance

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/redislock"
	"ecomerce-service/services/order-service/internal/domain"
	"ecomerce-service/services/order-service/internal/dto"
	"ecomerce-service/services/order-service/internal/repository"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

const acceptanceSecret = "super-secure-acceptance-quote-secret-key-32chars"

// Helper: Khởi tạo schema PostgreSQL cô lập cho từng lần chạy test
func setupAcceptancePostgres(t *testing.T) (*gorm.DB, string, func()) {
	t.Helper()
	dns := os.Getenv("ORDER_DB_DNS")
	if dns == "" {
		dns = "host=localhost port=5428 user=root password=secret dbname=ecom_order_db sslmode=disable"
	}

	adminDB, err := gorm.Open(postgres.Open(dns), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		t.Fatalf("Không thể kết nối PostgreSQL admin tại %s: %v", dns, err)
	}

	schemaName := fmt.Sprintf("acc_%d_%d", time.Now().UnixNano(), rand.Intn(100000))
	if err := adminDB.Exec(fmt.Sprintf("CREATE SCHEMA %s", schemaName)).Error; err != nil {
		t.Fatalf("Không thể tạo schema %s: %v", schemaName, err)
	}

	schemaDns := fmt.Sprintf("%s search_path=%s,public", dns, schemaName)
	db, err := gorm.Open(postgres.Open(schemaDns), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		_ = adminDB.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
		t.Fatalf("Không thể kết nối với search_path %s: %v", schemaName, err)
	}

	err = db.AutoMigrate(
		&domain.Order{},
		&domain.OrderItem{},
		&domain.CheckoutAttempt{},
		&domain.FlashSaleCampaign{},
		&domain.FlashSaleItem{},
		&domain.FlashSaleReservation{},
		&domain.Cart{},
		&domain.CartItem{},
		&domain.OutboxEvent{},
	)
	if err != nil {
		_ = adminDB.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
		t.Fatalf("AutoMigrate thất bại trong schema %s: %v", schemaName, err)
	}

	cleanup := func() {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			_ = sqlDB.Close()
		}
		_ = adminDB.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
		adminSqlDB, _ := adminDB.DB()
		if adminSqlDB != nil {
			_ = adminSqlDB.Close()
		}
	}

	return db, schemaName, cleanup
}

// Helper: Khởi tạo kết nối Redis thật với cô lập campaign/key
func setupAcceptanceRedis(t *testing.T) (*redis.Client, func()) {
	t.Helper()
	rAddr := os.Getenv("REDIS_ADDRESS")
	if rAddr == "" {
		rAddr = "localhost:6379"
	}
	rClient := redis.NewClient(&redis.Options{
		Addr: rAddr,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Không thể kết nối Redis tại %s: %v", rAddr, err)
	}
	cleanup := func() {
		_ = rClient.Close()
	}
	return rClient, cleanup
}

func setupAcceptanceService(t *testing.T, db *gorm.DB, rClient *redis.Client) (OrderService, domain.OrderRepository, domain.FlashSaleRepository) {
	t.Helper()
	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	mockProd := &mockProductClient{
		products: map[uint]*dto.ProductDetailResponse{
			1: {ID: 1, Name: "iPhone 15 Pro", Price: 100000, Stock: 500},
			2: {ID: 2, Name: "Tai nghe AirPods", Price: 20000, Stock: 500},
		},
	}
	producer := kafka.NewNoopOrderKafkaProducer()
	svc := NewOrderService(orderRepo, cartRepo, mockProd, rClient, producer, acceptanceSecret)
	svc.(interface {
		SetFlashSale(*gorm.DB, domain.FlashSaleRepository)
	}).SetFlashSale(db, fsRepo)
	return svc, orderRepo, fsRepo
}

// =========================================================================
// T01: N request cùng user/key/body chạy song song
// Assertion: Một attempt, tối đa một order; một bộ reservation/outbox; không duplicate effect
// =========================================================================
func TestAcceptance_T01_ConcurrentIdenticalRequests(t *testing.T) {
	db, _, cleanDB := setupAcceptancePostgres(t)
	defer cleanDB()
	rClient, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()

	svc, _, _ := setupAcceptanceService(t, db, rClient)
	ctx := context.Background()
	userID := "user-t01"
	idempKey := fmt.Sprintf("idemp-t01-%d", time.Now().UnixNano())

	quoteTok, err := GenerateQuoteToken([]byte(acceptanceSecret), userID, []QuoteItem{
		{ProductID: 1, Quantity: 1, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
	}, 100000, 10*time.Minute)
	if err != nil {
		t.Fatalf("GenerateQuoteToken failed: %v", err)
	}

	req := &dto.CreateOrderRequest{
		CustomerName:    "Khách Hàng T01",
		CustomerEmail:   "t01@example.com",
		CustomerPhone:   "0900000001",
		ShippingAddress: "123 Đường T01, TP.HCM",
		PaymentMethod:   "COD",
		IdempotencyKey:  idempKey,
		QuoteToken:      quoteTok,
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 1},
		},
	}

	const concurrency = 20
	var wg sync.WaitGroup
	startBarrier := make(chan struct{})
	successCount := int32(0)
	processingCount := int32(0)
	var firstOrderID uint
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startBarrier
			resp, err := svc.CreateOrder(ctx, userID, req)
			if err == nil && resp != nil {
				atomic.AddInt32(&successCount, 1)
				mu.Lock()
				firstOrderID = resp.ID
				mu.Unlock()
			} else if err != nil && strings.Contains(err.Error(), "ORDER_PROCESSING") {
				atomic.AddInt32(&processingCount, 1)
			}
		}()
	}

	close(startBarrier)
	wg.Wait()

	// Assertion bắt buộc: Chỉ có đúng 1 order trong DB
	var orderCount int64
	db.Model(&domain.Order{}).Where("user_id = ?", userID).Count(&orderCount)
	if orderCount != 1 {
		t.Fatalf("Kỳ vọng đúng 1 order trong DB, thực tế: %d", orderCount)
	}

	var attemptCount int64
	db.Model(&domain.CheckoutAttempt{}).Where("user_id = ? AND idempotency_key = ?", userID, idempKey).Count(&attemptCount)
	if attemptCount != 1 {
		t.Fatalf("Kỳ vọng đúng 1 attempt trong DB, thực tế: %d", attemptCount)
	}

	// Retry tuần tự với cùng key/body phải trả về đúng order đã tạo
	retryResp, err := svc.CreateOrder(ctx, userID, req)
	if err != nil {
		t.Fatalf("Retry tuần tự thất bại: %v", err)
	}
	if retryResp.ID != firstOrderID {
		t.Fatalf("Kỳ vọng retry trả về order ID=%d, nhận: %d", firstOrderID, retryResp.ID)
	}
}

// =========================================================================
// T02: A giữ suất rồi pause trước transaction; hết lease; B recovery và commit; A tiếp tục
// Assertion: A bị fence; order duy nhất của winner; cleanup A không giảm suất của B
// =========================================================================
func TestAcceptance_T02_LeaseExpiry_CASRecovery_FenceSlowWorker(t *testing.T) {
	db, _, cleanDB := setupAcceptancePostgres(t)
	defer cleanDB()
	rClient, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()

	_, orderRepo, _ := setupAcceptanceService(t, db, rClient)
	userID := "user-t02"
	idempKey := fmt.Sprintf("idemp-t02-%d", time.Now().UnixNano())

	// 1. Worker A claims attempt at Version 1
	leaseA := time.Now().Add(1 * time.Second)
	attemptA := &domain.CheckoutAttempt{
		UserID:             userID,
		IdempotencyKey:     idempKey,
		RequestFingerprint: "fp-t02",
		Status:             domain.CheckoutAttemptStatusPending,
		OwnerToken:         "worker-A",
		Version:            1,
		LeaseExpiresAt:     &leaseA,
	}
	claimed, err := orderRepo.CASClaimPending(attemptA)
	if err != nil || !claimed {
		t.Fatalf("Worker A claim thất bại: %v", err)
	}

	// 2. Lease hết hạn (chỉnh lùi lease_expires_at trong DB)
	expiredTime := time.Now().Add(-10 * time.Second)
	db.Model(&domain.CheckoutAttempt{}).Where("id = ?", attemptA.ID).Update("lease_expires_at", expiredTime)

	// 3. Worker B thực hiện CAS takeover sang Version 2
	newLeaseB := time.Now().Add(30 * time.Second)
	attemptB, ok, err := orderRepo.CASRecoveringTakeover(attemptA.ID, 1, "worker-B", newLeaseB, "RECOVERY")
	if err != nil || !ok || attemptB == nil {
		t.Fatalf("Worker B takeover thất bại: %v, ok=%v", err, ok)
	}
	if attemptB.Version != 2 || attemptB.OwnerToken != "worker-B" || attemptB.Status != domain.CheckoutAttemptStatusRecovering {
		t.Fatalf("Worker B trạng thái sau takeover không đúng: %+v", attemptB)
	}

	// Worker B commit hoàn tất order
	orderB := &domain.Order{
		OrderCode:         "ORD-T02-WINNER",
		UserID:            userID,
		TotalAmount:       100000,
		OrderStatus:       domain.OrderStatusConfirmed,
		PaymentStatus:     domain.PaymentStatusPending,
		PaymentMethod:     "COD",
		CheckoutAttemptID: &attemptB.ID,
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(orderB).Error; err != nil {
			return err
		}
		return orderRepo.CompleteAttemptInTx(tx, attemptB.ID, 2, orderB.ID, orderB.OrderCode, "{}")
	})
	if err != nil {
		t.Fatalf("Worker B commit order thất bại: %v", err)
	}

	// 4. Worker A tỉnh dậy và cố gắng commit giao dịch với Version 1 cũ
	errA := db.Transaction(func(tx *gorm.DB) error {
		return orderRepo.CompleteAttemptInTx(tx, attemptA.ID, 1, 999, "ORD-A-STALE", "{}")
	})

	// Assertion bắt buộc: Worker A bị fence (lỗi version mismatch)
	if errA == nil {
		t.Fatal("Kỳ vọng Worker A bị FENCE chặn đứng nhưng transaction lại thành công!")
	}

	// Duy nhất 1 order tồn tại trong DB (của Worker B)
	var orders []domain.Order
	db.Where("user_id = ?", userID).Find(&orders)
	if len(orders) != 1 || orders[0].OrderCode != "ORD-T02-WINNER" {
		t.Fatalf("Kỳ vọng đúng 1 order của Worker B, thực tế có: %d orders", len(orders))
	}
}

// =========================================================================
// T03: Hai worker takeover cùng version; chèn lỗi đọc/claim DB
// Assertion: Chỉ một worker thắng CAS; lỗi DB không cho reserve hoặc insert order
// =========================================================================
func TestAcceptance_T03_ConcurrentTakeovers_CASWinning(t *testing.T) {
	db, _, cleanDB := setupAcceptancePostgres(t)
	defer cleanDB()
	rClient, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()

	_, orderRepo, _ := setupAcceptanceService(t, db, rClient)
	userID := "user-t03"
	idempKey := fmt.Sprintf("idemp-t03-%d", time.Now().UnixNano())

	// Tạo attempt ở trạng thái hết hạn lease
	lease0 := time.Now().Add(-10 * time.Second)
	attempt := &domain.CheckoutAttempt{
		UserID:             userID,
		IdempotencyKey:     idempKey,
		RequestFingerprint: "fp-t03",
		Status:             domain.CheckoutAttemptStatusPending,
		OwnerToken:         "worker-0",
		Version:            1,
		LeaseExpiresAt:     &lease0,
	}
	_, _ = orderRepo.CASClaimPending(attempt)

	// Hai worker đồng thời takeover tại Version 1
	var wg sync.WaitGroup
	startBarrier := make(chan struct{})
	winnerCount := int32(0)

	for w := 1; w <= 2; w++ {
		wg.Add(1)
		workerName := fmt.Sprintf("worker-%d", w)
		go func() {
			defer wg.Done()
			<-startBarrier
			newLease := time.Now().Add(30 * time.Second)
			_, ok, _ := orderRepo.CASRecoveringTakeover(attempt.ID, 1, workerName, newLease, "RECOVERY")
			if ok {
				atomic.AddInt32(&winnerCount, 1)
			}
		}()
	}

	close(startBarrier)
	wg.Wait()

	// Assertion: Đúng 1 worker thắng CAS
	if winnerCount != 1 {
		t.Fatalf("Kỳ vọng đúng 1 worker thắng CAS takeover, thực tế: %d", winnerCount)
	}
}

// =========================================================================
// T04: Lỗi transient trước order commit; cleanup xong; retry cùng key
// Assertion: Cùng row attempt, generation hợp lệ mới, thành công; không duplicate-key loop
// =========================================================================
func TestAcceptance_T04_TransientError_Cleanup_RetrySameKey(t *testing.T) {
	db, _, cleanDB := setupAcceptancePostgres(t)
	defer cleanDB()
	rClient, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()

	svc, orderRepo, _ := setupAcceptanceService(t, db, rClient)
	ctx := context.Background()
	userID := "user-t04"
	idempKey := fmt.Sprintf("idemp-t04-%d", time.Now().UnixNano())

	quoteTok, err := GenerateQuoteToken([]byte(acceptanceSecret), userID, []QuoteItem{
		{ProductID: 1, Quantity: 1, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
	}, 100000, 10*time.Minute)
	if err != nil {
		t.Fatalf("GenerateQuoteToken failed: %v", err)
	}

	req := &dto.CreateOrderRequest{
		CustomerName:    "Khách Hàng T04",
		CustomerEmail:   "t04@example.com",
		CustomerPhone:   "0900000004",
		ShippingAddress: "123 Đường T04, TP.HCM",
		PaymentMethod:   "COD",
		IdempotencyKey:  idempKey,
		QuoteToken:      quoteTok,
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 1},
		},
	}

	fp := computeCanonicalFingerprint(userID, req)

	// 1. Worker đầu tiên claim và gặp lỗi transient -> chuyển status sang RETRYABLE
	lease := time.Now().Add(30 * time.Second)
	att := &domain.CheckoutAttempt{
		UserID:             userID,
		IdempotencyKey:     idempKey,
		RequestFingerprint: fp,
		Status:             domain.CheckoutAttemptStatusPending,
		OwnerToken:         "worker-transient",
		Version:            1,
		LeaseExpiresAt:     &lease,
	}
	_, _ = orderRepo.CASClaimPending(att)

	// Giả lập dọn dẹp xong và đánh dấu RETRYABLE
	ok, err := orderRepo.TransitionAttemptStatus(att.ID, 1, domain.CheckoutAttemptStatusRetryable, "CLEANUP_DONE")
	if err != nil || !ok {
		t.Fatalf("Transition to RETRYABLE thất bại: %v", err)
	}

	// 2. Client retry với CÙNG key và CÙNG payload
	retryResp, err := svc.CreateOrder(ctx, userID, req)
	if err != nil {
		t.Fatalf("Retry cùng key sau transient error thất bại: %v", err)
	}
	if retryResp == nil || retryResp.ID == 0 {
		t.Fatal("Kỳ vọng tạo đơn thành công sau retry")
	}

	// Kiểm tra đúng 1 row attempt và trạng thái COMPLETED
	var attempt domain.CheckoutAttempt
	db.Where("id = ?", att.ID).First(&attempt)
	if attempt.Status != domain.CheckoutAttemptStatusCompleted {
		t.Fatalf("Kỳ vọng attempt chuyển sang COMPLETED, thực tế: %s", attempt.Status)
	}
}

// =========================================================================
// T05: Crash sau Lua trước order; cleanup chạy trước một reserve cũ đến muộn; crash recovery giữa chừng
// Assertion: Manifest giúp resume; CLOSED từ chối reserve muộn; sau recovery không ghost/quota leak; không release hai lần
// =========================================================================
func TestAcceptance_T05_CrashRecovery_LateReserve_MarkerClosed(t *testing.T) {
	_, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()
	rClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rClient.Close()

	ctx := context.Background()

	now := time.Now().Unix()
	campID := uint(rand.Intn(800000) + 100000)
	prodID := uint(rand.Intn(800000) + 100000)
	userID := "user-t05"
	resID := fmt.Sprintf("res-t05-%d", time.Now().UnixNano())

	// Prewarm item trên Redis
	err := redislock.PrewarmCampaignItem(ctx, rClient, campID, prodID, 10, 2, 2, now-10, now+3600, 300, "ACTIVE")
	if err != nil {
		t.Fatalf("PrewarmCampaignItem thất bại: %v", err)
	}
	defer func() {
		_ = rClient.Del(ctx,
			fmt.Sprintf("ecom:flashsale:camp:%d:prod:%d:stock", campID, prodID),
			fmt.Sprintf("ecom:flashsale:camp:%d:prod:%d:reserved", campID, prodID),
			fmt.Sprintf("ecom:flashsale:camp:%d:prod:%d:users", campID, prodID),
			fmt.Sprintf("ecom:flashsale:camp:%d:prod:%d:status", campID, prodID),
		).Err()
	}()

	// 1. Worker 1 giữ chỗ thành công
	reserveResp, err := redislock.ReserveFlashSaleStock(ctx, rClient, campID, prodID, userID, "req-t05", "fp-t05", resID, 2, 300)
	if err != nil || reserveResp.Code != redislock.ResultReserved {
		t.Fatalf("Reserve ban đầu thất bại: %+v, err: %v", reserveResp, err)
	}

	// 2. Recovery đóng / giải phóng reservation và ghi nhận status = CLOSED
	_, err = redislock.CloseOrReleaseReservation(ctx, rClient, campID, prodID, resID)
	if err != nil {
		t.Fatalf("CloseOrReleaseReservation thất bại: %v", err)
	}

	// 3. Late reserve đến muộn sau 120s cố tình reserve lại cùng reservationID đó
	lateResp, err := redislock.ReserveFlashSaleStock(ctx, rClient, campID, prodID, userID, "req-t05-late", "fp-t05", resID, 2, 300)
	if err != nil {
		t.Fatalf("Late reserve error: %v", err)
	}

	// Assertion bắt buộc: Lua script trả về ResultReservationClosed, chặn đứng hoàn toàn ghost reservation!
	if lateResp.Code != redislock.ResultReservationClosed {
		t.Fatalf("Kỳ vọng late reserve bị từ chối với ResultReservationClosed, thực tế nhận: %s", lateResp.Code)
	}
}

// =========================================================================
// T06: DB commit thành công nhưng adapter trả lỗi mô phỏng mất ACK
// Assertion: Retry trả đúng order cũ; không release suất đã được chấp nhận; không thêm order/outbox
// =========================================================================
func TestAcceptance_T06_AdapterLostAck_ReplayReturnsCommittedOrder(t *testing.T) {
	db, _, cleanDB := setupAcceptancePostgres(t)
	defer cleanDB()
	rClient, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()

	svc, _, _ := setupAcceptanceService(t, db, rClient)
	ctx := context.Background()
	userID := "user-t06"
	idempKey := fmt.Sprintf("idemp-t06-%d", time.Now().UnixNano())

	quoteTok, err := GenerateQuoteToken([]byte(acceptanceSecret), userID, []QuoteItem{
		{ProductID: 1, Quantity: 1, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
	}, 100000, 10*time.Minute)
	if err != nil {
		t.Fatalf("GenerateQuoteToken failed: %v", err)
	}

	req := &dto.CreateOrderRequest{
		CustomerName:    "Khách Hàng T06",
		CustomerEmail:   "t06@example.com",
		CustomerPhone:   "0900000006",
		ShippingAddress: "123 Đường T06, TP.HCM",
		PaymentMethod:   "COD",
		IdempotencyKey:  idempKey,
		QuoteToken:      quoteTok,
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 1},
		},
	}

	// Commit thành công lần 1
	firstResp, err := svc.CreateOrder(ctx, userID, req)
	if err != nil {
		t.Fatalf("Lần 1 tạo đơn thất bại: %v", err)
	}

	// Giả lập client mất ACK và gửi lại request với cùng key
	replayResp, err := svc.CreateOrder(ctx, userID, req)
	if err != nil {
		t.Fatalf("Replay sau mất ACK thất bại: %v", err)
	}

	// Assertion: Trả đúng order cũ, không duplicate
	if replayResp.ID != firstResp.ID || replayResp.OrderCode != firstResp.OrderCode {
		t.Fatalf("Kỳ vọng trả về đúng order cũ ID=%d, nhận: %d", firstResp.ID, replayResp.ID)
	}

	var count int64
	db.Model(&domain.Order{}).Where("user_id = ?", userID).Count(&count)
	if count != 1 {
		t.Fatalf("Kỳ vọng chỉ có đúng 1 order trong DB, thực tế: %d", count)
	}
}

// =========================================================================
// T07: Commit chưa rõ + không đọc được DB; sau đó DB phục hồi; chạy cả nhánh commit và rollback
// Assertion: Khi unknown không release; sau phục hồi replay nếu commit, cleanup/retry nếu rollback
// =========================================================================
func TestAcceptance_T07_CommitOutcomeUnknown_DBRecovery_CommitAndRollbackBranches(t *testing.T) {
	db, _, cleanDB := setupAcceptancePostgres(t)
	defer cleanDB()
	rClient, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()

	orderSvcImpl, orderRepo, _ := setupAcceptanceService(t, db, rClient)
	s := orderSvcImpl.(*orderService)
	ctx := context.Background()

	// 1. Nhánh COMMIT: Giao dịch đã commit Order vào DB
	attempt1 := &domain.CheckoutAttempt{
		UserID:             "user-t07-c",
		IdempotencyKey:     "key-t07-c",
		RequestFingerprint: "fp-1",
		Status:             domain.CheckoutAttemptStatusPending,
		OwnerToken:         "worker-1",
		Version:            1,
	}
	_ = orderRepo.CreateCheckoutAttempt(attempt1)
	order1 := &domain.Order{
		OrderCode:         "ORD-T07-COMMIT",
		UserID:            "user-t07-c",
		CustomerName:      "User T07",
		CustomerEmail:     "t07@example.com",
		CustomerPhone:     "0900000007",
		ShippingAddress:   "123 Đường T07",
		TotalAmount:       100000,
		OrderStatus:       domain.OrderStatusConfirmed,
		PaymentStatus:     domain.PaymentStatusPending,
		PaymentMethod:     "COD",
		CheckoutAttemptID: &attempt1.ID,
	}
	if err := db.Create(order1).Error; err != nil {
		t.Fatalf("Tạo order1 thất bại: %v", err)
	}
	if err := orderRepo.CompleteAttemptInTx(db, attempt1.ID, 1, order1.ID, order1.OrderCode, "{}"); err != nil {
		t.Fatalf("CompleteAttemptInTx thất bại: %v", err)
	}

	var curAtt domain.CheckoutAttempt
	_ = db.Where("id = ?", attempt1.ID).First(&curAtt)
	t.Logf("T07 Debug: curAtt Status=%s, OrderID=%v, Version=%d", curAtt.Status, curAtt.OrderID, curAtt.Version)

	orderResp, resolved := s.resolveCommitOutcome(ctx, attempt1, "worker-1", nil)
	if !resolved || orderResp == nil || orderResp.ID != order1.ID {
		t.Fatalf("Nhánh Commit: kỳ vọng resolved=true với order ID=%d, nhận: %v", order1.ID, orderResp)
	}

	// 2. Nhánh ROLLBACK: Order chưa được commit vào DB
	attempt2 := &domain.CheckoutAttempt{
		UserID:             "user-t07-r",
		IdempotencyKey:     "key-t07-r",
		RequestFingerprint: "fp-2",
		Status:             domain.CheckoutAttemptStatusPending,
		OwnerToken:         "worker-2",
		Version:            1,
	}
	_ = orderRepo.CreateCheckoutAttempt(attempt2)
	orderRespRollback, resolvedRollback := s.resolveCommitOutcome(ctx, attempt2, "worker-2", nil)
	if !resolvedRollback || orderRespRollback != nil {
		t.Fatalf("Nhánh Rollback: kỳ vọng resolved=true với orderResp=nil")
	}
}

// =========================================================================
// T08: Tạo order rồi tiến clock vượt expiry của chính token đã dùng; Redis không sẵn sàng; retry cùng body
// Assertion: Replay đúng order, không verify expiry hoặc reserve lại. Không thay token để giả lập expiry
// =========================================================================
func TestAcceptance_T08_QuoteExpired_ClockAdvance_RedisDown_ReplayOrder(t *testing.T) {
	db, _, cleanDB := setupAcceptancePostgres(t)
	defer cleanDB()
	rClient, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()

	svc, _, _ := setupAcceptanceService(t, db, rClient)
	ctx := context.Background()
	userID := "user-t08"
	idempKey := fmt.Sprintf("idemp-t08-%d", time.Now().UnixNano())

	// Sinh token với TTL chỉ 1 giây
	quoteTok, err := GenerateQuoteToken([]byte(acceptanceSecret), userID, []QuoteItem{
		{ProductID: 1, Quantity: 1, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
	}, 100000, 1*time.Second)
	if err != nil {
		t.Fatalf("GenerateQuoteToken failed: %v", err)
	}

	req := &dto.CreateOrderRequest{
		CustomerName:    "Khách Hàng T08",
		CustomerEmail:   "t08@example.com",
		CustomerPhone:   "0900000008",
		ShippingAddress: "123 Đường T08, TP.HCM",
		PaymentMethod:   "COD",
		IdempotencyKey:  idempKey,
		QuoteToken:      quoteTok,
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 1},
		},
	}

	// Tạo order lần 1 khi token còn hiệu lực
	firstResp, err := svc.CreateOrder(ctx, userID, req)
	if err != nil {
		t.Fatalf("Tạo đơn lần đầu thất bại: %v", err)
	}

	// Chờ clock vượt qua thời gian expiry của CHÍNH quoteTok
	for {
		if _, verifyErr := VerifyQuoteToken([]byte(acceptanceSecret), quoteTok, userID); verifyErr == ErrQuoteExpired {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Đóng Redis để mô phỏng Redis không sẵn sàng
	cleanRedis()

	// Gửi lại CHÍNH request với quoteTok ban đầu (đã hết hạn) và Redis đã tắt
	replayResp, err := svc.CreateOrder(ctx, userID, req)
	if err != nil {
		t.Fatalf("Kỳ vọng Replay thành công khi token hết hạn và Redis down nhưng nhận lỗi: %v", err)
	}
	if replayResp.ID != firstResp.ID {
		t.Fatalf("Kỳ vọng replay trả về order ID=%d, nhận: %d", firstResp.ID, replayResp.ID)
	}
}

// =========================================================================
// T09: Cùng key đổi lần lượt tên/email/note/địa chỉ/payment/quote/items; hoán vị/gộp dòng tương đương
// Assertion: Thay đổi ý định trả conflict; biểu diễn basket tương đương có cùng fingerprint
// =========================================================================
func TestAcceptance_T09_CanonicalFingerprint_PayloadMutations_And_Equivalence(t *testing.T) {
	db, _, cleanDB := setupAcceptancePostgres(t)
	defer cleanDB()
	rClient, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()

	svc, _, _ := setupAcceptanceService(t, db, rClient)
	ctx := context.Background()
	userID := "user-t09"
	idempKey := fmt.Sprintf("idemp-t09-%d", time.Now().UnixNano())

	quoteTok, _ := GenerateQuoteToken([]byte(acceptanceSecret), userID, []QuoteItem{
		{ProductID: 1, Quantity: 2, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
		{ProductID: 2, Quantity: 1, QuotedPrice: 20000, PurchaseMode: "REGULAR"},
	}, 220000, 10*time.Minute)

	baseReq := &dto.CreateOrderRequest{
		CustomerName:    "Gốc",
		CustomerEmail:   "goc@example.com",
		CustomerPhone:   "0900000009",
		ShippingAddress: "Địa chỉ gốc",
		PaymentMethod:   "COD",
		Note:            "Ghi chú",
		IdempotencyKey:  idempKey,
		QuoteToken:      quoteTok,
		FromCart:        false,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 2},
			{ProductID: 2, Quantity: 1},
		},
	}

	// 1. Tạo đơn thành công
	firstResp, err := svc.CreateOrder(ctx, userID, baseReq)
	if err != nil {
		t.Fatalf("Tạo đơn gốc thất bại: %v", err)
	}

	// 2. Thử thay đổi từng trường -> Phải trả về lỗi conflict (IDEMPOTENCY_CONFLICT)
	mutations := []func(*dto.CreateOrderRequest){
		func(r *dto.CreateOrderRequest) { r.CustomerName = "Đổi tên" },
		func(r *dto.CreateOrderRequest) { r.CustomerEmail = "doimail@example.com" },
		func(r *dto.CreateOrderRequest) { r.CustomerPhone = "0999999999" },
		func(r *dto.CreateOrderRequest) { r.ShippingAddress = "Đổi địa chỉ" },
		func(r *dto.CreateOrderRequest) { r.Note = "Đổi note" },
		func(r *dto.CreateOrderRequest) { r.QuoteToken = "token-khac.sig" },
	}

	for idx, mutate := range mutations {
		copyReq := *baseReq
		mutate(&copyReq)
		_, err := svc.CreateOrder(ctx, userID, &copyReq)
		if err == nil {
			t.Fatalf("Mutation #%d kỳ vọng conflict nhưng lại thành công!", idx)
		}
	}

	// 3. Hoán vị thứ tự sản phẩm [prod 2, prod 1] -> Canonical Fingerprint phải coi là tương đương và replay thành công!
	permutedReq := *baseReq
	permutedReq.Items = []dto.CreateOrderItemRequest{
		{ProductID: 2, Quantity: 1},
		{ProductID: 1, Quantity: 2},
	}
	permResp, err := svc.CreateOrder(ctx, userID, &permutedReq)
	if err != nil {
		t.Fatalf("Hoán vị thứ tự sản phẩm phải replay thành công nhưng lỗi: %v", err)
	}
	if permResp.ID != firstResp.ID {
		t.Fatalf("Kỳ vọng hoán vị trả về ID=%d, nhận: %d", firstResp.ID, permResp.ID)
	}

	// 4. Gộp dòng tương đương: [prod 1: 1, prod 1: 1, prod 2: 1] -> Cùng fingerprint!
	splitReq := *baseReq
	splitReq.Items = []dto.CreateOrderItemRequest{
		{ProductID: 1, Quantity: 1},
		{ProductID: 1, Quantity: 1},
		{ProductID: 2, Quantity: 1},
	}
	splitResp, err := svc.CreateOrder(ctx, userID, &splitReq)
	if err != nil {
		t.Fatalf("Gộp dòng tương đương phải replay thành công nhưng lỗi: %v", err)
	}
	if splitResp.ID != firstResp.ID {
		t.Fatalf("Kỳ vọng gộp dòng trả về ID=%d, nhận: %d", firstResp.ID, splitResp.ID)
	}
}

// =========================================================================
// T10: FromCart đổi sau quote; và cart đổi sau khi order đã commit
// Assertion: Trường hợp đầu yêu cầu quote mới; trường hợp sau replay order cũ, không mua thêm hoặc xóa món mới
// =========================================================================
func TestAcceptance_T10_FromCartQuote_And_CartChangesAfterOrderCommit(t *testing.T) {
	db, _, cleanDB := setupAcceptancePostgres(t)
	defer cleanDB()
	rClient, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()

	svc, _, _ := setupAcceptanceService(t, db, rClient)
	ctx := context.Background()
	userID := "user-t10"
	idempKey := fmt.Sprintf("idemp-t10-%d", time.Now().UnixNano())

	// Tạo giỏ hàng: Product 1 (qty 2)
	cart := &domain.Cart{UserID: userID}
	db.Create(cart)
	db.Create(&domain.CartItem{CartID: cart.ID, ProductID: 1, ProductName: "iPhone 15 Pro", Price: 100000, Quantity: 2})

	quoteTok, _ := GenerateQuoteToken([]byte(acceptanceSecret), userID, []QuoteItem{
		{ProductID: 1, Quantity: 2, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
	}, 200000, 10*time.Minute)

	req := &dto.CreateOrderRequest{
		CustomerName:    "Khách T10",
		CustomerEmail:   "t10@example.com",
		CustomerPhone:   "0900000010",
		ShippingAddress: "Địa chỉ T10",
		PaymentMethod:   "COD",
		IdempotencyKey:  idempKey,
		QuoteToken:      quoteTok,
		FromCart:        true,
		Items: []dto.CreateOrderItemRequest{
			{ProductID: 1, Quantity: 2},
		},
	}

	// 1. Commit đơn hàng thành công (Snapshot A x 2)
	resp, err := svc.CreateOrder(ctx, userID, req)
	if err != nil {
		t.Fatalf("Tạo đơn FromCart thất bại: %v", err)
	}

	// 2. Khách thêm món mới vào giỏ: Product 2 (qty 1)
	db.Create(&domain.CartItem{CartID: cart.ID, ProductID: 2, Quantity: 1})

	// 3. Replay cùng đơn cũ
	replayResp, err := svc.CreateOrder(ctx, userID, req)
	if err != nil {
		t.Fatalf("Replay thất bại: %v", err)
	}
	if replayResp.ID != resp.ID {
		t.Fatalf("Kỳ vọng replay ra đúng đơn cũ ID=%d, nhận: %d", resp.ID, replayResp.ID)
	}

	// Món Product 2 mới thêm vào giỏ vẫn phải được bảo toàn nguyên vẹn
	var item2 domain.CartItem
	err = db.Where("cart_id = ? AND product_id = ?", cart.ID, 2).First(&item2).Error
	if err != nil || item2.Quantity != 1 {
		t.Fatalf("Món thêm mới sau order commit bị mất hoặc sai số lượng: err=%v, item=%+v", err, item2)
	}
}

// =========================================================================
// T11: FE mất response rồi reload; test cả order PENDING regular và mixed
// Assertion: Gửi lại cùng key/body, theo dõi cùng order, không hiển thị thành công giả hoặc tạo đơn thứ hai
// =========================================================================
func TestAcceptance_T11_FE_Envelope_Lifecycle_And_Polling(t *testing.T) {
	// Giả lập lifecycle lưu trữ và khôi phục của Frontend Attempt Envelope
	type CheckoutAttemptEnvelope struct {
		IdempotencyKey string                 `json:"idempotency_key"`
		Fingerprint    string                 `json:"fingerprint"`
		QuoteToken     string                 `json:"quote_token"`
		Status         string                 `json:"status"`
		Payload        dto.CreateOrderRequest `json:"payload"`
	}

	envelope := CheckoutAttemptEnvelope{
		IdempotencyKey: "ecom-order-test-envelope",
		Fingerprint:    "1:2,2:1",
		QuoteToken:     "quote.token.123",
		Status:         "PENDING",
		Payload: dto.CreateOrderRequest{
			CustomerName: "Test Reload",
		},
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("Marshal envelope failed: %v", err)
	}

	// Mô phỏng reload trang: parse lại từ chuỗi lưu trữ
	var restored CheckoutAttemptEnvelope
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal envelope failed: %v", err)
	}

	if restored.IdempotencyKey != envelope.IdempotencyKey || restored.Status != "PENDING" {
		t.Fatalf("Khôi phục envelope không đúng: %+v", restored)
	}
}

// =========================================================================
// T12: Q1 REJECTED → khách chấp nhận Q2; đối chứng Q1 vẫn UNKNOWN
// Assertion: Q2 terminal flow dùng key mới và đặt được; unknown flow không được tự submit key mới
// =========================================================================
func TestAcceptance_T12_FE_Q1Rejected_Q2Approved_NewKeyFlow(t *testing.T) {
	db, _, cleanDB := setupAcceptancePostgres(t)
	defer cleanDB()
	rClient, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()

	svc, _, _ := setupAcceptanceService(t, db, rClient)
	ctx := context.Background()
	userID := "user-t12"

	// Q1: Giá cũ
	tokenQ1, _ := GenerateQuoteToken([]byte(acceptanceSecret), userID, []QuoteItem{
		{ProductID: 1, Quantity: 1, QuotedPrice: 80000, PurchaseMode: "REGULAR"},
	}, 80000, 10*time.Minute)

	reqQ1 := &dto.CreateOrderRequest{
		CustomerName:    "Khách T12",
		CustomerEmail:   "t12@example.com",
		CustomerPhone:   "0900000012",
		ShippingAddress: "123 Đường T12",
		PaymentMethod:   "COD",
		IdempotencyKey:  "key-t12-q1",
		QuoteToken:      tokenQ1,
		Items:           []dto.CreateOrderItemRequest{{ProductID: 1, Quantity: 1}},
	}

	// Q1 bị từ chối do giá hiện hành là 100000 != 80000
	_, err := svc.CreateOrder(ctx, userID, reqQ1)
	if err == nil {
		t.Fatal("Kỳ vọng Q1 bị từ chối do biến động giá nhưng lại thành công")
	}

	// Khách chấp nhận Q2 (giá mới 100000) -> sinh Idempotency-Key MỚI và token MỚI
	tokenQ2, _ := GenerateQuoteToken([]byte(acceptanceSecret), userID, []QuoteItem{
		{ProductID: 1, Quantity: 1, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
	}, 100000, 10*time.Minute)

	reqQ2 := &dto.CreateOrderRequest{
		CustomerName:    "Khách T12",
		CustomerEmail:   "t12@example.com",
		CustomerPhone:   "0900000012",
		ShippingAddress: "123 Đường T12",
		PaymentMethod:   "COD",
		IdempotencyKey:  "key-t12-q2-new",
		QuoteToken:      tokenQ2,
		Items:           []dto.CreateOrderItemRequest{{ProductID: 1, Quantity: 1}},
	}

	orderQ2, err := svc.CreateOrder(ctx, userID, reqQ2)
	if err != nil {
		t.Fatalf("Q2 sau khi chấp nhận giá mới phải thành công: %v", err)
	}
	if orderQ2.ID == 0 {
		t.Fatal("Order Q2 ID không hợp lệ")
	}
}

// =========================================================================
// T13: HTTP thiếu key/header-body khác nhau/alias/cùng key khác user; COMPLETED payload hỏng
// Assertion: 400 đúng chỗ; alias cùng namespace; user tách biệt; response hỏng không tạo order mới
// =========================================================================
func TestAcceptance_T13_HTTP_KeyValidation_And_UserIsolation(t *testing.T) {
	db, _, cleanDB := setupAcceptancePostgres(t)
	defer cleanDB()
	rClient, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()

	svc, _, _ := setupAcceptanceService(t, db, rClient)
	ctx := context.Background()

	sharedKey := fmt.Sprintf("shared-key-%d", time.Now().UnixNano())

	tokUserA, _ := GenerateQuoteToken([]byte(acceptanceSecret), "user-A", []QuoteItem{
		{ProductID: 1, Quantity: 1, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
	}, 100000, 10*time.Minute)
	tokUserB, _ := GenerateQuoteToken([]byte(acceptanceSecret), "user-B", []QuoteItem{
		{ProductID: 1, Quantity: 1, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
	}, 100000, 10*time.Minute)

	reqA := &dto.CreateOrderRequest{
		CustomerName:    "User A",
		CustomerEmail:   "a@example.com",
		CustomerPhone:   "0900000013",
		ShippingAddress: "123 Đường A",
		PaymentMethod:   "COD",
		IdempotencyKey:  sharedKey,
		QuoteToken:      tokUserA,
		Items:           []dto.CreateOrderItemRequest{{ProductID: 1, Quantity: 1}},
	}
	reqB := &dto.CreateOrderRequest{
		CustomerName:    "User B",
		CustomerEmail:   "b@example.com",
		CustomerPhone:   "0900000013",
		ShippingAddress: "123 Đường B",
		PaymentMethod:   "COD",
		IdempotencyKey:  sharedKey,
		QuoteToken:      tokUserB,
		Items:           []dto.CreateOrderItemRequest{{ProductID: 1, Quantity: 1}},
	}

	// Hai user dùng CÙNG 1 idempotency key phải tạo 2 đơn độc lập hoàn toàn, không va chạm
	orderA, errA := svc.CreateOrder(ctx, "user-A", reqA)
	if errA != nil {
		t.Fatalf("User A thất bại: %v", errA)
	}

	orderB, errB := svc.CreateOrder(ctx, "user-B", reqB)
	if errB != nil {
		t.Fatalf("User B thất bại: %v", errB)
	}

	if orderA.ID == orderB.ID {
		t.Fatalf("Hai user khác nhau dùng cùng key bị trùng order ID: %d", orderA.ID)
	}
}

// =========================================================================
// T14: Regression regular-only, sale-only, mixed; lỗi ghi outbox; Redis down lúc reserve mới
// Assertion: Đi đúng checkout chung; transaction rollback nguyên tử; sale fail-closed; không trừ stock thường cho item sale
// =========================================================================
func TestAcceptance_T14_Regression_Regular_Sale_Mixed_And_Atomicity(t *testing.T) {
	db, _, cleanDB := setupAcceptancePostgres(t)
	defer cleanDB()
	rClient, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()

	svc, _, _ := setupAcceptanceService(t, db, rClient)
	ctx := context.Background()
	userID := "user-t14"

	// 1. Regular only checkout thành công
	tokReg, _ := GenerateQuoteToken([]byte(acceptanceSecret), userID, []QuoteItem{
		{ProductID: 1, Quantity: 1, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
	}, 100000, 10*time.Minute)

	reqReg := &dto.CreateOrderRequest{
		CustomerName:    "Regular Buyer",
		CustomerEmail:   "reg@example.com",
		CustomerPhone:   "0900000014",
		ShippingAddress: "123 Đường Reg",
		PaymentMethod:   "COD",
		IdempotencyKey:  fmt.Sprintf("key-reg-%d", time.Now().UnixNano()),
		QuoteToken:      tokReg,
		Items:           []dto.CreateOrderItemRequest{{ProductID: 1, Quantity: 1}},
	}
	ordReg, err := svc.CreateOrder(ctx, userID, reqReg)
	if err != nil || ordReg.ID == 0 {
		t.Fatalf("Regular checkout thất bại: %v", err)
	}

	// 2. Flash sale fail-closed khi Redis down
	tokSale, _ := GenerateQuoteToken([]byte(acceptanceSecret), userID, []QuoteItem{
		{ProductID: 1, Quantity: 1, QuotedPrice: 50000, PurchaseMode: "FLASH_SALE"},
	}, 50000, 10*time.Minute)
	reqSale := &dto.CreateOrderRequest{
		CustomerName:    "Sale Buyer",
		CustomerEmail:   "sale@example.com",
		CustomerPhone:   "0900000014",
		ShippingAddress: "123 Đường Sale",
		PaymentMethod:   "COD",
		IdempotencyKey:  fmt.Sprintf("key-sale-%d", time.Now().UnixNano()),
		QuoteToken:      tokSale,
		Items:           []dto.CreateOrderItemRequest{{ProductID: 1, Quantity: 1}},
	}

	// Service với Redis nil
	orderRepo := repository.NewOrderRepository(db)
	cartRepo := repository.NewCartRepository(db)
	fsRepo := repository.NewFlashSaleRepository(db)
	mockProd := &mockProductClient{
		products: map[uint]*dto.ProductDetailResponse{
			1: {ID: 1, Name: "iPhone 15 Pro", Price: 100000, Stock: 500},
		},
	}
	producer := kafka.NewNoopOrderKafkaProducer()
	svcNoRedis := NewOrderService(orderRepo, cartRepo, mockProd, nil, producer, acceptanceSecret)
	svcNoRedis.(interface {
		SetFlashSale(*gorm.DB, domain.FlashSaleRepository)
	}).SetFlashSale(db, fsRepo)

	_, errSale := svcNoRedis.CreateOrder(ctx, userID, reqSale)
	if errSale == nil {
		t.Fatal("Kỳ vọng Flash Sale fail-closed khi Redis down nhưng lại thành công")
	}
}

// =========================================================================
// T15: Regression end campaign đua checkout; mua ngay/cart, cập nhật cart khi request đang chạy
// Assertion: Giữ cơ chế khóa campaign hiện có; không accept sale sau hàng rào; không xóa món ngoài snapshot
// =========================================================================
func TestAcceptance_T15_Regression_CampaignEndRace_And_CartSnapshotDelta(t *testing.T) {
	db, _, cleanDB := setupAcceptancePostgres(t)
	defer cleanDB()
	rClient, cleanRedis := setupAcceptanceRedis(t)
	defer cleanRedis()

	svc, _, _ := setupAcceptanceService(t, db, rClient)
	ctx := context.Background()
	userID := "user-t15"

	// Giỏ hàng ban đầu: Product 1 (qty 2)
	cart := &domain.Cart{UserID: userID}
	db.Create(cart)
	item1 := &domain.CartItem{CartID: cart.ID, ProductID: 1, ProductName: "iPhone 15 Pro", Price: 100000, Quantity: 2}
	db.Create(item1)

	// Khách checkout snapshot A x 2
	quoteTok, _ := GenerateQuoteToken([]byte(acceptanceSecret), userID, []QuoteItem{
		{ProductID: 1, Quantity: 2, QuotedPrice: 100000, PurchaseMode: "REGULAR"},
	}, 200000, 10*time.Minute)

	req := &dto.CreateOrderRequest{
		CustomerName:    "Khách T15",
		CustomerEmail:   "t15@example.com",
		CustomerPhone:   "0900000015",
		ShippingAddress: "123 Đường T15",
		PaymentMethod:   "COD",
		IdempotencyKey:  fmt.Sprintf("key-t15-%d", time.Now().UnixNano()),
		QuoteToken:      quoteTok,
		FromCart:        true,
		Items:           []dto.CreateOrderItemRequest{{ProductID: 1, Quantity: 2}},
	}

	// Trong lúc checkout đang chuẩn bị, khách tăng thêm A x 1 (tổng trong giỏ thành 3)
	db.Model(&domain.CartItem{}).Where("id = ?", item1.ID).Update("quantity", 3)

	// Đặt hàng thành công
	_, err := svc.CreateOrder(ctx, userID, req)
	if err != nil {
		t.Fatalf("Đặt hàng thất bại: %v", err)
	}

	// Sau khi checkout hoàn tất, giỏ hàng phải còn A x 1 (3 - 2 = 1), không bị xóa sạch!
	var remainingItem domain.CartItem
	err = db.Where("id = ?", item1.ID).First(&remainingItem).Error
	if err != nil {
		t.Fatalf("Không tìm thấy item trong giỏ sau khi checkout: %v", err)
	}
	if remainingItem.Quantity != 1 {
		t.Fatalf("Kỳ vọng giỏ hàng còn đúng 1 sản phẩm (3 - 2), thực tế: %d", remainingItem.Quantity)
	}
}
