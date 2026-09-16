# Kế hoạch Thực thi Khắc phục Review Vòng 2 (Mục 16 - Flash Sale & Mixed Checkout)

Tài liệu này đánh giá tính chuẩn xác của **Review vòng 2 (Mục 16)** trong [FLASH_SALE_IMPLEMENTATION_PLAN.md](file:///home/nhat/Workspace/microserice-ecomerce/backend/FLASH_SALE_IMPLEMENTATION_PLAN.md) và phác thảo lộ trình kỹ thuật chi tiết để giải quyết dứt điểm toàn bộ 5 yêu cầu bắt buộc (16.1 $\rightarrow$ 16.5).

---

## 1. Kết luận Đánh giá: Reviewer Nói "Chưa Đạt" Có Đúng Không?

> [!IMPORTANT]
> **XÁC NHẬN: NGƯỜI ĐÁNH GIÁ (REVIEWER) NÓI HOÀN TOÀN CHÍNH XÁC 100%!**
> 
> Reviewer có năng lực rất cao và góc nhìn thực chiến xuất sắc về **Distributed Systems, Microservices Concurrency và Saga Patterns**. Mặc dù mã nguồn trước đó đã pass các test Go đơn luồng, nhưng trong môi trường phân tán chịu tải cao với Kafka và PostgreSQL, mã nguồn hiện tại **chắc chắn sẽ gặp phải các lỗi nghiêm trọng sau**:
>
> 1. **Mục 16.1 (P0 - Double Deduction)**: `order_service.go:478` vừa ghi Outbox vừa tự bắn qua Kafka HTTP trực tiếp. Trong `mixed_order_stock_worker.go:161`, hàm `GetEvent` chạy **bên ngoài Transaction**. Khi 2 consumer đọc 2 bản sao cùng lúc, cả 2 đều thấy chưa có event và đều nhảy vào trừ kho thường $\rightarrow$ **Trừ kho 2 lần**.
> 2. **Mục 16.2 (P0 - Cross-Topic Race & Ghost Stock)**: Topic `order.mixed-stock.request` và `order.mixed-stock.compensate` là **hai topic Kafka độc lập**, hoàn toàn không có thứ tự bảo đảm. Nếu consumer bồi hoàn (Compensate) nhận trước request (do delay/lag), hàm `processCompensateMessage` đang chạy `stock = stock + qty` **vô điều kiện** $\rightarrow$ **Cộng khống tồn kho** (phantom stock) cho khách hàng khác mua quá số lượng thật.
> 3. **Mục 16.3 (P0 - Thiếu Atomic Invariant trong Order DB)**: Tại `order_service.go:318`, câu lệnh cập nhật `flash_sale_items.reserved_stock` không có điều kiện chặn `reserved_stock + sold_stock + quantity <= allocated_stock`. Nếu Redis bị lệch hoặc mất đồng bộ, Order DB sẽ âm thầm bán vượt quá chỉ tiêu phân bổ.
> 4. **Mục 16.4 (P0 - Commit Kafka Offset tùy tiện)**: Tại `mixed_order_stock_worker.go:148, 229`, khi gặp lỗi lưu kết quả thất bại hoặc lỗi phát Kafka, worker nuốt lỗi (`_ = ...`) và trả về `nil`, khiến Kafka tự động commit offset $\rightarrow$ Mất thông điệp vĩnh viễn, Order Service bị treo Saga không nhận được kết quả.
> 5. **Mục 16.5 (P1 - Silent Full-Price Charge khi hết Sale)**: Tại `order_service.go:157`, nếu chiến dịch Flash Sale vừa kết thúc 1 giây trước khi bấm Checkout, `GetActiveCampaign` trả về nil $\rightarrow$ Hệ thống âm thầm tính giá thường (full price) mà không hề báo lỗi `409 Conflict`, làm khách hàng bị trừ tiền gấp nhiều lần mà không hề hay biết.

---

## 2. Thiết kế Kỹ thuật Chi tiết cho Mục 16

### 2.1. [16.1 & 16.2] State Machine 5 Trạng Thái & Khóa Đơn Hàng trong Product DB

Tạo bảng quản trị trạng thái xử lý kho hỗn hợp trong Product DB:
```sql
CREATE TABLE mixed_order_stock_operations (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    order_code VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL, -- PENDING, DEDUCTED, FAILED, CANCELLED_BEFORE_DEDUCT, COMPENSATED
    deducted_items JSONB,        -- Lưu chính xác danh sách và số lượng đã thực tế trừ
    reason TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_mixed_stock_order_id UNIQUE (order_id)
);
```

#### Ma trận chuyển trạng thái (State Transition Table):
| Trạng thái hiện tại | Yêu cầu TRỪ KHO (Request) | Yêu cầu BỒI HOÀN (Compensate) |
|---|---|---|
| **Chưa có bản ghi** | Bắt đầu Tx: Khóa `order_id`. Trừ kho theo CAS `stock >= qty`. Nếu đủ $\rightarrow$ insert `DEDUCTED`, ghi outbox result. Nếu thiếu $\rightarrow$ insert `FAILED`, ghi outbox result. | Bắt đầu Tx: Insert bản ghi `CANCELLED_BEFORE_DEDUCT`. **KHÔNG CỘNG KHO!** |
| `DEDUCTED` | Trả lại kết quả Success đã lưu từ `mixed_order_stock_operations`; không trừ lại. | Bắt đầu Tx: Khóa hàng. Cộng lại kho cho đúng các mặt hàng trong `deducted_items`. Chuyển sang `COMPENSATED`. Ghi outbox xác nhận. |
| `FAILED` | Trả lại kết quả Failed đã lưu; không trừ lại. | No-op. Chuyển/giữ trạng thái kết thúc. Không cộng kho. |
| `CANCELLED_BEFORE_DEDUCT`| Không trừ kho! Ghi outbox kết quả CANCELLED/REJECTED. | No-op. Giữ nguyên. |
| `COMPENSATED` | Bỏ qua, không bao giờ trừ lại. | No-op. Trả lại kết quả đã bồi hoàn. |

#### Loại bỏ Direct Publish từ HTTP Request:
- Xóa bỏ hoàn toàn lệnh gọi `PublishMixedOrderStockRequest` inline tại `order_service.go:478`.
- 100% message chỉ được phát qua **Outbox Publisher Worker** để đảm bảo duy nhất 1 nguồn phát chuẩn xác sau khi DB commit.

---

### 2.2. [16.3] Bảo vệ Invariant Allocation trong Order DB

Tại `order_service.go`, thay thế câu lệnh update `reserved_stock`:
```go
res := tx.Model(&domain.FlashSaleItem{}).
    Where("id = ? AND campaign_id = ? AND reserved_stock + sold_stock + ? <= allocated_stock", 
        rev.flashSaleItemID, rev.campaignID, rev.quantity).
    Update("reserved_stock", gorm.Expr("reserved_stock + ?", rev.quantity))

if res.Error != nil {
    return fmt.Errorf("lỗi cập nhật reserved_stock: %w", res.Error)
}
if res.RowsAffected == 0 {
    return fmt.Errorf("ALLOCATION_EXCEEDED: Đợt sale đã hết hạn mức cho item #%d", rev.flashSaleItemID)
}
```
Nếu `RowsAffected == 0`: Rollback toàn bộ transaction của Order DB và hoàn trả toàn bộ các Redis reservation đã giữ chỗ.

---

### 2.3. [16.4] Kỷ luật Commit Kafka Offset & Chống Nuốt Lỗi

- Trong `mixed_order_stock_worker.go`:
  - Mọi lỗi DB, lỗi ghi outbox, hoặc lỗi serialize **bắt buộc phải trả về `error`** để Kafka Reader **KHÔNG COMMIT OFFSET** và tự động retry sau backoff.
  - Khi bắt gặp bản tin JSON hỏng: Gửi vào Dead Letter Queue (DLQ). Chỉ khi DLQ gửi thành công mới được return `nil` để commit offset. Nếu DLQ cũng lỗi $\rightarrow$ return `error` để giữ message.
  - Không bao giờ dùng `_ =` để bỏ qua lỗi ở các vị trí chốt chặn phân tán.

---

### 2.4. [16.5] Giao thức HMAC Server-Signed Quote Token & Re-Quote UX

#### Cấu trúc Quote Token:
Hệ thống cấp cho giỏ hàng 1 token được ký bằng bí mật HMAC-SHA256 của Server:
```json
{
  "user_id": "123",
  "basket_hash": "sha256(item_ids_and_quantities)",
  "items": [
    {"product_id": 1, "is_flash_sale": true, "campaign_id": 10, "expected_price": 50000},
    {"product_id": 2, "is_flash_sale": false, "expected_price": 200000}
  ],
  "total_expected": 250000,
  "expires_at": 1773650000
}
```

#### Xử lý tại Checkout:
1. Client gửi kèm `quote_token` khi gọi `POST /orders/checkout`.
2. Backend verify chữ ký HMAC và kiểm tra thời hạn `expires_at`.
3. Backend giải quyết lại giá (re-resolve) theo thời gian thực:
   - Nếu chiến dịch Flash Sale đã hết hạn hoặc hết suất mà giá nhảy lên giá thường:
   - **TỪ CHỐI TẠO ĐƠN NGAY LẬP TỨC** với HTTP `409 Conflict`.
   - Trả về payload gồm: `error_code: "PRICE_CHANGED"`, danh sách món bị đổi giá, `new_quote_token`, và tổng tiền mới.
4. Client nhận 409 $\rightarrow$ Hiển thị hộp thoại Re-quote rõ ràng với 2 lựa chọn:
   - **Chấp nhận giá mới**: Gửi lại request với `new_quote_token`.
   - **Hủy bỏ món hàng**: Xóa món hàng khỏi giỏ và tính lại.

---

## 3. Danh sách Tệp thay đổi (Proposed Changes)

### Product Service
- [NEW] [mixed_order_stock_operation.go](file:///home/nhat/Workspace/microserice-ecomerce/backend/services/product-service/internal/domain/mixed_order_stock_operation.go): Model `MixedOrderStockOperation`.
- [NEW] [mixed_order_stock_operation_repository.go](file:///home/nhat/Workspace/microserice-ecomerce/backend/services/product-service/internal/repository/mixed_order_stock_operation_repository.go): Repository với row-lock và CAS cho `order_id`.
- [MODIFY] [mixed_order_stock_worker.go](file:///home/nhat/Workspace/microserice-ecomerce/backend/services/product-service/internal/worker/mixed_order_stock_worker.go): Triển khai State Machine 5 trạng thái, loại bỏ `GetEvent` trước tx, kiểm soát chặt chẽ Kafka offset commit.

### Order Service
- [MODIFY] [order_service.go](file:///home/nhat/Workspace/microserice-ecomerce/backend/services/order-service/internal/service/order_service.go):
  - Bỏ direct publish qua Kafka, chỉ dùng Outbox.
  - Cập nhật conditional update `reserved_stock + sold_stock + qty <= allocated_stock`.
  - Xác thực `QuoteToken`. Trả về 409 nếu giá thay đổi hoặc flash sale hết hạn.
- [NEW] [quote_token.go](file:///home/nhat/Workspace/microserice-ecomerce/backend/services/order-service/internal/service/quote_token.go): HMAC token generator & validator.
- [MODIFY] [order_dto.go](file:///home/nhat/Workspace/microserice-ecomerce/backend/services/order-service/internal/dto/order_dto.go): Bổ sung `QuoteToken` vào `CreateOrderRequest` và DTO lỗi 409.
- [MODIFY] [order_handler.go](file:///home/nhat/Workspace/microserice-ecomerce/backend/services/order-service/internal/delivery/http/order_handler.go): Bắt lỗi `PRICE_CHANGED` trả về 409 kèm token mới.

### Frontend
- [MODIFY] [checkout/page.tsx](file:///home/nhat/Workspace/microserice-ecomerce/frontend/src/app/checkout/page.tsx): Tiếp nhận và xử lý HTTP 409 kèm `new_quote_token`.

---

## 4. Kế hoạch Kiểm thử & Xác nhận (Verification Plan)

### Automated Tests
1. **Concurrency Test (16.1 & 16.2)**:
   - Viết test song song 2 worker tiêu thụ cùng 1 `order_id` (với 2 event ID khác nhau) $\rightarrow$ Chứng minh tồn kho chỉ trừ đúng 1 lần.
   - Viết test `Compensate` đến trước `Request` $\rightarrow$ Chứng minh tồn kho không bị tăng ảo và request đến sau bị từ chối an toàn.
2. **Order DB Allocation Invariant Test (16.3)**:
   - Viết test khi `reserved_stock + sold_stock + qty > allocated_stock` $\rightarrow$ Giao dịch rollback hoàn toàn, không tạo order, giải phóng Redis.
3. **Re-quote Test (16.5)**:
   - Test chiến dịch kết thúc trước khi checkout $\rightarrow$ Trả về 409 `PRICE_CHANGED`, không bao giờ âm thầm tạo đơn giá thường.
