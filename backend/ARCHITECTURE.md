# 🏗️ Kiến Trúc Hệ Thống Microservices (Standard Production Go)

Hệ thống Backend E-Commerce được tổ chức theo mô hình **Multi-Service Monorepo** chuẩn mực của các công ty công nghệ lớn trong hệ sinh thái Golang (như Uber, Grab), bảo đảm tính cô lập biên giới (Boundary Isolation) tuyệt đối giữa các dịch vụ.

---

## 1. Sơ Đồ Cấu Trúc Dự Án (Directory Layout)

```text
backend/
├── go.work                                 # Go Workspace quản lý liên kết tất cả 5 modules
├── go.work.sum
├── docker-compose.yml                      # Hạ tầng: PostgreSQL, Redis, Apache Kafka
├── start_all.sh                            # Script 1-click khởi động & quản lý 4 microservices
├── test_e2e_order.sh                       # Kiểm thử luồng đặt hàng E2E qua Gateway
├── test_flash_sale.sh                      # Kiểm thử đồng thời Flash Sale (Redis Lock + Kafka)
│
├── pkg/                                    # [MODULE DÙNG CHUNG - ecomerce-service/pkg]
│   ├── go.mod                              # Module riêng biệt
│   ├── config/                             # Cấu hình nạp .env đồng nhất cho toàn hệ thống
│   ├── elasticsearch/                      # Client Elasticsearch 8 (Search Engine)
│   ├── kafka/                              # Kafka Producer, Topics & Saga Event Schemas
│   ├── logger/                             # Structured Zap/Slog Logger tích hợp Graylog GELF
│   ├── middlewares/                        # Auth JWT, RequestID, CORS, Idempotency Lock
│   ├── redislock/                          # Khóa phân tán Redis Lock cho Flash Sale
│   ├── response/                           # Chuẩn hóa JSON Response {status, code, data}
│   ├── server/                             # Fiber Server & DB GORM Bootstrap
│   └── utils/                              # Hashing, Password, Token Generators
│
└── services/                               # [CÁC MICROSERVICES ĐỘC LẬP]
    │
    ├── api-gateway/                        # Cổng vào duy nhất từ Frontend / Client (:8000)
    │   ├── go.mod                          # Module ecomerce-service/services/api-gateway
    │   ├── Dockerfile
    │   └── cmd/main.go                     # Reverse Proxy điều hướng sang 8001, 8002, 8003
    │
    ├── user-service/                       # Quản lý tài khoản, OTP & JWT (:8001, gRPC :50051)
    │   ├── go.mod                          # Module ecomerce-service/services/user-service
    │   ├── Dockerfile
    │   ├── proto/user.proto                # Protobuf contract
    │   ├── cmd/main.go                     # REST + gRPC Server
    │   └── internal/                       # [CÔ LẬP]: Chỉ user-service mới được import
    │       ├── domain/                     # User entity & UserRepository interface
    │       ├── dto/                        # Login, Register DTOs
    │       ├── repository/                 # Thao tác DB riêng: ecom_user_db
    │       ├── service/                    # Business logic (hash pass, OTP Redis, Asynq)
    │       ├── delivery/                   # REST handler (HTTP) & gRPC handler
    │       └── worker/                     # Asynq Background Worker gửi email xác thực
    │
    ├── product-service/                    # Quản lý sản phẩm, danh mục & tồn kho (:8002)
    │   ├── Dockerfile
    │   ├── cmd/main.go                     # REST Server
    │   └── internal/
    │       ├── domain/                     # Product, Category, Brand entities
    │       ├── dto/                        # Product DTOs
    │       ├── repository/                 # Thao tác DB riêng: ecom_product_db
    │       ├── service/                    # Business logic sản phẩm & Elasticsearch
    │       ├── delivery/http/              # REST handler (lấy danh mục, chi tiết sp)
    │       └── worker/                     # Kafka Stock Worker: trừ kho tự động khi nhận event
    │
    └── order-service/                      # Đặt hàng, Giỏ hàng & Flash Sale (:8003)
        ├── Dockerfile
        ├── cmd/main.go                     # REST Server
        └── internal/
            ├── domain/                     # Order, OrderItem, Cart entities
            ├── dto/                        # Order & Cart DTOs
            ├── repository/                 # Thao tác DB riêng: ecom_order_db
            ├── client/                     # ProductClient: gọi HTTP nội bộ sang :8002 (Zero DB Sharing)
            ├── service/                    # Business logic đơn hàng & giỏ hàng
            ├── delivery/http/              # REST handler (checkout, idempotency, flash sale)
            └── worker/                     # Kafka Saga Worker & Kafka Email Invoicing Worker
```

---

## 2. Các Ranh Giới Kỹ Thuật (Architectural Boundaries)

### 2.1. Go Compiler Boundary (`internal/`)
* Trong Go, bất kỳ package nào nằm dưới thư mục `services/<service-name>/internal/` **chỉ có thể được import bởi chính service đó**.
* Nếu `order-service` cố tình import `services/product-service/internal/...`, trình biên dịch Go sẽ **chặn đứng ngay lập tức**:
  `use of internal package ... not allowed`.
* Không một lập trình viên nào có thể vô tình gọi lén repository hay database của service khác.

### 2.2. Database-per-Service (Không dùng chung DB)
* Mỗi service sở hữu database riêng biệt hoàn toàn:
  * `user-service` $\rightarrow$ `ecom_user_db`
  * `product-service` $\rightarrow$ `ecom_product_db`
  * `order-service` $\rightarrow$ `ecom_order_db`
* Khi `order-service` cần kiểm tra thông tin sản phẩm, nó gọi qua `ProductClient` (REST API nội bộ `:8002` kết hợp Redis Cache), tuyệt đối không đụng vào `ecom_product_db`.

### 2.3. Event-Driven Messaging (100% Apache Kafka)
* Mọi giao dịch phân tán giữa các service được xử lý qua mô hình **Saga Choreography** với Kafka:
  * Checkout hàng thường và giỏ hỗn hợp ghi order `PENDING` cùng `mixed.stock.request` outbox trong Order DB transaction.
  * `product-service` xử lý stock request với operation ledger và Product DB outbox; phát `mixed.stock.result` bền vững.
  * `order-service` chỉ phát `order.created` sau khi stock result thành công và order `CONFIRMED`; Product Service không trừ kho từ `order.created` lần nữa.
  * Legacy `ProductStockWorker`/`OrderSagaWorker` (`order.created → stock.events`) không được khởi chạy. Trước khi nâng cấp môi trường có backlog luồng cũ, phải đối soát/migrate các order `PENDING` và offset liên quan; không reset offset để xử lý lại mù quáng.
  * Nếu thành công, `order-service` chuyển trạng thái đơn sang `CONFIRMED`.
  * Nếu thất bại, đơn được bù trừ `CANCELLED` và tự động hoàn kho / ghi nhận vào Dead Letter Topic `orders.dead_letter`.

### 2.4. Tiêu Chuẩn Cam Kết Offset & Độ Bền Vững Kafka Consumer (Consumer Integrity Invariant)
* **Synchronous Commit (`CommitInterval: 0`)**: Toàn bộ reader xử lý giao dịch Saga (`mixed.stock.request`, `mixed.stock.compensate`, `mixed.stock.result`, `mixed.stock.compensate_result`, `flashsale.confirmed`, `flashsale.orders`) thiết lập `CommitInterval: 0` để lệnh `CommitMessages` gửi yêu cầu đồng bộ trực tiếp tới Kafka broker, bảo đảm broker ghi nhận offset bền vững trước khi tiếp tục.
* **In-Place Retry Loop (Chống Bỏ Qua Offset Lỗi)**: Tuyệt đối không sử dụng mẫu `FetchMessage -> error -> continue -> FetchMessage`. Khi một message `m` gặp lỗi kết nối DB hoặc deadlock, consumer thực hiện vòng retry nội bộ cho chính `m` với exponential backoff. Tuyệt đối không fetch message kế tiếp trên partition khi `m` chưa đạt trạng thái bền vững (durable state).
* **Cam Kết Sau Kết Quả Bền Vững**: Offset chỉ được commit sau khi DB transaction đã hoàn tất thành công hoặc sự kiện đã được gửi sang Dead Letter Queue (DLQ) thành công. Nếu gửi DLQ lỗi, consumer tiếp tục retry thay vì commit bỏ sót.
* **Commit Retry Loop**: Nếu gọi `CommitMessages` gặp lỗi mạng tạm thời, consumer thử lại việc commit cho đến khi broker phản hồi thành công trước khi chuyển sang fetch message tiếp theo.

### 2.5. Động Cơ Flash Sale Unified Checkout (Revision 2)
Hệ thống áp dụng kiến trúc **Unified Checkout Đơn Nhất** chuẩn production, hợp nhất 100% hàng thường và hàng Flash Sale vào 1 giỏ hàng và 1 giao dịch đặt hàng duy nhất (`POST /orders/checkout`), được mô tả chi tiết tại [FLASH_SALE_ARCHITECTURE.md](FLASH_SALE_ARCHITECTURE.md) và [FLASH_SALE_CHECKOUT_ACCEPTANCE_PLAN.md](FLASH_SALE_CHECKOUT_ACCEPTANCE_PLAN.md):

1. **Pipeline Checkout Đơn Nhất (Canonical Checkout)**:
   - **Endpoint**: `POST /orders/checkout/quote` (Báo giá & Ký số `QuoteToken` HMAC-SHA256) và `POST /orders/checkout` (Đặt hàng chuẩn hóa, hỗ trợ `from_cart: true/false`).
   - **Frontend Attempt Envelope (R3)**: Lưu phiên đặt hàng trong `sessionStorage`, tự động retry tối đa 5 lần với exponential backoff kèm jitter $\pm 20\%$. Hỗ trợ khôi phục tiến trình khi reload trang.
   - **Canonical Request Fingerprint (R5)**: Băm SHA-256 trên struct đã chuẩn hóa (sắp xếp ID sản phẩm tăng dần, gộp dòng trùng, lowercase email). Phát hiện ngay lập tức hành vi sửa đổi nội dung với cùng một Idempotency-Key (`409 IDEMPOTENCY_CONFLICT`).

2. **Transaction Fencing 5 Bước Nguyên Tử & CAS State Machine (R1 & R4)**:
   - Trong 1 Transaction duy nhất tại `ecom_order_db`: Lấy `FOR SHARE` lock trên campaign $\rightarrow$ Kiểm tra quota Flash Sale $\rightarrow$ Chèn Outbox event Saga $\rightarrow$ Tạo `orders` $\rightarrow$ Chốt `checkout_attempts` sang `COMPLETED` với Fencing Version CAS check.
   - Ngăn chặn triệt để Stale Slow Worker ghi đè kết quả khi lease bị quá hạn. Phân giải Commit Outcome trước khi giải phóng Redis để loại bỏ hoàn toàn nguy cơ bán âm kho.

3. **Marker `CLOSED` Trên Redis Lua Script (R2)**:
   - Sau khi attempt bị cleanup hoặc recovery, hệ thống ghi marker `CLOSED` (0 TTL) trên Redis; Lua script `ReserveFlashSaleStock` từ chối tức thì các request trễ mạng sau 120s, triệt tiêu ghost reservation và rò rỉ quota.

4. **Snapshot Delta Cart Cleanup**:
   - Dọn giỏ hàng theo quantity delta (`cartItem.Quantity - orderItem.Quantity`), bảo toàn chính xác các sản phẩm mà khách hàng thêm mới trong lúc request checkout đang in-flight.

5. **Saga Trừ Kho Có Operation Ledger & Barrier Bảo Vệ (Product Service)**:
   - Bảng `mixed_order_stock_operations` lưu vết `DEDUCTED`/`COMPENSATED`, xử lý an toàn duplicate message và Late Success.
   - **Drain Barrier & Settlement Barrier**: Chỉ cho phép kết thúc Flash Sale khi toàn bộ reservation in-flight đã xả cạn (`ReservedStock == 0`) và sổ cái 2 database đối soát khớp 100% (`Product.sold == Order.sold`).


---

## 3. Triển Khai Độc Lập Lên Cloud (Docker & Kubernetes)

Mỗi service đều có một file `Dockerfile` tối ưu nhiều tầng (Multi-stage build):
1. **Chỉ copy thư mục của chính service đó và `pkg/`**.
2. Không copy code của các service khác.
3. Tạo ra Docker Image cực kỳ gọn nhẹ (~20MB dựa trên Alpine Linux).
4. Có thể đẩy lên Docker Hub / AWS ECR và deploy độc lập lên Kubernetes / Cloud Run / AWS ECS.

```bash
# Ví dụ build và deploy độc lập order-service:
docker build -f services/order-service/Dockerfile -t my-ecom/order-service:v1 .
docker run -d -p 8003:8003 my-ecom/order-service:v1
```

---

## 4. Lệnh Vận Hành Nhanh

| Mục đích | Lệnh thực thi |
| :--- | :--- |
| **Khởi động toàn bộ 4 service (ngầm)** | `./start_all.sh run -d` |
| **Kiểm tra trạng thái & port** | `./start_all.sh status` |
| **Xem log thời gian thực** | `./start_all.sh logs [gateway\|user\|product\|order]` |
| **Dừng sạch sẽ toàn bộ services** | `./start_all.sh stop` |
| **Khởi động lại** | `./start_all.sh restart -d` |
| **Chạy kiểm thử luồng đặt hàng E2E** | `./test_e2e_order.sh` |
| **Chạy kiểm thử chịu tải Flash Sale** | `./test_flash_sale.sh` |
