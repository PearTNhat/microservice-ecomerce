# 🚀 Kiến Trúc Enterprise Microservices & Lộ Trình Triển Khai: Sàn TMĐT Điện Máy

> **Tiêu chuẩn:** Enterprise Distributed Systems • High Concurrency • Event-Driven Architecture  
> **Tech Stack:** Golang 1.25 (Clean Architecture) • PostgreSQL • Redis • RabbitMQ • Apache Kafka • Graylog (OpenSearch + MongoDB) • gRPC/Protobuf • Go Fiber • Asynq • Docker

---

## 🏛️ PHẦN 1: BẢN THIẾT KẾ KIẾN TRÚC TOÀN DIỆN

Hệ thống được thiết kế theo mô hình **Monorepo Microservices độc lập**, gồm **API Gateway**, **3 Core Microservices**, và **Hệ thống giám sát Graylog**:

```mermaid
graph TB
    Client["Web App / Mobile App (Người Mua Hàng)"] --> Gateway["API Gateway (Go Fiber) - Port 8000"]

    subgraph IndependentMicroservices["Các Microservices Độc Lập"]
        Gateway -->|"Proxy: /login, /register, /user/*"| S1["1. User Service (REST: 8001 | gRPC: 50051)"]
        Gateway -->|"Proxy: /products/*, /categories, /brands"| S2["2. Product Service (REST: 8002)"]
        Gateway -->|"Proxy: /cart/*, /orders/*"| S3["3. Order Service (REST: 8003 | gRPC: 50053)"]
    end

    subgraph ObservabilityLayer["Giám Sát & Quản Lý Log Tập Trung"]
        S1 & S2 & S3 & Gateway -.->|"GELF UDP / JSON"| Graylog["Graylog Web UI (Port 9000)"]
        Graylog --> OpenSearch[("OpenSearch Storage")]
    end

    subgraph EnterpriseInfra["Hạ Tầng Phân Tán & Message Brokers"]
        S2 -.->|"Kafka: Stream lượt xem máy lạnh/tủ lạnh"| KAFKA[("Apache Kafka (KRaft)")]
        S3 -.->|"RabbitMQ: Saga Order Events"| RMQ[("RabbitMQ (DLX)")]
        RMQ -.->|"Consume gửi mail hóa đơn"| WORKER["Notification & Asynq Worker"]
        S1 & S2 & S3 --> REDIS[("Redis (Singleflight + Lock Flash Sale + OTP)")]
        S1 & S2 & S3 --> PG[("PostgreSQL (JSONB Specs + DB)")]
    end
```

---

## 📊 PHẦN 2: BẢNG MA TRẬN CÔNG NGHỆ VÀ TRÁCH NHIỆM

| Service / Tool | Port | Trách nhiệm chính trong hệ thống | Kỹ thuật Enterprise áp dụng |
| :--- | :--- | :--- | :--- |
| **🌐 API Gateway** | `8000` | - Cửa ngõ duy nhất đón nhận request từ Client.<br>- Điều hướng Reverse Proxy sang các service con. | Rate Limiter (100 req/min), Global CORS, `X-Request-ID` Trace ID. |
| **👤 User Service** | `8001` (gRPC: `50051`) | - Đăng ký nhận OTP qua Email, kích hoạt tài khoản.<br>- Đăng nhập cấp JWT Token, xem Profile người mua hàng. | Asynq Worker, Redis OTP TTL 15m, gRPC Server. |
| **📦 Product Service** | `8002` | - Danh mục, thương hiệu, sản phẩm điện máy.<br>- Thông số kỹ thuật động (BTU, Inverter, dung tích...). | Cache-Aside (Redis), `singleflight` chống sập DB, Kafka Producer. |
| **🛒 Order Service** | `8003` (gRPC: `50053`) | - Giỏ hàng, Đặt hàng, Áp mã Voucher, Thanh toán.<br>- Điều phối trạng thái đơn hàng. | Redis Atomic `DECR` (Flash Sale), `Idempotency-Key`, RabbitMQ Saga. |
| **📊 Graylog** | `9000` (GELF: `12201`) | - Quản lý và tìm kiếm log tập trung toàn hệ thống. | Chuẩn GELF UDP, OpenSearch indexing, Trace ID search. |

---

## 📍 PHẦN 3: BẢNG THEO DÕI TIẾN ĐỘ THỰC TẾ (PROGRESS STATUS)

```mermaid
graph LR
    B1["✅ Bước 1: Observability & User (DONE)"] --> B2["✅ Bước 2: Product & Kafka & Gateway (DONE)"]
    B2 --> B3["✅ Bước 3: Order, Cart & Basic Lock (DONE)"]
    B3 --> B31["⏳ Bước 3.1: Flash Sale Peak Clipping (PLANNED)"]
    B31 --> B4["⏸️ Bước 4: Hoàn Thiện Toàn Diện & CV"]
```

### ✅ BƯỚC 1: Observability (Graylog & Slog), Trace ID & User Service (ĐÃ HOÀN THÀNH 100%)
- [x] Cấu hình cụm **Graylog + OpenSearch + MongoDB** trong `docker-compose.yml`.
- [x] Xây dựng gói **Structured Logger** (`pkg/logger`) dùng `log/slog` gửi GELF UDP tới Graylog.
- [x] Viết Middleware **`X-Request-ID`** tự động gán Trace ID và đo `latency_ms`.
- [x] Viết Middleware **`RBAC`** kiểm tra phân quyền.
- [x] Tinh gọn luồng **Customer / Buyer** (Đăng ký OTP $\rightarrow$ Verify Email $\rightarrow$ Login $\rightarrow$ Profile).
- [x] Viết Unit Test và đạt 100% PASS.

---

### ✅ BƯỚC 2: Product & Catalog Service, Redis Cache & Kafka Stream (ĐÃ HOÀN THÀNH 100%)
- [x] Thiết kế Domain & DB Entity: `Category`, `Brand`, `Product` với cột `specifications (JSONB)`.
- [x] Tạo PostgreSQL Repository hỗ trợ lọc đa chiều (`min_price`, `max_price`, danh mục, thương hiệu, từ khóa `ILIKE`).
- [x] Tự động Seed dữ liệu mẫu thực tế về đồ điện máy (Daikin, Panasonic, Samsung, Bosch...).
- [x] Xây dựng cơ chế **Cache-Aside Redis** kết hợp **`singleflight`** chống sập database (Cache Stampede).
- [x] Tích hợp **Apache Kafka Producer** bắn event `ProductViewedEvent` ngầm.
- [x] Xây dựng **Kafka View Consumer Worker** (`internal/worker/product_view_worker.go`): Gom batch lượt xem định kỳ 5s cập nhật vào PostgreSQL và tự động xóa Cache Redis.
- [x] Cung cấp REST endpoints: `GET /categories`, `GET /brands`, `GET /products`, `GET /products/:id`.
- [x] Chuyển đổi thành công kiến trúc **Monorepo Microservices** với các binary độc lập:
  - `cmd/api-gateway/main.go` (Port 8000)
  - `cmd/user-service/main.go` (Port 8001 & gRPC 50051)
  - `cmd/product-service/main.go` (Port 8002)
- [x] Viết `Makefile` tự động hóa build/run và kiểm thử đạt 100% PASS.

---

### ✅ BƯỚC 3: Order & Cart Service, Redis Flash Sale Lock & RabbitMQ (ĐÃ HOÀN THÀNH CƠ BẢN)
- [x] Thiết kế Domain & Entity: `Cart`, `CartItem`, `Order`, `OrderItem`.
- [x] Viết API Giỏ hàng: `GET /cart`, `POST /cart/add`, `PUT /cart/items/:id`, `DELETE /cart/items/:id`, `DELETE /cart`.
- [x] Áp dụng **`Idempotency-Key` Middleware** chống bấm đặt hàng / thanh toán 2 lần bằng Redis `SETNX`.
- [x] Xây dựng module **Flash Sale Atomic Lock với Redis (`DECR` / Lua Script)**: Chống bán âm kho khi hàng nghìn người cùng tranh mua.
- [x] Cấu hình **RabbitMQ (Topic Exchange & Dead Letter Queue - DLX)**:
  - `Order Service` tạo đơn $\rightarrow$ Publish event `order.created`.
  - `Worker` nhận event $\rightarrow$ Gửi email hóa đơn xác nhận đơn hàng ngầm.
- [x] Viết `cmd/order-service/main.go` (Port 8003) và cấu hình proxy trên `API Gateway` (Port 8000).
- [x] Viết Unit Test cho Order Service & Middlewares đạt 100% PASS.

---

### ⚡ BƯỚC 3.1: Nâng Cấp Kiến Trúc Flash Sale High-Concurrency Chống Sập Hệ Thống (LỘ TRÌNH THỰC THI)

> **Mục tiêu:** Đảm bảo hệ thống chịu được **50.000 - 100.000+ QPS** trong các chiến dịch Flash Sale mà **Database PostgreSQL và Gateway hoàn toàn KHÔNG THỂ BỊ SẬP**.

```mermaid
sequenceDiagram
    autonumber
    actor User as Người Mua Hàng
    participant Gateway as API Gateway (Rate Limit + AntiBot)
    participant Redis as Redis Cluster (Stock Lock + Status)
    participant Queue as RabbitMQ (flashsale.orders.queue)
    participant Worker as FlashSaleOrderWorker
    participant DB as PostgreSQL (ecom_order_db)

    Note over User,Gateway: 1. TIẾP NHẬN & TRỪ KHO SIÊU TỐC TRÊN RAM (< 5ms)
    User->>Gateway: POST /orders/flash-sale (ProductID, Customer Info)
    Gateway->>Gateway: Kiểm tra Rate Limit & Idempotency Key
    Gateway->>Redis: Chạy Lua Script (Trừ tồn kho Atomic + Check 1 user/1 món)
    
    alt Hết hàng (Stock <= 0)
        Redis-->>Gateway: Trả về 0 (Out of stock)
        Gateway-->>User: 400 Bad Request ("Sản phẩm đã hết hàng hoặc không đủ tồn kho")
    else Trừ kho thành công (Còn hàng)
        Redis-->>Gateway: Trả về 1 (Thành công)
        Gateway->>Redis: Set Key "flash_sale:order:<token>" = {"status": "PENDING"} (TTL 10m)
        Gateway->>Queue: Bắn Task tạo đơn vào RabbitMQ "flashsale.orders.queue"
        Gateway-->>User: 202 Accepted {"order_token": "FSO-xxx", "status": "PENDING", "message": "Đang xếp hàng tạo đơn"}
    end

    Note over Queue,DB: 2. CẮT ĐỈNH TẢI (PEAK CLIPPING) & GHI DB TỪ TỪ
    Queue->>Worker: Consume Order Task (Prefetch kiểm soát 20-50 msg/lần)
    Worker->>DB: INSERT INTO orders & order_items (An toàn tuyệt đối cho Connection Pool)
    
    alt Ghi Database Thành Công
        Worker->>Redis: Cập nhật "flash_sale:order:<token>" = {"status": "SUCCESS", "order_id": 123}
        Worker->>Queue: Ack & Publish event "order.created" gửi email
    else Lỗi DB nghiêm trọng
        Worker->>Redis: Hoàn lại kho (RevertStockAtomic)
        Worker->>Redis: Cập nhật "flash_sale:order:<token>" = {"status": "FAILED", "reason": "..."}
        Worker->>Queue: Ack / Đẩy vào Dead Letter Queue (DLQ)
    end

    Note over User,Redis: 3. POLLING TRẠNG THÁI (ZERO DB HIT)
    loop Polling mỗi 1-2s
        User->>Gateway: GET /orders/flash-sale/status/:token
        Gateway->>Redis: Đọc trực tiếp từ RAM Redis (Không query Postgres)
        Redis-->>User: {"status": "SUCCESS", "order_id": 123, "order_code": "ORD-xxx"}
    end
```

#### 📋 Danh sách Task Cần Thực Thi:
- [ ] **1. Quản lý Tồn kho & Nạp trước (Stock Pre-warming):**
  - Viết helper / endpoint `POST /orders/flash-sale/prewarm` cho phép Admin nạp trước số lượng sản phẩm lên key Redis `product:stock:<id>` trước giờ G.
  - Bổ sung cơ chế Rollback an toàn đa món (`RevertStockAtomic`) nếu giỏ hàng bị lỗi giữa chừng.
- [ ] **2. Kênh Message Queue Cắt Đỉnh Tải (RabbitMQ Flash Sale):**
  - Khai báo Exchange `ecom.flashsale.topic`, Queue `flashsale.orders.queue`, Routing key `flashsale.order.create` trong [producer.go](file:///home/nhat/Workspace/microserice-ecomerce/backend/pkg/rabbitmq/producer.go).
  - Bổ sung struct `FlashSaleOrderTaskPayload` và hàm `PublishFlashSaleOrderTask()`.
- [ ] **3. Order Service - Luồng Bất Đồng Bộ (Async Flash Sale):**
  - Thêm phương thức `CreateFlashSaleOrderAsync()`: Trừ kho trên Redis $\rightarrow$ Lưu trạng thái `PENDING` $\rightarrow$ Đẩy task vào RabbitMQ $\rightarrow$ Trả về mã Token `FSO-...` (HTTP 202 Accepted).
  - Thêm phương thức `GetFlashSaleOrderStatus()`: Đọc kết quả tạo đơn trực tiếp từ RAM Redis (Zero DB Hit).
- [ ] **4. Flash Sale Order Worker (`internal/worker/flash_sale_worker.go`):**
  - Lắng nghe `flashsale.orders.queue` với `Qos(20)`.
  - Ghi đơn hàng vào PostgreSQL một cách ổn định, tự động hoàn trả kho Redis nếu DB lỗi và cập nhật trạng thái `SUCCESS` / `FAILED`.
- [ ] **5. REST Endpoints & Gateway Routing:**
  - `POST /orders/flash-sale` (Đặt hàng Flash Sale bất đồng bộ).
  - `GET /orders/flash-sale/status/:token` (Kiểm tra tiến độ đơn hàng).
  - `POST /orders/flash-sale/prewarm` (Nạp trước kho - Admin).

---

### ⏸️ BƯỚC 4: Hoàn Thiện Toàn Bộ Hệ Thống & Đóng Gói CV (GIAI ĐOẠN CUỐI)
- [ ] Bật toàn bộ dịch vụ trong `docker-compose.yml` (Postgres, Redis, Kafka, RabbitMQ, Graylog, Gateway, Services).
- [ ] Viết tài liệu `README.md` chuyên nghiệp có sơ đồ kiến trúc để đưa vào CV.
- [ ] Benchmark hiệu năng (ab / k6) ghi lại số liệu chịu tải ấn tượng cho buổi phỏng vấn.

---

## 💻 HƯỚNG DẪN CHẠY HỆ THỐNG HIỆN TẠI

```bash
# 1. Khởi chạy toàn bộ hạ tầng Docker (Postgres, Redis, Graylog, OpenSearch, Mongo)
make run-docker

# 2. Chạy kiểm thử tự động toàn bộ codebase
make test

# 3. Biên dịch tất cả 3 Microservices
make build-all

# 4. Khởi chạy từng Service trên các terminal riêng:
# Terminal 1:
make run-user        # Chạy User Service tại :8001 (gRPC: 50051)

# Terminal 2:
make run-product     # Chạy Product Service tại :8002

# Terminal 3:
make run-gateway     # Chạy API Gateway tại :8000
```
