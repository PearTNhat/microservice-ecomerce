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
  * `order-service` phát sự kiện `order.created` lên topic `order.events`.
  * `product-service` lắng nghe, trừ kho trong `ecom_product_db` rồi phát `stock.events`.
  * Nếu thành công, `order-service` chuyển trạng thái đơn sang `CONFIRMED`.
  * Nếu thất bại, đơn được bù trừ `CANCELLED` và tự động hoàn kho / ghi nhận vào Dead Letter Topic `orders.dead_letter`.

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
