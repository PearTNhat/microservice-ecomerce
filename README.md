# 🛍️ High-Performance E-Commerce Microservices Platform

[![Go Version](https://img.shields.io/badge/Go-1.26-00ADD8?style=flat&logo=go)](https://golang.org)
[![Next.js](https://img.shields.io/badge/Next.js-15-black?style=flat&logo=next.js)](https://nextjs.org/)
[![Apache Kafka](https://img.shields.io/badge/Kafka-3.7-231F20?style=flat&logo=apachekafka)](https://kafka.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql)](https://www.postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat&logo=redis)](https://redis.io)
[![Architecture](https://img.shields.io/badge/Architecture-Clean%20%26%20Saga%20Choreography-success)](#-kiến-trúc-hệ-thống)

Hệ thống Thương mại Điện tử phân tán (Distributed E-Commerce Platform) hiệu năng cao, được thiết kế theo kiến trúc **Event-Driven Microservices**, tuân thủ nghiêm ngặt **Clean Architecture**, **Database-per-Service**, quản lý giao dịch phân tán bằng **Saga Choreography (100% Apache Kafka)** và xử lý tranh chấp Flash Sale bằng **Redis Distributed Lock**.

---

## 🏛 Kiến Trúc Hệ Thống (System Architecture)

```
microservice-ecomerce/
├── backend/                         # Hệ thống Backend Go Microservices
│   ├── go.work                      # Multi-Module Go Workspace
│   ├── docker-compose.yml           # PostgreSQL, Redis, Kafka, Elasticsearch
│   ├── infra/                       # Script tự động khởi tạo 3 Database riêng
│   ├── pkg/                         # Module dùng chung (Kafka, Redis Lock, Middlewares...)
│   └── services/                    # 4 Microservices độc lập
│       ├── api-gateway/             # Cổng điều hướng & Auth (:8000)
│       ├── user-service/            # Quản lý User & gRPC (:8001, gRPC :50051)
│       ├── product-service/         # Sản phẩm & Flash Sale Atomic Lock (:8002)
│       └── order-service/           # Đơn hàng, Giỏ hàng & Kafka Saga Worker (:8003)
│
├── frontend/                        # Ứng dụng Web Client (Next.js 15 + TypeScript)
│   ├── src/app/                     # Next.js App Router (Màn hình mua sắm, giỏ hàng, flash sale)
│   └── src/components/              # UI Components tối ưu trải nghiệm người dùng
│
├── start_all.sh                     # Script khởi chạy toàn bộ hệ thống trong 1 lệnh
└── README.md                        # Tài liệu tổng quan dự án
```

---

## 🌟 Điểm Nhấn Công Nghệ & Kỹ Thuật (Key Highlights)

| Tính Năng | Giải Pháp Kỹ Thuật | Mô Tả |
| :--- | :--- | :--- |
| **Giao dịch phân tán** | **Saga Choreography** | Phối hợp xử lý đơn hàng và trừ kho bất đồng bộ qua **Apache Kafka**, tự động kích hoạt **Compensating Transaction** khi hết hàng. |
| **Bảo vệ toàn vẹn dữ liệu** | **Database-per-Service** | Mỗi microservice sở hữu một database PostgreSQL riêng biệt, loại bỏ hoàn toàn Shared Database anti-pattern. |
| **Chống bán âm (Flash Sale)** | **Redis Atomic Lua Script** | Kiểm tra và trừ tồn kho trực tiếp trên RAM với độ trễ < 1ms, chặn đứng race-condition và overselling khi hàng ngàn người mua cùng lúc. |
| **Chống gửi trùng đơn** | **Idempotency Key Middleware** | Chặn double-click và mạng lag bằng cơ chế Redis Lock + TTL, tự động trả `HTTP 409 Conflict`. |
| **Clean Architecture** | **Domain-Driven Isolation** | Tách biệt 4 tầng: Domain $\rightarrow$ Use Cases $\rightarrow$ Adapters $\rightarrow$ Delivery. Lớp lõi không phụ thuộc vào bất kỳ framework nào. |
| **Theo dõi phân tán** | **Structured Logger + Trace ID** | Tự động sinh và truyền `X-Trace-ID` xuyên suốt từ Gateway qua các service REST/gRPC. |

---

## 🔌 Danh Sách Dịch Vụ & Cổng (Services & Ports)

### Microservices Backend
- **API Gateway**: `http://localhost:8000`
- **User Service**: `http://localhost:8001` (gRPC: `:50051`, Database: `ecom_user_db`)
- **Product Service**: `http://localhost:8002` (Database: `ecom_product_db`)
- **Order Service**: `http://localhost:8003` (Database: `ecom_order_db`)

### Frontend & Hạ Tầng
- **Frontend Next.js**: `http://localhost:3000`
- **PostgreSQL**: `localhost:5428`
- **Redis**: `localhost:6379`
- **Apache Kafka**: `localhost:9092`
- **Elasticsearch**: `localhost:9200`

---

## 🚀 Hướng Dẫn Khởi Động Nhanh (Quick Start)

### 1. Khởi động Hạ tầng Docker:
```bash
cd backend
docker compose up -d
```
> Script khởi tạo sẽ tự động tạo đủ 3 database độc lập: `ecom_user_db`, `ecom_product_db`, `ecom_order_db`.

### 2. Khởi chạy toàn bộ Microservices:
```bash
# Đứng tại thư mục backend:
./start_all.sh run

# Kiểm tra trạng thái hoạt động:
./start_all.sh status
```

### 3. Khởi chạy Frontend:
```bash
cd frontend
npm install
npm run dev
```
Truy cập giao diện tại: `http://localhost:3000`

---

## 🧪 Kiểm Thử Tự Động (Automated Testing)

```bash
cd backend

# Chạy toàn bộ Unit Tests trong Workspace:
make test

# Chạy kiểm thử luồng Đặt hàng E2E (End-to-End):
./test_e2e_order.sh

# Chạy kiểm thử Flash Sale (Giả lập tranh chấp kho cao điểm):
./test_flash_sale.sh
```

---

## 📚 Tài Liệu Chi Tiết

- 📖 [Backend Architecture & Clean Architecture Details](backend/ARCHITECTURE.md)
- 📖 [Backend Quick Guide & API Reference](backend/README.md)
- 📖 [Saga Choreography & Kafka Migration Report](backend/KAFKA_SAGA_MIGRATION.md)
- 🤖 [AI Agent Standard Operating Procedures (SOP)](backend/AGENTS.md)
