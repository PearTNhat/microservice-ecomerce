# 🛍️ High-Performance E-Commerce Microservices Platform

[![Go Version](https://img.shields.io/badge/Go-1.26-00ADD8?style=flat&logo=go)](https://golang.org)
[![Next.js](https://img.shields.io/badge/Next.js-15-black?style=flat&logo=next.js)](https://nextjs.org/)
[![Apache Kafka](https://img.shields.io/badge/Kafka-3.7-231F20?style=flat&logo=apachekafka)](https://kafka.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql)](https://www.postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat&logo=redis)](https://redis.io)
[![Architecture](https://img.shields.io/badge/Architecture-Clean%20%26%20Saga%20Choreography-success)](#-kiến-trúc-hệ-thống)
[![Acceptance Tests](https://img.shields.io/badge/Acceptance%20Tests-15%2F15%20PASS%20(-race%20-count%3D20)-brightgreen)](#-kiểm-thử-tự-động-automated-testing)

Hệ thống Thương mại Điện tử phân tán (Distributed E-Commerce Platform) hiệu năng cao, được thiết kế theo kiến trúc **Event-Driven Microservices**, tuân thủ nghiêm ngặt **Clean Architecture**, **Database-per-Service**, quản lý giao dịch phân tán bằng **Saga Choreography (100% Apache Kafka)** và sở hữu động cơ **Flash Sale Unified Checkout (Revision 2)** đạt chuẩn production các sàn TMĐT lớn (Shopee, Tiki).

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
├── frontend/                        # Ứng dụng Web Client (Next.js 15 App Router + TypeScript)
│   ├── src/app/                     # Next.js App Router (Màn hình mua sắm, giỏ hàng, unified checkout)
│   └── src/components/              # UI Components tối ưu trải nghiệm người dùng
│
├── start_all.sh                     # Script khởi chạy toàn bộ hệ thống trong 1 lệnh
└── README.md                        # Tài liệu tổng quan dự án
```

---

## 🌟 Điểm Nhấn Công Nghệ & Kỹ Thuật (Key Highlights)

| Tính Năng | Giải Pháp Kỹ Thuật | Mô Tả |
| :--- | :--- | :--- |
| **Flash Sale Unified Checkout** | **Single Canonical Pipeline** | Hợp nhất 100% hàng thường và hàng Flash Sale vào chung 1 giỏ hàng và 1 giao dịch duy nhất (`POST /orders/checkout`). Hỗ trợ chế độ Direct Mua ngay bảo toàn giỏ hàng chính. |
| **Fencing Token Transaction** | **Version uint64 & CAS** | 5 bước nguyên tử trong 1 DB Transaction: SHARE lock campaign $\rightarrow$ check quota $\rightarrow$ Outbox Saga $\rightarrow$ Tạo đơn $\rightarrow$ Chốt attempt với CAS version check, ngăn chặn tuyệt đối Stale Slow Worker. |
| **Chống Late-Reserve** | **Marker CLOSED (Redis Lua)** | Đánh dấu `CLOSED` (0 TTL) trên Redis sau khi cleanup/recovery; Lua script từ chối tức thì các request trễ mạng đến sau thời điểm dọn dẹp, triệt tiêu ghost reservation. |
| **Replay-First Idempotency** | **Canonical SHA-256 Hash** | So khớp fingerprint trước khi check hạn QuoteToken; replay ngay kết quả cũ bất chấp token đã hết hạn. Phát hiện ngay lập tức hành vi sửa body cùng một Idempotency-Key. |
| **Snapshot Delta Cart Cleanup** | **Delta Math Algorithm** | Trừ chính xác delta (`cart.qty - order.qty`), bảo toàn nguyên vẹn các món hàng hoặc số lượng khách thêm mới vào giỏ trong lúc checkout đang in-flight. |
| **Giao dịch phân tán** | **Saga Choreography** | Phối hợp xử lý đơn hàng và trừ kho thường bất đồng bộ qua **Apache Kafka**, tự động kích hoạt **Compensating Transaction** khi hết hàng. |
| **Operation Ledger** | **Idempotent Stock Worker** | Bảng sổ cái `mixed_order_stock_operations` tại Product DB lưu trạng thái `DEDUCTED`/`COMPENSATED`, xử lý triệt để bài toán Late Success và Duplicate Message. |
| **Drain & Settlement Barrier** | **2-Phase Campaign Ending** | Tự động từ chối kết thúc Flash Sale khi còn in-flight reservations (`ReservedStock > 0`); đối soát số lượng bán 2 DB trước khi hoàn trả kho thừa. |

---

## 🔌 Danh Sách Dịch Vụ & Cổng (Services & Ports)

### Microservices Backend
- **API Gateway**: `http://localhost:8000`
- **User Service**: `http://localhost:8001` (gRPC: `:50051`, Database: `ecom_user_db`)
- **Product Service**: `http://localhost:8002` (Database: `ecom_product_db`)
- **Order Service**: `http://localhost:8003` (Database: `ecom_order_db`)

### Frontend & Hạ Tầng
- **Frontend Next.js**: `http://localhost:3000`
- **PostgreSQL**: `localhost:5428` (3 databases: `ecom_user_db`, `ecom_product_db`, `ecom_order_db`)
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

## 🧪 Kiểm Thử Tự Động & Nghiệm Thu (Automated Testing)

Toàn bộ hệ thống được bảo vệ bởi bộ kiểm thử tự động toàn diện, bao gồm cả Unit Tests và bộ **Acceptance Race Tests (T01 – T15)**:

```bash
cd backend

# 1. Chạy toàn bộ quy trình nghiệm thu chuẩn Production (Race Detector lặp 20 lần):
make test-acceptance

# 2. Chạy toàn bộ Unit Tests trong Workspace:
make test

# 3. Chạy kiểm thử luồng Đặt hàng E2E:
./test_e2e_order.sh

# 4. Chạy kiểm thử Flash Sale:
./test_flash_sale.sh
```

### Kết Quả Nghiệm Thu (15/15 PASS - Zero Race Conditions):
```
=== [1/3] Đang chạy Acceptance Tests với Race Detector (15 ca, count=20) ===
ok      ecomerce-service/services/order-service/internal/service    81.377s
=== [2/3] Chạy Unit Tests toàn hệ thống ===
ok      ecomerce-service/services/order-service/internal/service    1.947s
... (Toàn bộ microservices pass 100%)
=== [3/3] Kiểm tra Frontend TypeCheck và Build ===
✓ Compiled successfully in 1178ms
✓ Finished TypeScript in 2.8s
✅ TOÀN BỘ ACCEPTANCE TESTS & PRODUCTION BUILDS HOÀN THÀNH 100%!
```

---

## 📚 Tài Liệu Kỹ Thuật Chi Tiết

- ⚡ [Kiến Trúc Flash Sale Unified Checkout Toàn Diện (Revision 2)](backend/FLASH_SALE_ARCHITECTURE.md)
- 📋 [Kế Hoạch & Checklist Nghiệm Thu 15 Ca Kiểm Thử (T01–T15)](backend/FLASH_SALE_CHECKOUT_ACCEPTANCE_PLAN.md)
- 📖 [Backend Architecture & Clean Architecture Details](backend/ARCHITECTURE.md)
- 📖 [Backend Quick Guide & API Reference](backend/README.md)
- 📖 [Saga Choreography & Kafka Migration Report](backend/KAFKA_SAGA_MIGRATION.md)
- 🤖 [AI Agent Standard Operating Procedures (SOP)](backend/AGENTS.md)
