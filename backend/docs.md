# Hướng dẫn Quản trị Docker & Microservices

Tài liệu hướng dẫn quản lý hệ thống hạ tầng Docker và công cụ giám sát (Graylog, RedisInsight, RabbitMQ) cho dự án E-Commerce Microservices.

---

## ⚡ Khởi Chạy Toàn Bộ Hệ Thống Với 1 Lệnh (All-in-One)

Để khởi động toàn bộ Docker, 4 Microservices Go (API Gateway, User, Product, Order) và Frontend Next.js trong **1 Terminal duy nhất kèm log màu phân biệt**:

```bash
# Từ thư mục gốc dự án:
./start_all.sh

# Hoặc từ thư mục backend:
make run-all
```
*(Bấm `Ctrl + C` để tự động tắt toàn bộ các service sạch sẽ, không lo bị chiếm cổng).*

---

## 🧭 1. Cách Chạy Riêng Lẻ Từng Dịch Vụ (Single Service)

Thay vì bật toàn bộ 8 dịch vụ cùng lúc làm tốn RAM, bạn có thể **chỉ định tên dịch vụ** muốn chạy sau lệnh `docker compose`:

### 📌 Các lệnh chạy từng dịch vụ cụ thể:
- **Chạy riêng Kafka (Event Streaming):**
  ```bash
  docker compose up -d kafka
  ```
- **Chạy riêng RabbitMQ & Giao diện Web Management UI (Message Broker):**
  ```bash
  docker compose up -d rabbitmq
  # Web UI: http://localhost:15672 (guest/guest)
  ```
- **Chạy riêng PostgreSQL (Cơ sở dữ liệu):**
  ```bash
  docker compose up -d postgres
  ```
- **Chạy riêng Redis & Giao diện RedisInsight:**
  ```bash
  docker compose up -d redis redisinsight
  ```
- **Chạy riêng Elasticsearch (Search Engine):**
  ```bash
  docker compose up -d elasticsearch
  ```
- **Chạy riêng Cụm Giám Sát Log Graylog (Graylog + OpenSearch + Mongo):**
  ```bash
  docker compose up -d graylog opensearch mongo
  ```

---

## 🛑 2. Các Lệnh Dừng / Khởi Động Lại Từng Dịch Vụ

- **Dừng 1 dịch vụ cụ thể:**
  ```bash
  docker compose stop <tên_service>
  # Ví dụ: docker compose stop kafka
  ```
- **Khởi động lại 1 dịch vụ:**
  ```bash
  docker compose restart <tên_service>
  # Ví dụ: docker compose restart postgres
  ```
- **Xem log thời gian thực của 1 dịch vụ:**
  ```bash
  docker compose logs -f <tên_service>
  # Ví dụ: docker compose logs -f kafka
  ```

---

## 💾 3. Quản Lý Docker Volumes (Ổ Lưu Trữ Dữ Liệu)

Docker Volumes là nơi lưu giữ dữ liệu an toàn ngay cả khi bạn tắt hoặc xóa container.

- **Xem danh sách tất cả Volumes:**
  ```bash
  docker volume ls
  ```
- **Xem chi tiết dung lượng & đường dẫn lưu trên ổ cứng của 1 Volume:**
  ```bash
  docker volume inspect backend_postgres_data
  docker volume inspect backend_kafka_data
  ```
- **Xóa 1 Volume riêng lẻ (Dùng khi muốn làm mới hoàn toàn dữ liệu):**
  ```bash
  docker volume rm <tên_volume>
  # Ví dụ: docker volume rm backend_kafka_data
  ```
- **Xóa toàn bộ Volumes không còn dùng đến:**
  ```bash
  docker volume prune -f
  ```

---

## 🖥️ 4. Hướng Dẫn Sử Dụng Portainer (Giao Diện Trực Quan Web)

**Portainer** là công cụ giao diện Web giúp bạn bật/tắt container và xem volume bằng một cú click chuột:

### Khởi tạo Portainer (Chỉ chạy 1 lần):
```bash
docker run -d -p 8000:8000 -p 9000:9000 --name portainer --restart=always -v /var/run/docker.sock:/var/run/docker.sock -v portainer_data:/data portainer/portainer-ce:latest
```

### Thông tin Đăng nhập:
- **Đường dẫn:** `http://localhost:9000`
- **Tài khoản:** `admin`
- **Mật khẩu:** `admin123456789`

### Bật / Tắt Portainer:
- **Tắt:** `docker stop portainer`
- **Bật:** `docker start portainer`
- **Gỡ bỏ cài lại:** `docker rm -f portainer`

---

## 📊 5. Hướng Dẫn Sử Dụng Graylog (Quản Lý & Giám Sát Log Tập Trung)

**Graylog** là máy chủ quản lý, thu thập và lập chỉ mục log tập trung cho toàn bộ Microservices trong dự án thông qua chuẩn **GELF UDP**.

### 📌 Thông Tin Đăng Nhập Graylog:
- **Đường dẫn Web UI:** [http://localhost:9000](http://localhost:9000)
- **Tài khoản:** `admin`
- **Mật khẩu:** `admin`
- **Cổng nhận log GELF UDP:** `12201` (đã được cấu hình tự động)

### 🚀 Khởi Động Cụm Graylog (Graylog + OpenSearch + MongoDB):
```bash
docker compose up -d graylog opensearch mongo
```

### 🔍 Cách Tra Cứu & Lọc Log Trên Giao Diện Web (Tab Search):
Vào tab **`Search`** ở thanh menu trên cùng, chọn khung thời gian (ví dụ: `Search in the last 5 minutes`) và bật chế độ **Auto-refresh** (`1s` hoặc `5s`) để xem luồng log realtime.

#### Một số cú pháp tìm kiếm hữu ích (Lucene Query Syntax):
| Mục đích | Cú pháp tìm kiếm |
| :--- | :--- |
| **Theo dấu 1 Request xuyên suốt (Trace ID)** | `trace_id:"ce716647-c629-40d8-bec4-dd5b3340ae74"` |
| **Lọc riêng 1 Microservice** | `service_name:"order-service"` |
| **Lọc nhiều Service cùng lúc** | `service_name:("user-service" OR "api-gateway")` |
| **Tìm toàn bộ lỗi trong hệ thống** | `level:ERROR` |
| **Tìm các Request HTTP thất bại (4xx, 5xx)** | `status:>=400` |
| **Phát hiện các Request xử lý chậm** | `latency_ms:>50` |
| **Tìm theo từ khóa nghiệp vụ** | `"Xác thực email"` OR `"Đặt hàng thành công"` |

### 🛠️ Tùy Biến Cột Hiển Thị (Columns):
Ở thanh sidebar **Fields** bên trái màn hình Search, rê chuột vào các trường và bấm biểu tượng mũi tên để đưa thành cột trên bảng:
- `service_name` • `method` • `path` • `status` • `latency_ms` • `trace_id`