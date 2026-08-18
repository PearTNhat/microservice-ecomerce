# Hướng dẫn Quản trị Docker & Docker Compose

Tài liệu hướng dẫn quản lý hệ thống hạ tầng Docker cho dự án E-Commerce Microservices.

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