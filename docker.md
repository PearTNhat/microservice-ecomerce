# Hướng dẫn sử dụng Portainer (Quản lý Docker)

**Portainer** là công cụ giao diện Web giúp bạn quản lý Docker (Containers, Images, Volumes...) một cách trực quan, dễ dàng mà không cần gõ lệnh.

## 1. Lệnh Khởi tạo ban đầu (Chỉ chạy 1 lần)
Chạy lệnh này để tải và bật Portainer lên:
```bash
docker run -d -p 8000:8000 -p 9000:9000 --name portainer --restart=always -v /var/run/docker.sock:/var/run/docker.sock -v portainer_data:/data portainer/portainer-ce:latest
```

## 2. Thông tin Đăng nhập
Sau khi chạy lệnh trên, hãy mở trình duyệt Web và truy cập:
- **Đường dẫn:** `http://localhost:9000`
- **Tài khoản:** `admin`
- **Mật khẩu:** `admin123456789`

*(Lưu ý: Bấm vào mục **Local** để bắt đầu quản lý Docker trên máy này).*

## 3. Các lệnh Bật / Tắt hàng ngày
Vì đã đặt tên là `portainer`, bạn có thể dùng các lệnh ngắn gọn sau:
- **Tắt Portainer:** `docker stop portainer`
- **Bật Portainer:** `docker start portainer`
- **Khởi động lại:** `docker restart portainer`

## 4. Xóa Portainer để cài lại
Nếu ứng dụng bị lỗi và bạn muốn cài lại từ đầu (không mất dữ liệu):
```bash
docker rm -f portainer
```
Sau đó quay lại bước 1 để chạy lệnh khởi tạo lại.