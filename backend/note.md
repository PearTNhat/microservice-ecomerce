Hôm trước có đi phỏng vấn được hỏi đến vấn đề transaction trong microservice.
Bài toán đặt ra cho em là service A thực hiện xong rồi sẽ call sang service B xử lý tiếp, sau đó lại trả về cho service A xử lý nốt phần còn lại.
Bên pv có hỏi em
Em xử lý transaction ở đây như thế nào nếu service B xử lý lỗi
và tiếp đó xử lý như nào nếu service B xong rồi trả về service A nhưng đến lúc đó service A mới lỗi.
Mong mng có thể chỉ cho em hướng tiếp cận với bài toán để em có thể tìm hiểu thêm.
Em cũng muốn hỏi thêm là về SQL thì e nên tìm hiểu sâu hơn về phần nào?

Tìm hiểu distributed transaction nhé, tiêu biểu có saga, 2pc, nên xem ưu nhược điểm mỗi loại


Câu hỏi rất hay! Để hiểu rõ, chúng ta cần phân biệt giữa **môi trường máy tính của bạn hiện tại (Local/Dev)** và **môi trường thực tế khi chạy thật (Production)**:

---

### 1. Hiện tại trên máy bạn: Chỉ có ĐÚNG 1 MÁY CHỦ (1 Container Docker)
- Trên máy của bạn lúc này, bạn chỉ bật 1 container `redis:7-alpine` trong [docker-compose.yml](file:///home/nhat/Workspace/microserice-ecomerce/backend/docker-compose.yml#L18-L25). 
- Toàn bộ các key (`fs`, `cache`, `asynq`,...) **đang cùng nằm trong RAM của 1 máy tính của bạn**.
- 👉 Trên máy dev, dù bạn **có dùng hay không dùng `{...}` thì code vẫn chạy được** vì tất cả đều ở chung 1 chỗ.

---

### 2. Nhưng khi lên Production (Hệ thống thật có hàng triệu người dùng): Vì sao lại cần NHIỀU MÁY CHỦ?

Hãy hình dung ngày hội **Flash Sale 11/11 của Shopee hoặc Lazada**:
- Hàng trăm ngàn người cùng bấm nút "Mua" trong 1 giây.
- 1 máy chủ bình thường chỉ có ví dụ **32GB RAM** và **CPU xử lý tối đa được 100.000 requests/giây**.
- Nếu dồn toàn bộ dữ liệu của cả sàn TMĐT (hàng triệu sản phẩm, giỏ hàng, thông tin session, tồn kho) vào **chỉ 1 máy chủ duy nhất**:
  - RAM máy chủ sẽ bị **tràn (Out of Memory - OOM)**.
  - CPU chạm ngưỡng **100%**, máy chủ bị treo, cả hệ thống sập.

---

### 3. Redis lưu dữ liệu lên nhiều máy chủ kiểu gì? (Cơ chế Sharding / Redis Cluster)

Để giải quyết vấn đề quá tải, người ta dùng mô hình **Redis Cluster** (Cụm phân tán). Thay vì mua 1 cái máy siêu to đắt tiền, người ta ghép **nhiều máy chủ nhỏ lại với nhau**:

```
                              [REDIS CLUSTER]
            ┌────────────────────────┼────────────────────────┐
            ▼                        ▼                        ▼
      [MÁY CHỦ 1]              [MÁY CHỦ 2]              [MÁY CHỦ 3]
     (Slot 0 -> 5460)        (Slot 5461 -> 10922)    (Slot 10923 -> 16383)
            │                        │                        │
       Lưu các key              Lưu các key              Lưu các key
       băm ra số nhỏ            băm ra số vừa            băm ra số lớn
```

#### Redis chia dữ liệu thế nào?
Redis chia toàn bộ không gian dữ liệu làm **16.384 "ngăn chứa" (gọi là Hash Slot)**:
- Máy chủ 1 giữ từ ngăn `0` đến `5460`
- Máy chủ 2 giữ từ ngăn `5461` đến `10922`
- Máy chủ 3 giữ từ ngăn `10923` đến `16383`

Khi bạn `SET` một key bất kỳ, Redis sẽ tính toán:
$$\text{Ngăn (Slot)} = \text{Băm}(\text{Tên Key}) \pmod{16384}$$

* Ví dụ:
  - Bạn lưu key `"ao_thun"` $\rightarrow$ Băm ra ngăn **1000** $\rightarrow$ Tự động bay sang **Máy 1** lưu.
  - Bạn lưu key `"quan_jean"` $\rightarrow$ Băm ra ngăn **8000** $\rightarrow$ Tự động bay sang **Máy 2** lưu.
  - Bạn lưu key `"giay_sneaker"` $\rightarrow$ Băm ra ngăn **14000** $\rightarrow$ Tự động bay sang **Máy 3** lưu.

Nhờ cách này:
1. **Dữ liệu được xé nhỏ chia đều** cho 3 máy, không máy nào bị tràn RAM.
2. **Tải truy cập được chia đều**, 3 máy cùng xử lý song song nên chịu được hàng triệu khách hàng.

---

### 4. Kết luận
* **Tại sao code của bạn đã có cú pháp `{...}`?**
  Là vì kiến trúc sư / người viết code Flash Sale trong project này đã **thiết kế chuẩn từ đầu theo chuẩn Production (Scale-ready)**.
* Code này giúp dự án:
  - Khi chạy ở máy tính cá nhân (1 máy đơn) thì vẫn chạy bình thường.
  - Khi sau này đem code này deploy lên hệ thống thật (Redis Cluster gồm 5 - 10 máy chủ), **code sẽ chạy mượt mà ngay lập tức mà không cần phải viết lại hay bị lỗi văng giao dịch**.

# Saga Choreography qua Kafka Topics

``` mermaid
sequenceDiagram
    autonumber
    actor Client as Khách hàng
    participant OrderSvc as Order Service (:8003)<br/>[DB: ecom_order_db]
    participant Kafka as Apache Kafka (:9092)
    participant ProductSvc as Product Service (:8002)<br/>[DB: ecom_product_db]
    participant EmailWorker as Email Worker (:8003)

    Client->>OrderSvc: 1. POST /orders (Đặt hàng)
    Note over OrderSvc: 2. Lưu đơn PENDING vào ecom_order_db
    OrderSvc->>Kafka: 3. Publish topic "order.events"<br/>[Key: order_id, Event: ORDER_CREATED]
    OrderSvc-->>Client: 4. Trả về ngay HTTP 201 Created (status: PENDING)

    par ProductStockWorker (product-service)
        Kafka->>ProductSvc: 5. Nhận ORDER_CREATED
        Note over ProductSvc: 6. Chạy SQL trừ kho trong ecom_product_db
        alt Còn đủ hàng
            ProductSvc->>Kafka: 7a. Publish topic "stock.events"<br/>[Event: STOCK_DEDUCTED_SUCCESS]
            Kafka->>OrderSvc: 8a. OrderSagaWorker nhận event:<br/>Cập nhật đơn PENDING -> CONFIRMED
        else Hết hàng hoặc lỗi
            ProductSvc->>Kafka: 7b. Publish topic "stock.events"<br/>[Event: STOCK_DEDUCTED_FAILED]
            Kafka->>OrderSvc: 8b. Giao dịch bù trừ (Compensating Transaction):<br/>Cập nhật đơn PENDING -> CANCELLED
        end
    and OrderEmailWorker (order-service)
        Kafka->>EmailWorker: 9. Nhận ORDER_CREATED -> Gửi email hóa đơn ngầm
    end
```

