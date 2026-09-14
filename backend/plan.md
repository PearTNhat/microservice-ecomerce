# Kế hoạch triển khai Flash Sale chuẩn Production

## 1. Mục tiêu và invariant

### 1.1. Mục tiêu

- Không bán vượt `allocated_stock` của từng sản phẩm trong từng campaign.
- Giới hạn tổng quantity theo `(campaign_id, product_id, user_id)`.
- Client retry, Kafka replay hoặc worker restart không tạo order, trừ kho hay hoàn kho hai lần.
- Redis xử lý tranh chấp tải cao; PostgreSQL lưu trạng thái bền vững và phục vụ đối soát.
- Kafka tạm ngừng không làm mất request mà API đã trả `202 Accepted`.
- SSE cập nhật gần realtime; status API là nguồn đọc fallback.

### 1.2. Invariant bắt buộc

```text
Redis:
available_stock >= 0
available_stock + reserved_quantity + purchased_quantity = allocated_stock

Theo user:
reserved_by_user + purchased_by_user <= max_quantity_per_user
    (khi max_quantity_per_user > 0)

Database:
allocated_quantity = sold_quantity + released_quantity + remaining_quantity
```

Không dùng tuyên bố “exactly once” hoặc “100% tuyệt đối”. Hệ thống dùng:

```text
Transactional Outbox + Kafka at-least-once + Idempotent Consumer
→ effectively-once ở tầng nghiệp vụ
```

### 1.3. Quyết định thiết kế

1. Mỗi đợt sale là một `campaign_id` mới.
2. Không có chức năng reset campaign đã chạy. Muốn chạy lại thì clone thành campaign mới.
3. Redis deduction đầu tiên là reservation có thời hạn, chưa phải mua thành công.
4. Flash Sale stock tách khỏi stock bán thường.
5. Product Service sở hữu stock gốc và allocation; Order Service sở hữu campaign, reservation và order.
6. Không có distributed database transaction giữa hai service; activation sử dụng Saga và các thao tác idempotent.
7. SSE chỉ thông báo trạng thái, không trừ kho hoặc quyết định người thắng.

---

## 2. Trạng thái nghiệp vụ

### 2.1. Campaign

```text
DRAFT → SCHEDULED
DRAFT/SCHEDULED
  → ALLOCATING
  → PREWARMING
  → ACTIVE
  → ENDING
  → ENDED

ALLOCATING/PREWARMING
  → ACTIVATION_FAILED
  → COMPENSATING
  → DRAFT hoặc CANCELLED
```

Campaign đã `ACTIVE` không được sửa product, giá, quota hoặc allocated stock trực tiếp. Mọi điều chỉnh phải qua use case riêng, có audit và idempotency.

### 2.2. Reservation

```text
RESERVED → PROCESSING → CONFIRMED
RESERVED → CANCELLED
RESERVED → EXPIRED
PROCESSING → CANCELLED
```

`CONFIRMED`, `CANCELLED`, `EXPIRED` là trạng thái cuối.

PostgreSQL quyết định trạng thái cuối bằng conditional update. Redis là projection tốc độ cao và phải được reconciliation nếu đồng bộ thất bại.

---

## 3. Database schema

Không dùng `ON DELETE CASCADE` cho campaign đã có giao dịch. Campaign được soft-delete hoặc chuyển trạng thái; dữ liệu giao dịch phải được giữ để audit.

### 3.1. Order Service — `ecom_order_db`

#### `flash_sale_campaigns`

```sql
CREATE TABLE flash_sale_campaigns (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'DRAFT',
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_campaign_time CHECK (ends_at > starts_at),
    CONSTRAINT chk_campaign_status CHECK (
        status IN (
            'DRAFT', 'SCHEDULED', 'ALLOCATING', 'PREWARMING', 'ACTIVE',
            'ENDING', 'ENDED', 'ACTIVATION_FAILED',
            'COMPENSATING', 'CANCELLED'
        )
    )
);

CREATE INDEX idx_fs_campaign_status_time
ON flash_sale_campaigns(status, starts_at, ends_at);
```

#### `flash_sale_items`

```sql
CREATE TABLE flash_sale_items (
    id BIGSERIAL PRIMARY KEY,
    campaign_id BIGINT NOT NULL REFERENCES flash_sale_campaigns(id),
    product_id BIGINT NOT NULL,
    sale_price NUMERIC(15,2) NOT NULL CHECK (sale_price >= 0),
    original_price NUMERIC(15,2) NOT NULL CHECK (original_price >= 0),
    allocated_stock INT NOT NULL CHECK (allocated_stock > 0),
    reserved_stock INT NOT NULL DEFAULT 0 CHECK (reserved_stock >= 0),
    sold_stock INT NOT NULL DEFAULT 0 CHECK (sold_stock >= 0),
    max_quantity_per_user INT NOT NULL DEFAULT 1
        CHECK (max_quantity_per_user >= 0),
    max_quantity_per_order INT NOT NULL DEFAULT 1
        CHECK (max_quantity_per_order > 0),
    reservation_seconds INT NOT NULL DEFAULT 120
        CHECK (reservation_seconds > 0),
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fs_campaign_product UNIQUE(campaign_id, product_id),
    CONSTRAINT chk_fs_stock_balance
        CHECK (reserved_stock + sold_stock <= allocated_stock)
);
```

`max_quantity_per_user = 0` nghĩa là không giới hạn theo user nhưng vẫn bị giới hạn bởi `max_quantity_per_order` và stock.

#### `flash_sale_reservations`

```sql
CREATE TABLE flash_sale_reservations (
    id VARCHAR(64) PRIMARY KEY,
    request_id VARCHAR(64) NOT NULL,
    request_fingerprint VARCHAR(64) NOT NULL,
    campaign_id BIGINT NOT NULL REFERENCES flash_sale_campaigns(id),
    flash_sale_item_id BIGINT NOT NULL REFERENCES flash_sale_items(id),
    product_id BIGINT NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(15,2) NOT NULL CHECK (unit_price >= 0),
    total_amount NUMERIC(15,2) NOT NULL CHECK (total_amount >= 0),
    payment_method VARCHAR(20) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'RESERVED',
    order_id BIGINT NULL REFERENCES orders(id),
    expires_at TIMESTAMPTZ NOT NULL,
    failure_reason VARCHAR(255),
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fs_idempotency
        UNIQUE(campaign_id, product_id, user_id, request_id),
    CONSTRAINT chk_fs_reservation_status CHECK (
        status IN ('RESERVED', 'PROCESSING', 'CONFIRMED', 'CANCELLED', 'EXPIRED')
    )
);

CREATE INDEX idx_fs_reservation_user
ON flash_sale_reservations(campaign_id, product_id, user_id);

CREATE INDEX idx_fs_reservation_expiry
ON flash_sale_reservations(expires_at)
WHERE status IN ('RESERVED', 'PROCESSING');
```

`request_fingerprint` là SHA-256 của các trường nghiệp vụ đã canonicalize. Cùng Idempotency-Key nhưng fingerprint khác phải trả `409 IDEMPOTENCY_KEY_REUSED`.

#### `outbox_events`

```sql
CREATE TABLE outbox_events (
    id VARCHAR(64) PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    topic VARCHAR(100) NOT NULL,
    partition_key VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    locked_by VARCHAR(100),
    locked_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ,
    CONSTRAINT chk_outbox_status
        CHECK (status IN ('PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED'))
);

CREATE INDEX idx_outbox_pending
ON outbox_events(next_attempt_at)
WHERE status = 'PENDING';

CREATE INDEX idx_outbox_stale_processing
ON outbox_events(locked_until)
WHERE status = 'PROCESSING';
```

#### `processed_events`

```sql
CREATE TABLE processed_events (
    consumer_name VARCHAR(100) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(consumer_name, event_id)
);
```

#### Cập nhật counter item

Mọi thay đổi `reserved_stock/sold_stock` trong DB dùng conditional update hoặc row lock trong cùng transaction với reservation:

```sql
UPDATE flash_sale_items
SET reserved_stock = reserved_stock + :qty,
    version = version + 1
WHERE id = :item_id
  AND reserved_stock + sold_stock + :qty <= allocated_stock;
```

Các counter này là dữ liệu đối soát, không nằm trên hot path tranh chấp trước Redis.

### 3.2. Product Service — `ecom_product_db`

Không chỉ thêm một cột `flash_sale_allocated` vào products. Cần allocation ledger theo campaign:

```sql
CREATE TABLE product_stock_allocations (
    id BIGSERIAL PRIMARY KEY,
    campaign_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL REFERENCES products(id),
    request_id VARCHAR(64) NOT NULL,
    allocated_quantity INT NOT NULL CHECK (allocated_quantity > 0),
    sold_quantity INT NOT NULL DEFAULT 0 CHECK (sold_quantity >= 0),
    released_quantity INT NOT NULL DEFAULT 0 CHECK (released_quantity >= 0),
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_product_campaign_allocation
        UNIQUE(campaign_id, product_id),
    CONSTRAINT uq_product_allocation_request UNIQUE(request_id),
    CONSTRAINT chk_product_allocation_balance
        CHECK (sold_quantity + released_quantity <= allocated_quantity)
);
```

Allocate chạy trong một Product DB transaction:

```sql
UPDATE products
SET stock = stock - :qty
WHERE id = :product_id AND stock >= :qty;

INSERT INTO product_stock_allocations (...);
```

Nếu update không trả row thì allocation thất bại. Retry cùng request ID trả kết quả cũ, không trừ stock lần hai.

Release cũng idempotent và chỉ hoàn:

```text
allocated_quantity - sold_quantity - released_quantity
```

---

## 4. Redis keys và Lua contract

### 4.1. Cluster-safe keys

Các key trong cùng Lua script dùng cùng hash tag:

| Key | Kiểu | Ý nghĩa |
|---|---|---|
| `fs:{c:C:p:P}:stock` | String | Stock có thể reserve |
| `fs:{c:C:p:P}:reserved` | Hash | userID → reserved quantity |
| `fs:{c:C:p:P}:purchased` | Hash | userID → confirmed quantity |
| `fs:{c:C:p:P}:state` | String | ACTIVE/PAUSED/ENDED |
| `fs:{c:C:p:P}:config` | Hash | quota, time, reservation seconds |
| `fs:{c:C:p:P}:req:U:R` | Hash | fingerprint + serialized result |
| `fs:{c:C:p:P}:resv:RID` | Hash | reservation projection |
| `fs:{c:C:p:P}:expiry` | ZSet | reservation expiry theo item |
| `fs:order-status:RID` | String | snapshot cho status/SSE |

Idempotency được scope theo endpoint:

```text
(campaign_id, product_id, user_id, request_id)
```

Scope này phải giống unique constraint trong DB. Client vẫn phải sinh UUID mới cho mỗi thao tác nghiệp vụ mới.

### 4.2. Structured Lua result

Lua luôn trả JSON/array có typed code, không dùng `redis.error_reply` cho lỗi nghiệp vụ:

```json
{"code":"RESERVED","reservation_id":"...","expires_at":123}
{"code":"SOLD_OUT"}
{"code":"LIMIT_EXCEEDED"}
{"code":"CAMPAIGN_NOT_ACTIVE"}
{"code":"IDEMPOTENCY_KEY_REUSED"}
{"code":"INVARIANT_VIOLATION"}
```

Trong các đoạn Lua minh họa bên dưới, `result(code)` là helper serialize typed result; agent phải định nghĩa helper này hoặc trả array có schema tương đương và viết test cho từng code.

Chỉ lỗi kết nối, timeout hoặc Lua runtime mới được coi là infrastructure error.

### 4.3. Reserve script

Script nhận request ID, fingerprint, reservation ID, user ID và quantity. Thời gian và reservation TTL không lấy từ client/application argument:

1. Đọc idempotency key. Nếu tồn tại:
   - fingerprint giống: trả result cũ;
   - fingerprint khác: trả `IDEMPOTENCY_KEY_REUSED`.
2. Lấy thời gian bằng Redis `TIME`.
3. Kiểm tra state ACTIVE và `starts_at <= now < ends_at`.
4. Đọc `max_per_user`, `max_per_order`, `resv_sec` từ config.
5. Validate quantity.
6. Kiểm tra `reserved[user] + purchased[user] + quantity`.
7. Kiểm tra stock.
8. Trừ stock, tăng reserved.
9. Ghi reservation gồm user ID, quantity, status, fingerprint, created/expires time.
10. Thêm reservation vào expiry ZSet.
11. Cache kết quả idempotency.

Cache ngắn cả kết quả ổn định `SOLD_OUT`, `LIMIT_EXCEEDED`, `INVALID_QUANTITY`. Không cache lỗi hạ tầng.

Điều kiện thời gian:

```lua
local redis_time = redis.call('TIME')
local now = tonumber(redis_time[1])
if now < starts_at or now >= ends_at then
    return result('CAMPAIGN_NOT_ACTIVE')
end
```

### 4.4. Confirm script

Caller chỉ cung cấp reservation key/ID; script đọc user và quantity từ reservation hash.

```lua
local status = redis.call('HGET', KEYS[3], 'status')

if status == 'CONFIRMED' then
    return result('ALREADY_CONFIRMED')
end

if status ~= 'RESERVED' and status ~= 'PROCESSING' then
    return result('INVALID_STATE')
end

local user_id = redis.call('HGET', KEYS[3], 'user_id')
local qty = tonumber(redis.call('HGET', KEYS[3], 'quantity'))
if not user_id or not qty or qty <= 0 then
    return result('INVARIANT_VIOLATION')
end

local current = tonumber(redis.call('HGET', KEYS[1], user_id) or '0')
if current < qty then
    return result('INVARIANT_VIOLATION')
end

local remaining = redis.call('HINCRBY', KEYS[1], user_id, -qty)
if remaining == 0 then redis.call('HDEL', KEYS[1], user_id) end

redis.call('HINCRBY', KEYS[2], user_id, qty)
redis.call('HSET', KEYS[3], 'status', 'CONFIRMED')
redis.call('ZREM', KEYS[4], ARGV[1])
return result('CONFIRMED')
```

### 4.5. Release script

Không được cộng stock nếu reservation không tồn tại hoặc không ở trạng thái releasable.

```lua
local status = redis.call('HGET', KEYS[3], 'status')

if status == 'EXPIRED' or status == 'CANCELLED' then
    return result('ALREADY_RELEASED')
end

if status ~= 'RESERVED' and status ~= 'PROCESSING' then
    return result('INVALID_STATE')
end

local user_id = redis.call('HGET', KEYS[3], 'user_id')
local qty = tonumber(redis.call('HGET', KEYS[3], 'quantity'))
if not user_id or not qty or qty <= 0 then
    return result('INVARIANT_VIOLATION')
end

local current = tonumber(redis.call('HGET', KEYS[2], user_id) or '0')
if current < qty then
    return result('INVARIANT_VIOLATION')
end

redis.call('INCRBY', KEYS[1], qty)
local remaining = redis.call('HINCRBY', KEYS[2], user_id, -qty)
if remaining == 0 then redis.call('HDEL', KEYS[2], user_id) end

redis.call('HSET', KEYS[3], 'status', ARGV[2])
redis.call('ZREM', KEYS[4], ARGV[1])
return result('RELEASED')
```

`ARGV[2]` chỉ được Go wrapper map từ enum nội bộ `CANCELLED/EXPIRED`, không nhận trực tiếp từ HTTP.

### 4.6. TTL và rebuild

- Campaign Redis keys: expire sau `ends_at + 7 ngày`.
- Idempotency result: ít nhất `ends_at + 24 giờ`.
- Order status snapshot: 24 giờ hoặc theo retention policy.
- Không đặt TTL riêng khiến stock/config/quota biến mất lệch nhau.
- Có command/job rebuild Redis từ active campaign, DB reservations và confirmed orders.

---

## 5. API contract

### 5.1. Admin

```http
POST /admin/flash-sales
POST /admin/flash-sales/:campaignId/items
POST /admin/flash-sales/:campaignId/activate
POST /admin/flash-sales/:campaignId/end
POST /admin/flash-sales/:campaignId/cancel
POST /admin/flash-sales/:campaignId/clone
GET  /admin/flash-sales/:campaignId
```

Không có endpoint reset. `clone` tạo campaign ID mới và chỉ sao chép cấu hình, không sao chép reservation/order.

### 5.2. Customer

```http
POST /flash-sales/:campaignId/items/:productId/orders
Authorization: Bearer <token>
Idempotency-Key: <uuid>

{
  "quantity": 1,
  "payment_method": "COD",
  "customer_name": "...",
  "customer_email": "...",
  "customer_phone": "...",
  "shipping_address": "..."
}
```

Server tự lấy user ID, giá, campaign timing và quota. Không lấy được config/giá authoritative thì fail closed; không dùng giá fallback.

```http
202 Accepted
{
  "reservation_id": "...",
  "status": "RESERVED",
  "expires_at": "...",
  "status_url": "/flash-sales/orders/:id",
  "stream_url": "/flash-sales/orders/:id/stream"
}
```

```http
GET /flash-sales/orders/:reservationId
GET /flash-sales/orders/:reservationId/stream
```

Cả status và SSE phải kiểm tra reservation thuộc user hiện tại; admin mới được đọc user khác.

Mã lỗi:

| HTTP | Code |
|---|---|
| 409 | CAMPAIGN_NOT_ACTIVE |
| 422 | INVALID_QUANTITY |
| 409 | LIMIT_EXCEEDED |
| 409 | SOLD_OUT |
| 409 | IDEMPOTENCY_KEY_REUSED |
| 404 | RESERVATION_NOT_FOUND |
| 503 | TEMPORARILY_UNAVAILABLE |

---

## 6. Activation Saga và stock ownership

### 6.1. API nội bộ Product Service

```http
POST /internal/stock-allocations
Idempotency-Key: <activation-step-id>

{
  "campaign_id": 10,
  "product_id": 1,
  "quantity": 100
}

POST /internal/stock-allocations/:campaignId/:productId/release
Idempotency-Key: <release-step-id>
```

Hai API phải authenticate service-to-service và idempotent.

### 6.2. Activate

1. Conditional update campaign `DRAFT/SCHEDULED → ALLOCATING`.
2. Gọi Product Service allocate cho từng item bằng stable idempotency key.
3. Nếu tất cả thành công: `ALLOCATING → PREWARMING`.
4. Prewarm Redis từ campaign và allocation đã xác nhận nhưng để item state là `PAUSED`.
5. Verify Redis stock/config projection.
6. Conditional update campaign `PREWARMING → ACTIVE` trong DB.
7. Chuyển Redis item state `PAUSED → ACTIVE`. Nếu crash ở đây, campaign chỉ tạm thời chưa nhận order; activation retry tiếp tục mở Redis mà không allocate lại.
8. Nếu lỗi trước DB ACTIVE, retry an toàn. Nếu không thể hoàn tất, chuyển `ACTIVATION_FAILED → COMPENSATING`, release các allocation đã thành công, sau đó `DRAFT/CANCELLED`.

Không trả success cho admin trước khi campaign DB và Redis ở trạng thái nhất quán đủ để phục vụ.

### 6.3. End/cancel

1. Chuyển campaign `ACTIVE → ENDING`; Redis state thành `ENDED` để chặn reserve mới.
2. Chờ hoặc xử lý reservation đang mở theo policy.
3. Tính unsold stock từ DB allocation và confirmed order.
4. Gọi Product Service release idempotently.
5. Reconcile rồi chuyển `ENDED`.

ProductStockWorker khi nhận `ORDER_CREATED`:

- Order thường: trừ stock theo flow hiện tại.
- Order Flash Sale: không trừ stock thường lần nữa vì đã allocate.
- Vẫn ghi processed event trong transaction; không được `return` trước bước idempotency/ack cần thiết.
- Khi Flash Sale order được CONFIRMED, consume event `FLASH_SALE_ORDER_CONFIRMED` để tăng `product_stock_allocations.sold_quantity` bằng conditional update trong cùng transaction với `processed_events`.
- Replay event confirm không được tăng `sold_quantity` lần hai.

Event phải có `is_flash_sale`, `campaign_id`, `allocation_id`, `reservation_id`.

---

## 7. Luồng mua và transaction boundary

### 7.1. Reserve

1. Validate JWT, ownership và request DTO.
2. Canonicalize request và tính fingerprint.
3. Gọi Redis reserve Lua.
4. Nếu reserve thành công, mở Order DB transaction:
   - insert reservation;
   - tăng DB `reserved_stock` bằng conditional update;
   - insert outbox `FLASH_SALE_RESERVED`;
   - commit.
5. Nếu DB transaction thất bại, gọi Redis release idempotently.
6. Chỉ trả `202` sau DB commit.

Nếu insert gặp duplicate idempotency constraint do Redis idempotency cache đã hết hoặc bị mất:

1. Đọc reservation hiện có bằng `(campaign, product, user, request_id)`.
2. So sánh fingerprint.
3. Release reservation Redis vừa tạo nếu nó khác reservation canonical.
4. Cùng fingerprint thì trả kết quả canonical; khác fingerprint thì trả `IDEMPOTENCY_KEY_REUSED`.

Crash giữa bước Redis reserve và DB commit tạo ghost reservation. Ghost cleanup xử lý bằng reservation ID trong Redis expiry ZSet.

### 7.2. FlashSaleWorker

Trong một Order DB transaction:

1. Insert `processed_events(consumer_name,event_id)`.
2. Nếu duplicate key: đọc trạng thái canonical rồi ack; không tạo order lại.
3. Conditional update reservation `RESERVED → PROCESSING`.
4. Tạo Order và OrderItem.
5. Conditional update reservation `PROCESSING → CONFIRMED`.
6. Giảm item `reserved_stock`, tăng `sold_stock`.
7. Insert outbox `ORDER_CREATED`.
8. Insert outbox/internal task `FLASH_SALE_REDIS_CONFIRM_REQUIRED`.
9. Commit.

Redis confirm và SSE notification thực hiện sau DB commit. Nếu Redis lỗi, task/outbox vẫn PENDING để retry; duplicate Kafka event không được làm mất bước đồng bộ Redis còn thiếu.

Với online payment, bước tạo order có thể chuyển reservation sang `PROCESSING`; chỉ webhook thanh toán hợp lệ mới CAS sang `CONFIRMED`.

### 7.3. Expiration và race với confirm

Expiry worker claim theo batch:

```sql
UPDATE flash_sale_reservations
SET status = 'EXPIRED',
    version = version + 1,
    updated_at = NOW()
WHERE id IN (
    SELECT id
    FROM flash_sale_reservations
    WHERE status IN ('RESERVED', 'PROCESSING')
      AND expires_at <= NOW()
    FOR UPDATE SKIP LOCKED
    LIMIT :batch_size
)
RETURNING *;
```

Trong cùng transaction:

- giảm DB `reserved_stock`;
- insert outbox/internal task `FLASH_SALE_REDIS_RELEASE_REQUIRED`.

Confirm cũng dùng CAS:

```sql
UPDATE flash_sale_reservations
SET status = 'CONFIRMED'
WHERE id = :id
  AND status IN ('RESERVED', 'PROCESSING')
  AND expires_at > NOW()
RETURNING *;
```

Chỉ một trong confirm/expire lấy được row. Bên không lấy được row phải đọc trạng thái cuối và xử lý theo policy, không tự sửa Redis.

### 7.4. Late payment webhook

- Webhook idempotent theo provider transaction ID.
- Nếu DB reservation đã EXPIRED/CANCELLED, không confirm lại.
- Ghi payment thành `PENDING_REFUND` và thực hiện refund qua outbox/worker riêng.
- Không gọi trực tiếp refund bên ngoài bên trong DB transaction.

---

## 8. Outbox và consumer idempotency

### 8.1. Outbox publisher

- Trong transaction ngắn, claim batch bằng `FOR UPDATE SKIP LOCKED`, chuyển sang `PROCESSING`, gán `locked_by/locked_until`, rồi commit.
- Publish Kafka ngoài DB transaction để không giữ lock trong lúc chờ network.
- Publish xong cập nhật `PUBLISHED`; lỗi thì trả `PENDING`, tăng attempts và đặt `next_attempt_at`.
- Worker khác thu hồi row `PROCESSING` có `locked_until < NOW()` khi publisher crash.
- Retry exponential backoff; quá ngưỡng chuyển FAILED/DLT và alert.
- Publish thành công nhưng crash trước mark PUBLISHED có thể phát trùng; đây là hành vi dự kiến.

### 8.2. Event envelope

```json
{
  "event_id": "uuid",
  "event_type": "FLASH_SALE_RESERVED",
  "aggregate_id": "reservation-id",
  "occurred_at": "RFC3339",
  "trace_id": "...",
  "schema_version": 1,
  "payload": {}
}
```

Consumer ghi business change và `processed_events` trong cùng DB transaction. Chỉ commit Kafka offset sau khi transaction thành công hoặc duplicate đã được xử lý an toàn.

---

## 9. Ghost cleanup và reconciliation

### 9.1. Ghost reservation

Worker duyệt từng expiry ZSet của campaign ACTIVE/ENDING:

1. Lấy reservation Redis đã cũ hơn grace period.
2. Query DB theo reservation ID.
3. Không có DB row: gọi release Lua.
4. Có DB row: đồng bộ Redis theo DB state, không phán đoán chỉ từ tuổi key.

Không release reservation chỉ vì “created hơn 60 giây”; phải xét `expires_at` hoặc xác nhận DB row không tồn tại.

### 9.2. Reconciliation

Đối chiếu định kỳ:

```text
DB allocation
DB item reserved/sold
DB reservation theo status
DB confirmed orders
Redis available/reserved/purchased
```

Mismatch tạo metric và audit record. Auto-repair chỉ áp dụng rule xác định rõ; trường hợp không chắc chắn phải alert để xử lý thủ công.

---

## 10. SSE và status fallback

```http
GET /flash-sales/orders/:reservationId/stream
Accept: text/event-stream
```

Native `EventSource` không đặt được Authorization header tùy ý. Chọn một trong:

1. Cookie HttpOnly/SameSite phù hợp; hoặc
2. API tạo SSE ticket dùng một lần, TTL ngắn.

Không đưa access token dài hạn vào URL.

SSE handler:

- authorize reservation ownership;
- subscribe notification trước rồi đọc/push snapshot canonical để giảm race;
- mỗi event có ID/version;
- heartbeat 15–30 giây;
- cleanup subscription khi disconnect;
- đóng khi CONFIRMED/CANCELLED/EXPIRED;
- giới hạn connection/user và max connection time;
- `Cache-Control: no-cache`, `X-Accel-Buffering: no`;
- gateway/read timeout phù hợp.

Redis Pub/Sub có thể mất message. Snapshot DB/Redis status và status API mới là cơ chế phục hồi.

Frontend:

- mở SSE sau response 202;
- dùng native reconnect/`Last-Event-ID` nếu hỗ trợ;
- chỉ fallback polling khi SSE lỗi, reconnect thất bại hoặc vượt timeout hợp lý;
- polling exponential backoff và dừng ở trạng thái cuối;
- reload trang đọc status trước rồi mở SSE nếu chưa kết thúc.

---

## 11. Thay đổi theo module

### Order Service

- `internal/domain/flash_sale.go`: campaign/item/reservation/status.
- `internal/domain/outbox.go`: outbox và processed event.
- `internal/dto/flash_sale_dto.go`: DTO riêng.
- `internal/repository/flash_sale_repository.go`: transaction và CAS.
- `internal/repository/outbox_repository.go`: claim/retry/mark.
- `internal/service/flash_sale_service.go`: activate/reserve/confirm/release/status.
- `internal/delivery/http/flash_sale_handler.go`: admin/customer/status/SSE.
- `internal/worker/outbox_publisher_worker.go`.
- `internal/worker/flash_sale_worker.go`: flow idempotent mới.
- `internal/worker/reservation_expiry_worker.go`.
- `internal/worker/reconciliation_worker.go`.
- `cmd/main.go`: migration, DI và worker lifecycle.

### Product Service

- `internal/domain/stock_allocation.go`.
- `internal/repository/stock_allocation_repository.go`.
- internal allocate/release handlers có service authentication.
- sửa ProductStockWorker cho event Flash Sale mà không double deduction.
- consumer `FLASH_SALE_ORDER_CONFIRMED` cập nhật allocation sold counter idempotently.

### Shared

- `pkg/redislock/flash_sale_engine.go`: key builder, typed result, Lua scripts.
- `pkg/kafka/events.go`: versioned event envelope.
- `pkg/kafka/topics.go`: topic, group và partition-key contract.

### Code cũ phải loại bỏ

- `flash_sale:users:{product_id}`.
- Flash Sale dùng `product:stock:{product_id}`.
- `PrewarmStock` xóa lịch sử user.
- Giá fallback `1000000`.
- Reset campaign.
- ProductStockWorker trừ stock lần hai cho Flash Sale.

---

## 12. Thứ tự triển khai

Mỗi phase phải test xanh trước phase sau.

### Phase 1 — Domain, migrations, repositories

- Thêm schema hai service và constraints.
- Repository transaction/CAS/idempotent allocation.
- Unit test duplicate, rollback và concurrent CAS.

**Done:** DB từ chối duplicate; allocation retry không trừ hai lần; rollback không để state nửa vời.

### Phase 2 — Redis reservation engine

- Key builder cluster-safe.
- Reserve/confirm/release Lua với structured result.
- TTL và rebuild command.
- Concurrency/invariant tests.

**Done:** không âm stock/vượt quota; reservation thiếu không thể tăng stock; confirm/release retry không đổi counter lần hai.

### Phase 3 — Activation Saga và Admin API

- Create/item/activate/end/cancel/clone.
- Product allocation API idempotent.
- Compensation khi activate thất bại.

**Done:** crash ở mỗi bước đều retry/compensate được; không có allocation mồ côi.

### Phase 4 — Customer API và durable acceptance

- Reserve API với fingerprint.
- Reservation + item counter + outbox transaction.
- Status API và ownership authorization.

**Done:** không giả giá/user, không reuse key với body khác, không trả 202 trước durable commit.

### Phase 5 — Kafka/Outbox

- Publisher claim/retry/lease.
- Idempotent FlashSaleWorker.
- Redis confirmation task retry.
- ProductStockWorker skip deduction đúng contract.

**Done:** Kafka down phục hồi được; replay không tạo order; Redis lỗi sau DB commit vẫn được đồng bộ lại.

### Phase 6 — Expiry, payment và reconciliation

- DB CAS confirm/expire.
- Redis release/confirm retry task.
- Ghost cleanup và reconciliation.
- Late webhook/refund outbox nếu payment online nằm trong scope.

**Done:** confirm–expire race chỉ có một kết quả cuối; không double refund/release.

### Phase 7 — SSE và frontend

- Ticket/cookie auth.
- Snapshot, version, heartbeat, cleanup.
- Polling fallback.

**Done:** mất Pub/Sub/SSE/reload vẫn đọc đúng trạng thái canonical.

### Phase 8 — Load test và rollout

- Feature flag cho flow mới.
- Dual-read/metrics trong giai đoạn chuyển đổi nếu cần.
- Chỉ xóa endpoint cũ sau soak period.

---

## 13. Test matrix bắt buộc

| Kịch bản | Kỳ vọng |
|---|---|
| 1.000 user tranh 100 stock | Tổng quantity reserve đúng 100, stock không âm |
| Một user gửi 50 request khác key, max=1 | Tổng quantity user không vượt 1 |
| Cùng key + cùng body | Cùng reservation/result |
| Cùng key + body khác | IDEMPOTENCY_KEY_REUSED |
| Release reservation không tồn tại | Stock không thay đổi |
| Confirm/release truyền dữ liệu giả | Script dùng dữ liệu reservation, counter không lệch |
| Confirm đồng thời expire | Chỉ một DB CAS thắng |
| Kafka down | Request đã 202 nằm trong outbox và publish khi phục hồi |
| Event Kafka phát 3 lần | Một order, một business transition |
| Crash sau DB commit trước Redis confirm | Retry/reconciliation confirm Redis |
| Crash sau Redis reserve trước DB | Ghost cleanup release đúng một lần |
| Activate lỗi sau một phần allocation | Compensation release đúng allocation đã tạo |
| Release allocation gọi lặp | Product stock chỉ tăng một lần |
| Campaign mới cùng product | User được mua lại |
| User A đọc status/SSE user B | 403/404 |
| Pub/Sub mất event | Snapshot/status trả kết quả cuối |
| Không lấy được giá authoritative | Fail closed |

`test_flash_sale.sh` phải tạo/login nhiều user để có JWT khác nhau; đổi email/name trong body không tạo user khác.

Chạy:

```bash
make test
./test_e2e_order.sh
./test_flash_sale.sh
```

Load test báo throughput, p50/p95/p99, error rate theo code, DB/Redis counters và reconciliation mismatch.

---

## 14. Observability và nghiệm thu

Metrics tối thiểu:

- `flash_sale_reserve_total{result}`
- `flash_sale_reserve_duration_seconds`
- `flash_sale_active_reservations`
- `flash_sale_expired_total`
- `outbox_pending_total`
- `outbox_publish_failures_total`
- `consumer_duplicate_events_total`
- `sse_active_connections`
- `sse_delivery_failures_total`
- `flash_sale_reconciliation_mismatch_total`

Không gắn user ID vào metric label. Log có trace ID, event ID, request ID, reservation ID, campaign ID, product ID và user ID; không log JWT hoặc thông tin thanh toán nhạy cảm.

Chỉ nghiệm thu khi:

1. Tất cả invariant giữ đúng trong concurrency/failure test.
2. Kafka/Redis/worker restart không làm mất request đã 202 hoặc xử lý hai lần.
3. DB đủ dữ liệu để rebuild Redis.
4. Activation/end saga retry và compensate được.
5. SSE có auth, snapshot, heartbeat, cleanup và polling fallback.
6. Campaign mới không cần xóa lịch sử campaign cũ.
7. Không còn giá fallback và double stock deduction.

## 15. Cấu hình mặc định bản đầu

```text
max_quantity_per_user = 1
max_quantity_per_order = 1
reservation_seconds(COD) = 120
reservation_seconds(online payment) = 600
idempotency retention = campaign end + 24h
Redis audit retention = campaign end + 7d
SSE heartbeat = 20s
```

Các giá trị phải nằm trong campaign/item config, không hard-code trong Lua hoặc HTTP message.
