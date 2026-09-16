# Flash Sale Correctness Implementation Plan

## 1. Objective

Close the confirmed correctness and crash-recovery gaps in the Flash Sale flow while preserving Database-per-Service:

- Order Service owns campaigns, reservations, orders, its outbox, and the Redis Flash Sale projection.
- Product Service owns regular product stock and `product_stock_allocations`.
- Kafka provides at-least-once delivery; each consumer provides effectively-once business effects.
- Redis remains a fast, rebuildable projection. PostgreSQL remains the durable source of truth.

This plan does not introduce distributed transactions or a shared database.

### Baseline at review time (2026-09-16)

The standalone Flash Sale path already has a confirmation Outbox event, a Product Service `sold_quantity` consumer, an Order Redis projection consumer, retry-aware Kafka acknowledgement, COD-only validation, and a row lock in Product Service release. Sections 4–11 describe target properties and remaining tests/fixes; they are not all new features to build. The new scope is mixed-cart checkout, offer resolution, item-level inventory routing, and recovery of a partially completed mixed order. Keep standalone Flash Sale behavior working while the mixed path is introduced.

## 2. Required invariants

The implementation is complete only while these invariants hold:

```text
Product DB allocation:
allocated_quantity = sold_quantity + released_quantity + remaining_quantity

Order DB Flash Sale item:
allocated_stock >= reserved_stock + sold_stock

Campaign activation:
regular product stock decreases exactly once for each allocation

Campaign end:
regular stock increases by allocated - sold - already_released

Event processing:
the same event_id may be delivered many times but changes business data once
```

Example:

```text
Initial regular stock: 100
Allocate to Flash Sale: 30  -> regular stock = 70
Sell through Flash Sale: 5  -> allocation sold = 5
End campaign: release 25    -> regular stock = 95
```

## 3. Target event flow

Use a dedicated durable event after a Flash Sale order is confirmed:

```text
Customer reserve request
  -> Redis Lua reserves stock
  -> Order DB transaction writes reservation + FLASH_SALE_RESERVED outbox
  -> Outbox publishes FLASH_SALE_RESERVED
  -> FlashSaleWorker creates order
  -> Order DB transaction:
       create order
       confirm reservation
       reserved_stock -> sold_stock
       insert FLASH_SALE_ORDER_CONFIRMED outbox
  -> Outbox publishes FLASH_SALE_ORDER_CONFIRMED
       |-> Product consumer group: increment allocation.sold_quantity
       |-> Order Redis-projector group: confirm Redis reservation and notify SSE
```

Both consumers receive the event independently. Product Service never accesses Order DB or Order Redis directly.

## 3.1. Canonical offer routing — consistent price on every entry point

### Business rule selected for this project

Use a **promotion-aware cart and checkout** policy for every product allocated to an active campaign:

```text
Flash Sale allocation still available and customer is eligible
  -> show Flash Sale price everywhere
  -> cart line is quoted at Flash Sale price
  -> checkout reserves the Flash Sale allocation before creating the order

Flash Sale allocation sold out, campaign inactive, or customer is not eligible
  -> show the current regular offer
  -> checkout requires acceptance if this differs from a previously shown sale quote
```

The allocation may still be physically split, for example `30` discounted units and `70` regular units. The `70` regular units are the fallback inventory after the discounted allocation is unavailable; they must not silently be offered at a different price to the same eligible customer while the Flash Sale offer is still available.

An alternative business rule is to show two explicit choices (for example “Buy 1 Flash Sale unit at 199k” and “Buy regular at 299k”). Do not implement two simultaneous prices unless the UI clearly labels the distinction and the business explicitly approves it.

### Backend: authoritative offer-resolution APIs (Single & Batch)

Add public read endpoints owned by Order Service because Order Service owns campaign state:

1. **Single Offer**:
```http
GET /flash-sales/offers/:productId
```

2. **Batch Offer (Crucial for Homepage, Search Results, Category Listings & Cart Revalidation)**:
```http
POST /flash-sales/offers/batch
```

Request:
```json
{
  "product_ids": [1, 2, 45, 102]
}
```

Response:
```json
{
  "offers": {
    "1": {
      "product_id": 1,
      "purchase_mode": "FLASH_SALE",
      "effective_price": 199000,
      "regular_price": 299000,
      "campaign_id": 10,
      "sale_price": 199000,
      "remaining_display": 12,
      "max_quantity_per_user": 1,
      "ends_at": "2026-09-15T14:00:00Z"
    },
    "2": {
      "product_id": 2,
      "purchase_mode": "REGULAR",
      "effective_price": 450000,
      "regular_price": 450000
    }
  }
}
```

If no usable offer exists, return `purchase_mode: "REGULAR"`. `remaining_display` is advisory UI data only; the Redis Lua reservation remains the final authority for stock and quota.

**Redis offer index**: Order Service may maintain `product_id -> campaign_id, sale_price, starts_at, ends_at` for active campaigns and read many IDs with `HMGET`. Activation and ending must update or invalidate this index; after Redis loss it must be rebuilt from Order DB before serving sale prices. Resolve remaining quantity from the campaign stock projection. For authenticated users, include quota eligibility or return it as unknown until checkout. Cap and validate batch size (for example, 100 IDs). Load tests determine latency targets; the batch endpoint removes per-card HTTP requests but still performs work proportional to the number of IDs.

### Dual purchase modes: Fast-Path 1-Tap "Buy Now" vs Mixed Cart "Add to Cart"

To balance high-velocity viral deals with multi-item e-commerce browsing:
1. **Fast-Path "Buy Now" (Direct Modal)**:
   - For viral "Deal sốc" (e.g. 1k, 9k deals, iPhone flash sales with extreme scarcity).
   - Skips cart navigation: opens the existing direct COD purchase modal. The HTTP request submits the reservation; SSE reports the later result. Measure latency under load before setting an SLA.
2. **Mixed Cart "Add to Cart"**:
   - For standard multi-item shopping: adds the item to the cart with Flash Sale metadata.
   - Allows users to check out Flash Sale items alongside normal catalog products in one unified basket.

### Cart semantics & Anti-Hoarding policy (Strict Stock Reservation Deferral)

The cart is **not** a stock reservation. It represents only customer intent:
```text
product_id + requested quantity
```

**Anti-Hoarding Rule (Vô hiệu hóa tình trạng găm hàng ảo)**:
- **Never reserve stock when an item is added to the cart.**
- If adding to cart reserved stock, malicious actors or casual browsers could add scarce Flash Sale items to their carts and abandon them, locking out legitimate buyers ("Phantom Inventory Hoarding").
- **Stock reservation occurs strictly at Checkout Submission** via Redis Lua script.
- The cart displays the latest quoted sale price, but clearly alerts the customer: *"Giá ưu đãi và số lượng chỉ được đảm bảo khi hoàn tất đặt hàng"*.

### Backend: enforce pricing, do not trust frontend prices

The checkout endpoints (`POST /orders/checkout`, `POST /orders/direct`) must resolve each line's effective offer on the server before creating an order:
```text
Product has a usable Flash Sale offer for this customer
  -> reserve against the Flash Sale allocation in Redis
  -> create an order line with sale price, campaign_id, and reservation_id

No usable Flash Sale offer (expired or sold out)
  -> compare with the customer's accepted quote
  -> if price rose, return 409 with a new quote; do not create an order yet
  -> after explicit acceptance, use the regular inventory path
```

Client-submitted prices are strictly ignored.

### Mixed cart checkout conflict UX & Re-quote protocol

In a mixed cart (e.g. 1 Flash Sale item + 2 regular items), the Flash Sale item may sell out between the time it was viewed in the cart and the moment the user clicks "Place Order".

When Redis Lua reservation fails during checkout:
1. The server must **never** silently convert the Flash Sale item to regular price and charge a higher amount.
2. The checkout endpoint returns HTTP `409 Conflict`:
```json
{
  "code": "FLASH_SALE_OUT_OF_STOCK",
  "message": "Sản phẩm Flash Sale đã hết suất ưu đãi hoặc thay đổi giá",
  "affected_items": [
    {
      "product_id": 1,
      "product_name": "Tai nghe không dây XYZ",
      "quoted_price": 199000,
      "new_regular_price": 299000,
      "reason": "SOLD_OUT"
    }
  ]
}
```
3. Return a new server-calculated quote and short-lived quote/version token for the whole basket. Bind the token to user ID, basket fingerprint, prices, and expiry using a server signature or durable server-side quote record. The frontend presents two explicit actions; neither submits an order until the customer confirms:
   - **Accept Regular Price**: Accept the updated total and submit a new checkout attempt using the quote token.
   - **Remove Item**: Show the new basket and total, then submit only after confirmation.
4. Revalidate the accepted quote at submission. If it changes again, return another `409`. Use an idempotency key per logical checkout attempt and a new key for a changed basket or accepted price.

### Payment method scope

The first mixed-cart release supports **COD only**, matching the current Flash Sale reservation endpoint. Reject online payment when the basket contains a Flash Sale item. Regular-only checkout keeps its existing payment choices. Online mixed-cart payment is a separate later phase requiring a verified webhook, a durable payment deadline, idempotent payment/refund records, and normal-stock hold/release semantics. A Redis key TTL alone cannot perform compensation or prove whether a payment succeeded.

### COD checkout Saga for a mixed cart

The existing regular order flow deducts Product DB stock asynchronously after order creation; it has no synchronous regular-stock reservation API. Keep that behavior explicit and leave the mixed order `PENDING` until both inventory paths settle:

```text
1. Validate the accepted server quote and an order-level idempotency key.
2. Reserve each Flash Sale line in Redis using a distinct deterministic line key
   derived from checkout request ID + product/campaign ID. Compensate earlier
   reservations if a later Flash Sale line fails.
3. In one Order DB transaction, create one PENDING mixed order with immutable
   item-level price/source metadata, durable reservation links, and an Outbox
   event requesting deduction of regular lines. Return 202 with order/status ID.
4. Product Service processes only regular lines, records a durable/idempotent
   success or failure result, and compensates partial regular deductions.
5. On regular-stock success, Order Service verifies Flash Sale reservations
   are still valid, then atomically confirms the order and those reservations
   in Order DB and writes one confirmation Outbox event per Flash Sale line
   plus one general ORDER_CREATED event. Redis projection follows the existing
   inline attempt + durable retry path.
6. If regular stock fails or a Flash Sale reservation expires first, cancel
   the mixed order, release Flash Sale reservations, and compensate any regular
   deduction. Persist each unfinished compensation for retry after crashes.
```

Use a dedicated stock-request/result event for this mixed-order Saga. Do not publish the general `ORDER_CREATED` before the order succeeds: the current email worker treats it as a completed purchase. A regular-only order may keep its current route until that flow is migrated separately. Each compensation and consumer action needs its own stable idempotency key. Across Redis keys for different products, reservation is sequential with compensation; a single Lua script is not atomic across Redis Cluster hash slots.

The reservation timeout must cover the expected Product stock response. If the stock result arrives after expiry, reject confirmation and compensate the regular deduction. A background worker resumes `PENDING` orders and incomplete compensations after a crash; the API request does not wait for the entire Saga.

### Item-level, not order-level, Flash Sale classification

A mixed order cannot use one order-wide `IsFlashSale` flag. For example:

```text
Order #500:
  Line A: Product A, Flash Sale, 199k
  Line B: Product B, regular,    500k
```

If `OrderCreatedPayload.IsFlashSale=true` for the entire order, `ProductStockWorker` would skip normal stock deduction for Line B. If `false`, it would deduct regular stock for Line A a second time. Both are wrong.

Move the fields to each order item/event item:

```text
is_flash_sale
campaign_id
reservation_id
effective_price
```

Product Service must then process every line independently:

```text
Flash Sale confirmation event -> increment allocation.sold_quantity;
                                 do not decrement products.stock
Mixed-order regular-stock request -> decrement only regular lines;
                                     emit a stock result
```

Do not send both a dedicated Flash Sale confirmation event and an `ORDER_CREATED` event through two inventory deduction paths. The `ORDER_CREATED` consumer must skip Flash Sale lines and must not deduct regular lines already handled by the mixed-order stock request. Keep email/analytics consumption of the general event.

### Frontend changes

Apply the same Offer API to product detail, product cards/search, home page, and cart revalidation:

```text
Product listing / search / home loads:
  -> POST /flash-sales/offers/batch with array of product IDs
  -> Enrich product cards with sale prices, countdown timers, and badges

Product detail page loads:
  -> GET /flash-sales/offers/:productId
  -> FLASH_SALE: show sale price/countdown/quota, with "Mua ngay" and "Thêm vào giỏ"
  -> REGULAR: show regular price and standard "Thêm vào giỏ"

Cart Drawer / Cart Page loads:
  -> POST /flash-sales/offers/batch with current cart item IDs
  -> Re-validate displayed prices before checkout button is clicked
```

For `FLASH_SALE` mode, add the item to the normal cart with a visible Flash Sale badge, sale price, campaign end time, and per-user limit. The cart must clearly say that final availability and price are confirmed at checkout.

For `REGULAR` mode, add the item normally.

At checkout, send product IDs, requested quantities, the accepted server quote/version token, COD payment choice, and an idempotency key. Never send a client price as authority. Backend validates/re-quotes all lines. If Flash Sale became active after an item entered the cart, show its new offer. If a Flash Sale expired or sold out, show the regular-price re-quote and require customer confirmation.

### Acceptance tests

- The same eligible user sees the same effective price and purchase mode on homepage, search, product detail, and cart.
- Product grid loads 50 items using a single batch offer request without N+1 HTTP calls.
- Adding a Flash Sale item to the cart does not decrement Redis or PostgreSQL stock.
- A mixed cart creates one order with correct Flash Sale and regular item metadata.
- Normal checkout directly resolves the Flash Sale offer on the server; client-supplied price is ignored.
- When Flash Sale stock sells out mid-checkout, server returns 409 Conflict with clear options (buy at regular price or drop item).
- Mixed carts containing a Flash Sale item reject non-COD payment in the first release.
- Regular-stock failure or delayed success after Flash Sale reservation expiry cancels the mixed order and compensates completed inventory steps.
- After Flash Sale stock is sold out, every entry point exposes the normal purchase path.
- A stale frontend display cannot oversell because Redis Lua remains authoritative.

## 4. Phase 1 — Define a stable event contract

### Changes

Add an event envelope or at minimum add these fields to the confirmation payload:

```json
{
  "event_id": "uuid",
  "event_type": "FLASH_SALE_ORDER_CONFIRMED",
  "occurred_at": "2026-09-14T10:00:00Z",
  "trace_id": "trace-id",
  "order_id": 501,
  "reservation_id": "FSR-123",
  "campaign_id": 10,
  "product_id": 1,
  "quantity": 1
}
```

The standalone confirmation contract and constant already exist in `pkg/kafka`. Extend them only where mixed line identity is required and add the dedicated mixed regular-stock request/result contract.

### Rules

- `event_id` is generated once before the Order DB transaction.
- The outbox row ID and payload `event_id` must be the same value.
- Apply the same rule to the existing `FLASH_SALE_RESERVED` event: its payload must carry the ID of the Outbox row that produced it. `FlashSaleWorker` must use that producer-created ID as `inputEventID` instead of synthesizing `fs-order-{reservationID}`.
- The partition key should be `campaign_id:product_id` so sale updates for one allocation remain ordered.
- Do not use `reservation_id` synthesized by a consumer as the only event identity.
- Keep `ORDER_CREATED` for general downstream consumers if needed, but Product Service must update Flash Sale allocations only from `FLASH_SALE_ORDER_CONFIRMED`.
  - **Rationale**: Downstream workers (specifically `order_email_worker.go`) listen to `order.events` for `ORDER_CREATED` to send order confirmation emails. Replacing `ORDER_CREATED` entirely would break customer email notifications and third-party webhooks. Dual-publishing via Outbox (`FLASH_SALE_ORDER_CONFIRMED` for inventory allocation + `ORDER_CREATED` with `IsFlashSale=true` for downstream consumers) guarantees durable publication of both business facts without breaking existing services.
  - This guarantees event publication, not successful email delivery. `OrderEmailWorker` must separately retry SMTP failures instead of ignoring `sendInvoiceEmail` errors and committing the Kafka offset.

### Acceptance tests

- JSON round-trip preserves every required field.
- Validation rejects zero campaign/product/order IDs, empty event/reservation IDs, and quantity `<= 0`.
- `FLASH_SALE_RESERVED.event_id` equals its Outbox row ID and is consumed unchanged by `FlashSaleWorker`.

## 5. Phase 2 — Make order confirmation and publication atomic

### Current problem

The standalone Flash Sale worker already writes confirmation and general order events into Outbox. Preserve that transaction boundary when adapting it to mixed orders with multiple Flash Sale lines; write one allocation confirmation event per line and one general order event only after the mixed order succeeds.

### Changes

Inside the existing Order DB transaction in `flash_sale_worker.go`:

1. Insert the processed input event marker.
2. Create `orders` and `order_items`.
3. CAS the reservation from `RESERVED/PROCESSING` to `CONFIRMED`.
4. Move Order DB item quantity from `reserved_stock` to `sold_stock`.
5. Insert one `FLASH_SALE_ORDER_CONFIRMED` outbox row per confirmed Flash Sale line for allocation settlement and Redis projection.
6. Insert an `ORDER_CREATED` outbox row with `IsFlashSale=true` for email, analytics, and other general order consumers.
7. Commit once.

Add a repository method similar to:

```go
ConfirmReservationAndCreateOrder(
    tx *gorm.DB,
    inputEventID string,
    order *domain.Order,
    reservationID string,
    outboxEvents []*domain.OutboxEvent,
) error
```

The existing generic Outbox publisher then publishes the confirmation event. Remove direct best-effort publication as the correctness mechanism.

- **Hybrid Confirmation Strategy (Dual-Path)**:
  - **Fast-path (inline best-effort)**: `FlashSaleWorker` retains an immediate synchronous call with a strict short timeout to `ConfirmFlashSaleReservation` + Redis status snapshot + SSE Pub/Sub directly after DB commit. Do not use an untracked goroutine.
  - **Guaranteed-path (async safety net)**: The Kafka consumer for `FLASH_SALE_ORDER_CONFIRMED` (Phase 5) verifies and completes Redis confirmation idempotently.
  - **Rationale**: The inline attempt minimizes user-visible SSE/polling latency, while the asynchronous consumer provides crash recovery. Do not claim a fixed sub-50ms or sub-100ms SLA until it is verified by load tests. Because Lua `ConfirmFlashSaleReservation` is idempotent (`ALREADY_CONFIRMED` returns success), duplicate execution is safe.

### Acceptance tests

- Failure while creating the order leaves no processed marker, no order, no confirmation, and no confirmation outbox.
- Failure while inserting the outbox rolls back the order and confirmation.
- A committed order always has exactly one confirmation outbox record and exactly one general `ORDER_CREATED` outbox record.
- Reprocessing the same input event creates no second order.

## 6. Phase 3 — Fix Kafka acknowledgement semantics

### Current problem

The standalone worker already has success/terminal/retryable results. Reuse the same acknowledgement rule for new mixed-order stock-result and compensation consumers; verify that malformed events are committed only after successful dead-letter publication.

### Changes

Make the processor return an explicit result:

```go
type ProcessingResult int

const (
    ProcessingSucceeded ProcessingResult = iota
    ProcessingTerminal
    ProcessingRetryable
)

func (w *FlashSaleWorker) processFlashSaleOrder(
    ctx context.Context,
    message kafka.Message,
) (ProcessingResult, error)
```

Commit the Kafka offset only for:

- `ProcessingSucceeded`;
- a duplicate already processed successfully;
- an intentional terminal business state such as an already expired reservation;
- a poison message successfully written to the dead-letter topic.

Do not commit for transient DB, Redis, network, or dependency failures. Apply bounded exponential backoff to avoid a hot retry loop.

- **Do NOT release Redis reservation on transient DB errors**:
  - **Review check**: Ensure both standalone and mixed workers distinguish transient DB errors from terminal business rejection.
  - **Rule**: Release or cancel only after a confirmed terminal outcome. Retry a transient DB error without committing the Kafka offset.
  - **Rationale**: Temporary DB pressure must not forfeit a valid customer reservation.

Validate payloads before indexing or slicing `reservation_id`. This removes the short-ID panic risk.

### Acceptance tests

- A transient database failure does not commit the Kafka offset.
- A retry after recovery creates one order.
- Invalid JSON is committed only after successful DLT publication.
- A reservation ID shorter than eight characters cannot panic the worker.

## 7. Phase 4 — Update Product DB effectively once

### Database migration

Product DB already has `processed_events`. Preserve this key and verify migration behavior in a fresh database:

```sql
CREATE TABLE processed_events (
    consumer_name VARCHAR(100) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_name, event_id)
);
```

The Product Service model is already included in GORM AutoMigrate. Add an explicit migration if production deployments move away from AutoMigrate.

### Consumer transaction

Product Service already consumes `FLASH_SALE_ORDER_CONFIRMED`. Extend its tests and event contract for one confirmation event per Flash Sale line. Its Product DB transaction must retain this behavior:

```text
1. INSERT processed_events(consumer_name, event_id).
2. Inspect `RowsAffected`; if it is `0` because of `ON CONFLICT DO NOTHING`, return Duplicate and commit the Kafka offset.
3. Conditional UPDATE product_stock_allocations:
     sold_quantity = sold_quantity + quantity
   WHERE campaign_id = ?
     AND product_id = ?
     AND sold_quantity + released_quantity + quantity <= allocated_quantity.
4. Require RowsAffected = 1.
5. COMMIT Product DB.
6. COMMIT Kafka offset.
```

Refactor `IncrementSoldQuantity` to accept a transaction, or expose a higher-level repository method that owns both processed-event insertion and allocation update.

The insert must not infer duplicate status only from `Error == nil`, because `ON CONFLICT DO NOTHING` returns no error for duplicates:

```go
result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&processed)
if result.Error != nil {
    return result.Error
}
if result.RowsAffected == 0 {
    return ErrDuplicateEvent
}
```

Do not decrement `products.stock` for this event because the stock was already removed during allocation.

### Failure classification

- Duplicate `event_id`: success without changing counters.
- Allocation not found: retry for a bounded period, then DLT and alert.
- Invariant violation: DLT and alert; do not silently commit as success.
- Database/network error: retry without committing the offset.

### Acceptance tests

- Delivering one event increments `sold_quantity` once.
- Delivering the same event 100 times still increments it once.
- Two concurrent consumers processing the same event produce one increment.
- `ON CONFLICT DO NOTHING` with `RowsAffected == 0` never proceeds to the allocation update.
- An update beyond allocated quantity fails without recording the event as processed.
- `products.stock` remains unchanged during a Flash Sale confirmation.

## 8. Phase 5 — Make Redis confirmation recoverable

The Order Service consumer group for `FLASH_SALE_ORDER_CONFIRMED` already treats Redis as a projection. Verify it handles each new mixed-order Flash Sale line correctly:

1. Call the idempotent `ConfirmFlashSaleReservation` Lua script.
2. Write the order-status snapshot.
3. Publish the SSE/PubSub notification.
4. Commit the Kafka offset only after the durable Redis projection is updated. Pub/Sub notification failure alone must not block the offset forever because Pub/Sub is ephemeral; clients can recover through the persisted status snapshot/polling endpoint.

`ALREADY_CONFIRMED` must be treated as success. Pub/Sub delivery itself is ephemeral, so the persisted Redis status snapshot and `GET /flash-sales/orders/:reservationId` remain the polling fallback.

Add reconciliation from Order DB for cases where Redis data is lost entirely:

- Load active campaigns and their items.
- Rebuild available stock from `allocated_stock - reserved_stock - sold_stock`.
- Rebuild non-expired active reservations.
- Restore confirmed status snapshots when requested or through a bounded background job.

- **Fix leak in `ReconciliationWorker` for confirmed reservations**:
  - **Current behavior**: `ReconciliationWorker` already calls `ConfirmFlashSaleReservation` for Order DB `CONFIRMED` reservations and removes leftover ZSet members for `ALREADY_CONFIRMED` or `INVALID_STATE`.
  - **Fix**: Verify the Redis reservation hash is absent or already confirmed before direct `ZREM`. If `INVALID_STATE` means a present but corrupt hash, rebuild the projection instead of merely deleting the expiry entry.
  - **Rationale**: Reserved/purchased counters and expiry membership must remain consistent with Order DB after a worker crash.

### Acceptance tests

- Crash after Order DB commit but before Redis confirmation is repaired after event redelivery.
- Duplicate confirmation does not change counters twice.
- Redis flush followed by rebuild produces counters matching Order DB.

### Missing Redis reservation during confirmation

If Redis was flushed, `ConfirmFlashSaleReservation` cannot reconstruct a missing reservation from the current confirmation event alone. Treat `INVALID_STATE`/missing reservation as a projection-rebuild case rather than retrying forever. Rebuild the item from Order DB campaign counters and active reservations, then retry confirmation, or mark the projection for a bounded reconciliation job.

## 8.1. Payment confirmation policy

The first release is COD-only for every order containing a Flash Sale line. The current Flash Sale endpoint already rejects non-COD payment. For COD, `CONFIRMED` means the order and inventory commitment succeeded; it does not mean cash was collected. A mixed order reaches that state only after the regular-stock result and all Flash Sale reservations are confirmed. Online mixed-cart payment remains a separate future phase with payment webhook, timeout, cancellation, and refund handling.

### Acceptance tests

- COD confirmation produces one sold unit.
- Non-COD is rejected when COD-only policy is enabled.
- A mixed COD order never increments allocation `sold_quantity` before regular-stock success and Order DB confirmation.

## 9. Phase 6 — Make campaign activation resumable

### Changes

- Keep `ALLOCATING` while idempotently allocating every item.
- Keep `PREWARMING` until every Redis item has been verified.
- Do not log-and-ignore prewarm errors.
- Mark `ACTIVE` only after all required Redis keys exist with the expected quantities and configuration.
- If activation cannot complete, either retry from the current state or enter `COMPENSATING` and release every confirmed allocation.
- Record per-item activation progress or derive it from Product allocations plus Redis verification.

Use deterministic request IDs already present:

```text
alloc-camp-{campaignID}-prod-{productID}
compensate-camp-{campaignID}-prod-{productID}
```

### Acceptance tests

- Crash after allocating item 1 resumes without allocating it twice.
- Failure prewarming item 2 never exposes a partially usable active campaign.
- Compensation called repeatedly returns regular stock once.

## 10. Phase 7 — Make campaign ending resumable

### Changes

- Transition `ACTIVE -> ENDING` and close Redis entry gates first.
- Wait for or expire outstanding reservations **and PENDING mixed orders** according to policy. Do not release allocation while any mixed checkout can still confirm a Flash Sale line.
- Establish a settlement barrier before releasing stock: for each item, wait until Product DB `sold_quantity` equals the authoritative Order DB `sold_stock`. Publishing an Outbox row is not sufficient because the Product consumer may still be behind.
- Add/read an internal Product Service allocation-status endpoint so the ending coordinator can verify `sold_quantity`. If the values do not match, keep the campaign in `ENDING` and retry; never release optimistically.
- Call Product Service release for every allocation.
- Do not ignore release errors.
- Remain in `ENDING` while any item is unreleased.
- A scheduled worker retries incomplete release steps.
- Transition to `ENDED` only after Product Service confirms all items satisfy:

```text
sold_quantity + released_quantity = allocated_quantity
```

Release must use a deterministic idempotency key:

```text
end-release-{campaignID}-{productID}
```

Product Service already locks the allocation row and records release request IDs. Verify this with concurrent integration tests; keep the product stock increment and allocation update in one transaction. Change the barrier check from `>=` to equality and alert when Product DB `sold_quantity` exceeds Order DB `sold_stock`.

### Acceptance tests

- With allocated 30 and sold 5, ending releases 25 and regular stock becomes 95.
- Product Service unavailable keeps the campaign in `ENDING`.
- A delayed confirmation event keeps the campaign in `ENDING`; release starts only after Product DB catches up with Order DB.
- Restart during ending resumes remaining items.
- Repeated end/release requests do not increase stock twice.
- Two concurrent release requests increase regular stock only once.

## 11. Phase 8 — Harden Outbox lease ownership

Change completion methods to require lease ownership:

```go
MarkPublished(id, workerID string) error
MarkFailed(id, workerID string, retryIn time.Duration) error
```

The update condition must include:

```sql
WHERE id = :id
  AND status = 'PROCESSING'
  AND locked_by = :worker_id
```

Require `RowsAffected = 1`. Consider renewing the lease during a slow publish, or claim a smaller batch so sequential publishing cannot exceed the 30-second lease.

### Acceptance tests

- An expired worker cannot overwrite a new worker's result.
- A stale `MarkFailed` cannot change `PUBLISHED` back to `PENDING`.
- Two workers claim disjoint pending events.
- A crashed worker's event becomes claimable after lease expiration.

## 12. Observability

Add structured logs and metrics for:

- event ID, reservation ID, order ID, campaign ID and product ID;
- allocation invariant violations;
- campaigns stuck in `ALLOCATING`, `PREWARMING`, `COMPENSATING`, or `ENDING`;
- outbox pending age, attempts and stale leases;
- consumer retries and DLT count;
- Redis-versus-Order-DB reconciliation differences.

Do not use user IDs or reservation IDs as metric labels because they create unbounded cardinality.

Suggested alerts:

```text
oldest_outbox_pending_age > 60 seconds
campaign_intermediate_state_age > 5 minutes
allocation_invariant_violation_total > 0
flash_sale_consumer_dlt_total > 0
redis_reconciliation_difference_total > 0
```

## 13. Delivery sequence

Implement in this order; do not expose mixed checkout until the stock-result Saga and its recovery tests pass:

1. Audit the current baseline: Product confirmation consumer, COD-only Flash Sale, Outbox, release lock, and settlement barrier already exist. Add regression tests for their remaining failure paths.
2. Define item-level order metadata, mixed stock-request/result events, stable event IDs, and persistent mixed-order/reservation links. Keep existing events compatible while consumers migrate.
3. Make Product Service's regular-stock request and compensation idempotent; report a durable result. Add Order Service's state transitions and retry worker for mixed orders.
4. Implement COD mixed checkout with Redis reservation, Order DB transaction + Outbox, regular-stock result, final confirmation/cancellation, and crash recovery.
5. Add the single and batch Offer APIs with bounded requests, Redis index invalidation/rebuild, and server-verified price quote/409 protocol.
6. Update product listings, product detail, cart and checkout to share the offer resolution. Keep the direct Flash Sale button as an optional shortcut to the same price/rules.
7. Fix campaign ending to drain pending mixed orders, compare sold counters with equality, then release. Complete activation/reconciliation and Outbox lease improvements.
8. Run end-to-end concurrency, duplicate-event, timeout and crash tests; update README and architecture documentation with the actual implemented flow.

Online payment for mixed carts is outside this first delivery sequence. Do not claim it works based on a Redis TTL alone.

## 14. Definition of done

The change is complete when all of the following pass:

- Existing unit tests.
- Offer-routing tests across homepage, search, product detail, cart, and direct normal-checkout bypass.
- A stale sale quote returns `409` and no higher-priced order is created before customer acceptance.
- Adding to cart does not reserve stock; concurrent checkout for the last Flash Sale unit confirms at most one order.
- A mixed checkout with a regular item out of stock cancels its Flash Sale reservations and creates no successful order.
- A delayed regular-stock success after Flash Sale reservation expiry is compensated, not confirmed.
- An application crash after any mixed-checkout step leaves a recoverable PENDING state or a completed compensation.
- Repository transaction and invariant tests.
- Duplicate-event tests for every consumer.
- Integration test with PostgreSQL, Redis and Kafka.
- Crash after Redis reserve but before Order DB commit.
- Crash after Order DB commit but before outbox publish.
- Crash after Kafka publish but before outbox `PUBLISHED` update.
- Crash after order confirmation but before Redis projection.
- Product Service unavailable during confirmation.
- Product Service unavailable during campaign end.
- Product confirmation delayed while campaign ending begins.
- Two concurrent campaign-release requests.
- Duplicate `processed_events` insert returning `RowsAffected == 0`.
- Redis data-loss and rebuild test.
- COD mixed checkout succeeds; non-COD mixed checkout is rejected before any reservation.
- Concurrency test: requested/sold/released quantities never exceed allocation.

Final numerical assertion for a representative scenario:

```text
initial regular stock = 100
allocated             = 30
confirmed Flash Sales = 5
released at end       = 25
final regular stock   = 95

100 = 5 sold + 95 remaining
```

## 15. Architectural Review Additions & Rationales (For External Evaluation)

This section documents additional critical edge cases and architectural refinements identified during peer review, along with the technical rationales to assist reviewers in evaluating the implementation plan.

### 15.1. Dual Outbox Publishing for Downstream Compatibility

* **Code Location**: `services/order-service/internal/worker/order_email_worker.go:L29`, `pkg/kafka/topics.go`
* **Vulnerability**: The existing notification pipeline (`order_email_worker.go`) listens to `TopicOrderEvents` (`order.events`) for `ORDER_CREATED` events to generate and dispatch confirmation emails. If `FlashSaleWorker` completely replaces `ORDER_CREATED` with `FLASH_SALE_ORDER_CONFIRMED`, existing downstream consumers unaware of flash-sale-specific event schemas will silently stop receiving order events, breaking customer emails and external webhooks.
* **Proposed Fix**: Within the Order DB transaction, write two events into the Outbox:
  1. `FLASH_SALE_ORDER_CONFIRMED` (partition key `campaign_id:product_id`) for `product-service` inventory ledger tracking.
  2. `ORDER_CREATED` (with flag `IsFlashSale: true`) for general downstream subscribers (email, invoices, analytics).
* **Rationale**: Decouples the specialized Flash Sale stock settlement contract from general order lifecycle consumers without requiring immediate cross-service refactoring. This guarantees durable publication of `ORDER_CREATED`; reliable SMTP delivery still requires `OrderEmailWorker` to retry send failures before committing its Kafka offset.

### 15.2. Hybrid Confirmation Architecture (Low-Latency SSE + Async Safety Net)

* **Code Location**: `services/order-service/internal/worker/flash_sale_worker.go:L240-L257`
* **Vulnerability**: Moving Redis confirmation, order status snapshot generation, and SSE Pub/Sub strictly to the downstream Kafka consumer creates queuing and ingestion latency jitter (tens to hundreds of milliseconds during high-concurrency spikes). Clients connected to `/flash-sales/orders/:reservationId/stream` or polling for status would experience noticeable delays.
* **Proposed Fix**: Implement a **Dual-Path Confirmation** strategy:
  - **Inline Best-Effort (Fast Path)**: Directly after the PostgreSQL transaction commits in `FlashSaleWorker`, perform a synchronous invocation with a strict short timeout of `ConfirmFlashSaleReservation`, write the Redis order snapshot, and trigger SSE Pub/Sub.
  - **Async Kafka Projection (Safety Net)**: The Kafka consumer processing `FLASH_SALE_ORDER_CONFIRMED` idempotently performs the exact same confirmation step on Redis.
* **Rationale**: The Lua script `ConfirmFlashSaleReservation` is designed to be idempotent (returns `ALREADY_CONFIRMED` when invoked repeatedly). The optimistic inline call minimizes user-facing latency, while the asynchronous Kafka consumer supplies the durable recovery path if the worker crashes before reaching Redis. Any latency SLA must be verified by load tests.

### 15.3. Prevention of False Reservation Cancellation on Transient DB Errors

* **Code Location**: `services/order-service/internal/worker/flash_sale_worker.go:L228-L236`
* **Review check**: The standalone worker now classifies processing as succeeded, terminal, or retryable. Keep the same rule in every new mixed-order consumer: transient DB failures retry without committing the Kafka offset; only a terminal business outcome can cancel the reservation.
* **Rationale**: Temporary database pressure must not forfeit a valid customer reservation.

### 15.4. Pruning Stale Confirmed Reservations in `ReconciliationWorker`

* **Code Location**: `services/order-service/internal/worker/reconciliation_worker.go:L100-L103`
* **Review check**: `ReconciliationWorker` now attempts Redis confirmation for Order DB `CONFIRMED` reservations. Verify that `INVALID_STATE` is not treated as success for a corrupt but still present reservation hash; rebuild the projection when needed. A direct `ZREM` is safe only for a proven orphan entry.
* **Rationale**: Redis reserved/purchased counters and expiry membership must agree with the durable Order DB state.

### 15.5. Batch Offer Resolution API to Prevent Frontend N+1 Query Storms

* **Code Location**: `services/order-service/internal/delivery/http/flash_sale_handler.go`, `frontend/src/features/products/components/product-card.tsx`, `frontend/src/features/cart/components/cart-drawer.tsx`
* **Vulnerability**: A per-card offer request creates many HTTP calls on catalog and search pages.
* **Proposed Fix**: Introduce bounded `POST /flash-sales/offers/batch` requests and a Redis active-offer index maintained and rebuilt by Order Service. Fetch offers once per visible page of products; measure response time under load.
* **Rationale**: Reduces network requests. Redis `HMGET` still performs work proportional to the number of requested products, and prices must be checked again at checkout.

### 15.6. Anti-Hoarding Cart Policy (Strict Stock Reservation Deferral)

* **Code Location**: `services/order-service/internal/service/cart_service.go:L41-L89`, `pkg/redislock/flash_sale.go`
* **Vulnerability**: Reserving when adding to cart would let abandoned carts hold discounted stock until expiry.
* **Proposed Fix**: Enforce strict decoupling between Cart Intent and Stock Reservation:
  - `AddToCart` stores only intent (`product_id`, `quantity`) and caches an advisory display price. It acquires zero Redis locks and makes no database reservations.
  - Stock reservation via Redis Lua script is deferred strictly to checkout submission (`POST /orders/checkout`).
  - Frontend displays an advisory banner: *"Giá ưu đãi và số lượng chỉ được đảm bảo khi hoàn tất đặt hàng"*.
* **Rationale**: A long-lived cart does not consume short-lived Flash Sale allocation; checkout remains the contention point.

### 15.7. Mixed Cart Checkout Race Condition Protocol & Re-Quoting UX

* **Code Location**: `services/order-service/internal/service/order_service.go`, `frontend/src/features/cart`
* **Vulnerability**: Flash Sale stock or price can change between cart viewing and checkout. A silent price increase would differ from the customer's accepted quote.
* **Proposed Fix**: Standardize an explicit HTTP `409 Conflict` error protocol:
  - Error code: `FLASH_SALE_OUT_OF_STOCK` or `PRICE_CHANGED`.
  - Response payload: identifies `affected_items`, previous quoted price, and updated regular price.
  - Frontend UX: Renders a dedicated resolution dialog presenting two unambiguous choices:
    1. *Action A (Accept Regular Price)*: Customer accepts the updated total and starts a new checkout attempt.
    2. *Action B (Omit Item)*: Customer reviews the basket without the item, then explicitly submits the updated order.
* **Rationale**: Keeps the charged amount and purchased items aligned with the customer's final confirmation.

### 15.8. Later phase: mixed order online payment

The first mixed-cart release is COD-only. A later online-payment phase needs a durable payment intent and deadline, verified and idempotent webhooks, reservation expiry in Order DB, Product Service regular-stock hold/release, and a late-payment refund path. Redis key expiry alone does not perform those transitions. Implement and test that Saga separately before enabling VNPAY, MoMo, or banking for mixed Flash Sale orders.

## 16. Review vòng 2 — việc bắt buộc sửa trước khi bật mixed checkout

Phần này là **danh sách thực thi cho trạng thái code hiện tại**, bổ sung cho các phase và Definition of Done phía trên. Các test Go và TypeScript hiện tại chạy được nhưng chưa chứng minh các race Kafka/Redis/PostgreSQL dưới đây an toàn. **Chưa coi mixed checkout là hoàn thành** chỉ vì API trả về đơn hàng hoặc unit test pass.

### 16.1. P0 — Chống trừ kho thường hai lần khi nhận hai bản sao cùng lúc

**Code cần sửa:** `services/product-service/internal/worker/mixed_order_stock_worker.go` (`processMessage`), repository/model thuộc Product Service; `services/order-service/internal/service/order_service.go` (đường publish request).

**Lỗi hiện tại:** Order Service ghi stock request vào Outbox nhưng cũng publish trực tiếp sau commit. Hai bản sao có cùng `event_id` có thể được tiêu thụ gần đồng thời. Product worker đang `GetEvent` **trước** transaction trừ kho; cả hai worker có thể cùng thấy chưa có marker, rồi cùng trừ kho. `SaveEvent` dùng upsert sau khi trừ, nên unique key không ngăn lần trừ thứ hai.

**Thiết kế yêu cầu:**

1. Bỏ direct publish `PublishMixedOrderStockRequest` khỏi request HTTP; **Outbox là đường phát duy nhất**. Nếu cần giảm độ trễ, đánh thức Outbox worker sau commit, không tạo thêm một publisher độc lập. Outbox row ID phải bằng payload `event_id`, và event ID được tạo một lần.
2. Trong Product DB, tạo một bản ghi xử lý kho hỗn hợp có `UNIQUE(order_id)` (ví dụ `mixed_order_stock_operations`) với trạng thái và kết quả bền vững. Có thể dùng một giải pháp tương đương, nhưng khóa phải đại diện cho **đơn hàng**, không chỉ cho một Kafka event ID: các event ID khác nhau vẫn có thể yêu cầu trừ cùng một đơn.
3. Consumer request chạy **một transaction**: claim/lock `order_id` bằng unique constraint + row lock hoặc CAS; nếu đã xử lý, lấy kết quả đã lưu; nếu chưa, kiểm tra và trừ toàn bộ regular lines bằng conditional update `stock >= quantity`, lưu trạng thái/kết quả và outbox stock-result; commit một lần. Không làm `GetEvent` ngoài transaction rồi coi kết quả đó là khóa đồng bộ.
4. Nếu một dòng thiếu kho, rollback mọi trừ kho trong transaction, ghi kết quả FAILED bền vững trong một transaction riêng và phát kết quả từ outbox. Phân biệt business failure với lỗi DB tạm thời; lỗi DB phải retry, không ghi FAILED giả.

**Test bắt buộc:** chạy đồng thời hai consumer với cùng request và cả với hai request khác `event_id` nhưng cùng `order_id`; stock chỉ giảm một lần, chỉ có một operation/result logic. Test crash sau DB commit nhưng trước Kafka publish: replay trả lại đúng kết quả, không trừ tiếp.

### 16.2. P0 — Compensation phải đúng dù đến trước request hoặc bị gửi nhiều lần

**Code cần sửa:** `services/order-service/internal/worker/reservation_expiry_worker.go`, `mixed_order_saga_worker.go`; `services/product-service/internal/worker/mixed_order_stock_worker.go` (`processCompensateMessage`); Kafka contracts/outbox cho kết quả compensation.

**Race hiện tại:** stock request và compensate đi qua **hai topic Kafka khác nhau**, nên không có thứ tự liên-topic. Expiry worker có thể gửi compensate trước khi Product xử lý request. Product hiện cộng stock vô điều kiện khi nhận compensate. Sau đó request đến và trừ kho: dù net có thể bằng 0, việc cộng trước có thể cho phép đơn khác mua quá số hàng thật. Ngoài ra expiry và late-success có thể phát hai compensation với **hai event ID khác nhau**, khiến Product cộng hai lần.

**State machine yêu cầu trong Product DB (khóa theo `order_id`):**

| Trạng thái trước | Request trừ kho | Request compensate |
| --- | --- | --- |
| Chưa có operation | Trừ kho một lần, lưu `DEDUCTED` hoặc `FAILED` | Lưu `CANCELLED_BEFORE_DEDUCT`; **không cộng kho** |
| `DEDUCTED` | Trả lại kết quả success cũ; không trừ thêm | Cộng lại đúng các dòng đã trừ **một lần**, lưu `COMPENSATED` |
| `FAILED` | Trả lại failure cũ | Đánh dấu kết thúc; **không cộng kho** |
| `CANCELLED_BEFORE_DEDUCT` | Trả kết quả cancelled/rejected; **không trừ kho** | No-op |
| `COMPENSATED` | Không trừ lại | No-op, trả lại kết quả compensation cũ |

Mọi chuyển trạng thái và thay đổi `products.stock` phải nằm trong **cùng Product DB transaction** dưới row lock/CAS. Nếu compensation đến khi request đang xử lý, nó phải đợi khóa và dựa vào trạng thái đã commit; không dựa vào thứ tự Kafka. Persist `deducted_items`/số lượng thật đã trừ để hoàn chính xác; không tin tùy ý vào danh sách món hàng trong compensation payload. Dùng identity ổn định theo `order_id`/operation, không theo timestamp. Không direct publish compensation song song với Outbox. Có `COMPENSATED` result bền vững để Order Service xác nhận việc hoàn đã hoàn tất; `CANCELLED` của order không được mặc định có nghĩa kho thường đã hoàn.

Ở Order DB, transition từ `PENDING` sang `COMPENSATING` khi hết hạn/thất bại, đồng thời ghi compensation intent vào Outbox; chỉ chuyển sang terminal `CANCELLED` sau khi Product báo đã `COMPENSATED` hoặc `CANCELLED_BEFORE_DEDUCT`/`FAILED`. Expiry, stock-success và stock-failure phải tranh chấp bằng row lock/CAS trên order; một nhánh thắng, nhánh còn lại tiếp tục từ trạng thái bền vững đó. Nếu một order có nhiều Flash Sale reservations, hủy/giải phóng tất cả reservation còn RESERVED trong cùng transaction, không chỉ reservation đầu tiên hết hạn.

**Test bắt buộc:** (A) compensate đến trước request; (B) request đến trước compensate; (C) hai compensation khác event ID cho cùng order; (D) expiry và success xử lý đồng thời; (E) crash sau khi hoàn stock nhưng trước ack; (F) order có từ hai Flash Sale lines trở lên. Sau mỗi kịch bản, stock cuối cùng phải bằng stock ban đầu nếu order bị hủy, không hơn và không kém.

### 16.3. P0 — Order DB cũng phải bảo vệ giới hạn Flash Sale allocation

**Code cần sửa:** `services/order-service/internal/service/order_service.go`, đoạn tăng `flash_sale_items.reserved_stock` trong transaction.

Đổi update hiện tại thành conditional update kiểu:

```sql
UPDATE flash_sale_items
SET reserved_stock = reserved_stock + :quantity,
    version = version + 1
WHERE id = :flash_sale_item_id
  AND campaign_id = :campaign_id
  AND reserved_stock + sold_stock + :quantity <= allocated_stock;
```

Kiểm tra `RowsAffected == 1`; nếu bằng 0 thì rollback toàn bộ Order DB transaction và giải phóng **mọi** Redis reservation đã giữ cho checkout này. Redis quyết định admission nhanh, nhưng Order DB vẫn phải giữ invariant vì Redis có thể lệch/rebuild hoặc có concurrent transaction. Không tạo order/outbox nếu conditional update thất bại. Khi release/confirm, giữ CAS status và kiểm tra counters tương ứng.

**Test bắt buộc:** Redis báo còn hàng nhưng Order DB đã hết allocation; checkout phải thất bại, không còn order/reservation/outbox, Redis được bồi hoàn. Test nhiều checkout đồng thời cho suất cuối: `reserved_stock + sold_stock <= allocated_stock` luôn đúng.

### 16.4. P0 — Không commit Kafka offset nếu kết quả FAILED/DLQ chưa bền vững

**Code cần sửa:** `services/product-service/internal/worker/mixed_order_stock_worker.go` cả request và compensation readers; `services/order-service/internal/worker/mixed_order_saga_worker.go`; repository lưu result/outbox.

Product worker hiện bỏ qua lỗi `SaveEvent(FAILED)`, lỗi publish failure và lỗi re-publish result cũ, rồi có thể trả `nil`; Kafka offset được commit dù Order Service chưa nhận kết quả. Các nhánh deserialize cũng bỏ qua lỗi publish DLQ. Sửa theo quy tắc:

- `nil`/ack chỉ sau khi stock operation **và** result-outbox đã commit, hoặc sau khi một bản sao đã được xác minh có result-outbox bền vững; không yêu cầu Kafka publish tức thì trong handler.
- Lỗi DB tạm thời, lỗi ghi outbox, hoặc lỗi ghi DLQ: trả lỗi, **không commit offset**. Nếu chọn DLQ qua Kafka trực tiếp thì phải kiểm tra publish thành công trước khi ack; tốt hơn là durable DLQ/outbox.
- Duplicate đọc result đã lưu và bảo đảm result còn/đã được lên Outbox. Không nuốt lỗi JSON của persisted result; đưa bản ghi hỏng vào cảnh báo/reconciliation thay vì ack mất sự kiện.
- Order Saga consumer chỉ ack sau khi trạng thái order, tất cả reservation transitions và compensation/confirmation outbox đã commit. Có test inject lỗi tại từng bước.

**Test bắt buộc:** thiếu kho + Kafka outage; lỗi ghi FAILED result; lỗi ghi outbox; duplicate success/failure; malformed message + DLQ outage. Không có trường hợp offset tăng mà thiếu kết quả bền vững.

### 16.5. P1 — Hoàn tất re-quote thật, không chỉ trả HTTP 409

**Code cần sửa:** `services/order-service/internal/dto/order_dto.go`, `internal/service/order_service.go`, `internal/delivery/http/order_handler.go`; frontend cart/checkout.

Hiện tại, `FLASH_SALE_OUT_OF_STOCK`/quota check chỉ áp dụng khi campaign vẫn được `GetActiveCampaign` trả về. Nếu Flash Sale vừa hết giờ/đóng sớm, backend có thể đi nhánh regular và tạo đơn giá thường mà không biết khách đang chấp nhận giá sale. Frontend hiện gửi `product_id`/`quantity` mà không gửi quote đã thấy; modal 409 không có quote mới được server xác nhận. `PRICE_CHANGED` chưa có đường kiểm tra thực tế.

**Giao thức cần triển khai:**

1. Offer/cart quote do server tạo gồm user ID, basket fingerprint (product ID + quantity), giá/nguồn giá từng line, campaign/version (nếu có), thời hạn và token ký HMAC hoặc bản ghi server-side. Không dùng giá từ FE làm nguồn sự thật. Giới hạn TTL và kích thước basket; token phải chống sửa nội dung.
2. Checkout bắt buộc gửi accepted quote token + idempotency key. Backend re-resolve toàn bộ basket **trước khi reserve/tạo order**. Nếu bất kỳ giá nào tăng, quota không còn, campaign kết thúc hoặc số lượng không đáp ứng quote: trả 409 với affected items, quote mới và tổng tiền mới; **không tạo order**.
3. Frontend hiển thị tổng mới và hai lựa chọn rõ: chấp nhận giá thường/tổng mới (submit request mới với quote token mới) hoặc bỏ/chỉnh món hàng. Không tự động submit sau khi hiện modal. Retry của cùng logical attempt giữ idempotency key; basket/giá được khách chấp nhận thay đổi thì dùng key mới.
4. Nếu stock Flash Sale cạn nhưng regular stock còn, cho phép mua regular **chỉ sau khi khách xác nhận giá mới**. Nếu giá không tăng, có thể áp dụng chính sách tự động nhưng phải ghi rõ và test. Tất cả entry points, kể cả gọi API checkout trực tiếp, theo cùng chính sách.

**Test bắt buộc:** sale hết giữa lúc xem cart và submit; campaign kết thúc trước checkout; quota hết; token hết hạn/bị sửa/thuộc user khác; hai lần re-quote liên tiếp; không có order giá cao hơn quote đã chấp nhận.

### 16.6. Gate triển khai và bàn giao cho reviewer

Thứ tự làm: **16.1 + 16.2 (cùng Product operation state machine) → 16.3 → 16.4 → 16.5 → integration/concurrency tests**. Đừng chỉ thêm `processed_events` hoặc thêm một event compensation: hai cách đó riêng lẻ không giải quyết race giữa các topic.

Trước khi đánh dấu xong, cung cấp cho reviewer: migration cho Product operation state, sơ đồ trạng thái request/compensate, contract Kafka và idempotency key, test chứng minh các interleaving trên PostgreSQL + Redis + Kafka, kết quả test, và cập nhật `backend/README.md`/`backend/ARCHITECTURE.md` theo `backend/AGENTS.md` nếu thay đổi model, topic hay luồng Saga. Các test SQLite/miniredis hiện có chỉ là unit tests; row lock, unique conflict, Kafka ordering và crash recovery cần integration tests trên các hệ thống thật.

## 17. Review vòng 3 — hướng dẫn sửa phần còn thiếu (không đánh dấu xong chỉ vì test pass)

Phần 16 đã được thực hiện **một phần**: Product Service có `mixed_order_stock_operations` theo `order_id`, request stock không còn direct publish, Order DB có conditional allocation update, và checkout đã bắt đầu nhận `quote_token`. Những việc dưới đây **chưa đạt ý định của phần 16**. Người thực thi cần sửa theo luồng từ đầu đến cuối, không chỉ thêm một field, một modal, hay một nhánh `if`.

### 17.1. P0 — Hoàn thiện quote → checkout → chấp nhận giá thường

**Lỗi cần hiểu:** `GetBatchProductOffers`/`GetProductOffer` hiện không phát `quote_token`, còn `CreateOrder` chỉ xác thực token **nếu client gửi**. Frontend mở checkout có thể không nhận token, rồi vẫn submit. Nếu campaign vừa kết thúc, backend không biết khách đã xem giá sale và có thể tạo đơn ở giá thường. Ở chiều ngược lại, nếu sale **vẫn ACTIVE nhưng hết suất**, lần submit đầu trả 409 với `new_quote_token` giá thường; khách bấm “Chấp nhận mua giá thường” rồi submit lại thì code vẫn đi vào nhánh `remaining < quantity` và lại từ chối. Nghĩa là cả hai biên của re-quote đều chưa đúng.

**Ví dụ bắt buộc phải chạy được:**

```text
11:59:58  UI hiển thị A giá sale 80, giá thường 100; quote Q1 ghi A=80/FLASH_SALE.
12:00:00  Sale kết thúc HOẶC suất sale bán hết, kho thường còn.
12:00:01  Checkout(Q1) -> HTTP 409, KHÔNG tạo order; trả quote Q2 ghi A=100/REGULAR.
12:00:05  Khách chủ động chọn "Chấp nhận giá 100".
12:00:06  Checkout(Q2) -> tạo order giá 100 đúng 1 lần, nếu kho thường còn.
```

**Cách triển khai:**

1. Tạo endpoint quote dành cho **toàn bộ basket** (ví dụ `POST /orders/checkout/quote`), nhận product IDs + quantities, xác thực user, trả line-level `price`, `purchase_mode`, campaign/version, `total`, `expires_at`, và token server ký hoặc quote record server-side. Single/batch offer API có thể tiếp tục phục vụ UI catalog nhưng **không thay thế basket quote** vì chưa biết số lượng và tổng đơn.
2. Frontend lấy basket quote trước khi cho submit; khi cart/quantity thay đổi, token cũ không còn hợp lệ và phải lấy quote mới. Nếu không lấy được quote, không được gửi checkout như thể đã chấp nhận một giá chưa xác minh. Backend checkout cũng **bắt buộc** có token hợp lệ cho đường mixed/Flash Sale và đường có khả năng chuyển từ sale sang regular; API trực tiếp không được bỏ qua quy tắc này.
3. `VerifyQuoteToken` phải kiểm tra user, expiry, chữ ký, **đúng toàn bộ basket và quantities**, không chỉ map theo product ID rồi bỏ qua item thừa/thiếu. Secret lấy từ cấu hình an toàn, không hard-code trong source. Bất kỳ lỗi token nào phải có response rõ để UI lấy lại quote; không tạo order.
4. Khi quote là `FLASH_SALE`: nếu sale hết, hết quota hoặc campaign kết thúc, trả 409 kèm Q2 giá thường; không reserve/tạo order. Khi khách submit **Q2 `REGULAR` đã được server phát**: nếu sale vẫn ACTIVE nhưng hết suất, **cho đi đường regular-stock** thay vì lại trả `FLASH_SALE_OUT_OF_STOCK`. Không được biến Q2 thành giá sale một cách âm thầm; nếu giá hoặc mode thay đổi lần nữa, phát Q3 và yêu cầu xác nhận lại theo cùng luật.
5. 409 phải chứa affected lines, giá cũ/mới, tổng mới, token mới. Nút “Chấp nhận giá thường” phải hiển thị **tổng mới**, cập nhật basket quote và cho phép submit Q2; không chỉ đóng modal. Một logical retry dùng lại idempotency key; khi khách chấp nhận Q2/basket mới thì dùng key mới.

**Test pass/fail:** không token; token từ user khác; token đúng chữ ký nhưng khác quantity/basket; sale hết giờ; sale ACTIVE nhưng stock sale bằng 0; quota hết; Q1→409→Q2→regular order thành công; Q2 hết hạn; giá lại thay đổi trước submit; gọi trực tiếp API checkout không qua FE. Không có đường nào tạo order giá cao hơn quote khách đã chấp nhận.

### 17.2. P0 — Đơn không được terminal `CANCELLED` trước khi Product xác nhận bồi hoàn

**Lỗi cần hiểu:** Product operation state machine đã có `COMPENSATED`/`CANCELLED_BEFORE_DEDUCT`, nhưng Order Service **không nghe kết quả compensation**. `reservation_expiry_worker.go` và `mixed_order_saga_worker.go` đang đổi order sang `CANCELLED` ngay khi mới **gửi yêu cầu** hoàn kho. Nếu Product worker lỗi/kẹt, giao diện và Order DB báo đã hủy trong khi kho thường chưa chắc được hoàn.

**Luồng cần làm:**

```text
PENDING --(stock fail / FS expiry)--> COMPENSATING
  Order DB transaction: khóa/CAS order, nhả toàn bộ FS reservations còn hợp lệ,
  ghi đúng một compensation intent vào Order Outbox.

Product Service nhận compensation:
  operation chưa trừ -> CANCELLED_BEFORE_DEDUCT, không cộng stock;
  operation DEDUCTED -> hoàn đúng số đã trừ, trạng thái COMPENSATED;
  operation FAILED/đã COMPENSATED -> no-op.
  Cùng Product DB transaction, ghi outbox compensation-result.

Order Service nhận compensation-result:
  kiểm tra order_id + operation identity, idempotent CAS COMPENSATING -> CANCELLED;
  chỉ lúc này mới phát snapshot/notification terminal CANCELLED.
```

Thêm `OrderStatusCompensating`, compensation-result contract/topic/consumer, Product outbox publisher hoặc cơ chế outbox chung, và retry/reconciliation cho order kẹt `COMPENSATING`. `PENDING -> CONFIRMED` và `PENDING -> COMPENSATING` phải CAS/lock để chỉ một nhánh thắng. Late stock success sau khi order đang `COMPENSATING` phải được xử lý dựa vào Product operation state, không bỏ qua vì order không còn PENDING. Cùng một order chỉ có **một logical compensation**; dùng key ổn định theo `order_id`, không ghép `UnixNano`. Bỏ direct publish compensation song song với outbox. Nếu có nhiều FS lines, expiry của một reservation phải làm sạch các reservation còn giữ chỗ của cùng order trong transaction phù hợp.

**Test pass/fail:** Product unavailable sau khi Order ghi compensation outbox: order vẫn `COMPENSATING`, không `CANCELLED`; Product xử lý xong và result tới thì mới `CANCELLED`. Test duplicate compensation/result, late success, expiry cùng lúc với success, crash ở từng cạnh transaction/outbox, nhiều FS lines. Kho cuối không vượt kho ban đầu, không bị thiếu, và order terminal khớp ledger Product.

### 17.3. P0 — Không dùng event payload để đoán số lượng hoàn khi Product ledger hỏng

**Lỗi cần hiểu:** `processCompensateMessage` hiện fallback sang `payload.Items` nếu `op.DeductedItems` rỗng hoặc JSON lỗi. Event yêu cầu hoàn **không chứng minh** Product đã thực trừ bao nhiêu. Cộng theo payload trong trường hợp ledger hỏng có thể tạo thêm hàng ảo.

**Cách sửa:** Với operation `DEDUCTED`, `DeductedItems` phải là dữ liệu bắt buộc, parse được và khớp số lượng dương/product IDs hợp lệ. Nếu thiếu/hỏng: rollback transaction, không cộng kho, không chuyển `COMPENSATED`, không ack compensation message; đưa order vào cảnh báo/reconciliation hoặc DLQ có điều tra thủ công. Chỉ dùng `payload.Items` để đối chiếu hoặc tracing, không làm nguồn số lượng hoàn. Với operation không từng trừ (`FAILED`, `CANCELLED_BEFORE_DEDUCT`), không cộng kho dù payload ghi gì.

**Test pass/fail:** xóa `DeductedItems`, JSON hỏng, payload bị sửa số lượng, payload có thêm product; trong mọi ca, stock không được cộng sai và trạng thái không được chuyển terminal thành công.

### 17.4. P1 — Làm kết quả Product Service bền vững, có đường tự phát lại

**Hiện trạng:** Product lưu `MixedOrderStockOperation.ResultPayload`, rồi publish `MixedOrderStockResult` trực tiếp sau DB commit. Request Kafka chưa ack sẽ replay nếu publish lỗi, nên có một đường cứu; tuy nhiên chưa có **Product DB result-outbox** và compensation-result như phần 16 yêu cầu. Product không thể chủ động phát lại kết quả từ DB khi input offset đã được commit nhưng downstream thiếu hoặc cần reconciliation.

**Cách sửa:** Ghi `stock-result` outbox trong **cùng Product DB transaction** với `DEDUCTED` hoặc `FAILED`; ghi `compensation-result` outbox trong cùng transaction với `COMPENSATED`/`CANCELLED_BEFORE_DEDUCT`. Outbox ID/event ID ổn định theo order + loại kết quả, publisher retry với lease/ack; consumer Order Service idempotent. `processed_events`/operation row không thay thế outbox. Nếu thiết kế chọn replay input Kafka thay cho Product outbox, phải ghi rõ điều kiện về retention, consumer offset, khôi phục khi offset đã commit và có integration tests chứng minh tương đương; không chỉ nói “Kafka sẽ gửi lại”.

**Test pass/fail:** crash ngay sau Product DB commit; Kafka result topic outage kéo dài; duplicate request; crash sau publish nhưng trước đánh dấu outbox published; Order Service restart; duplicate compensation-result. Kết quả cuối vẫn đến và chỉ đổi order một lần.

### 17.5. Điều kiện bàn giao cuối

Đừng báo “đã xong” khi chỉ thêm model, token field hoặc test SQLite. Reviewer cần xem được **API request/response mẫu Q1→409→Q2**, migration/order statuses mới, Product/Order outbox-result contracts, test liên-service PostgreSQL + Redis + Kafka cho từng race ở trên, và README/ARCHITECTURE được cập nhật theo `backend/AGENTS.md` khi model/topic/Saga thay đổi. Nếu một mục được cố ý để sau, ghi rõ là **out of scope** và không bật mixed checkout trước khi đã xử lý các mục P0.

## 18. Kế hoạch sửa sau review vòng 4 — mixed checkout đã có code, nhưng chưa đạt nghiệm thu

### 18.0. Nói rõ trạng thái hiện tại để tránh hiểu nhầm

**Mixed checkout không phải là “chưa được thực thi”.** Hiện đã có: nhánh tạo đơn Flash Sale + regular trong `order_service.go`, Redis Flash Sale reserve, Order DB reservation + stock-request Outbox, `MixedOrderStockWorker` ở Product Service, `MixedOrderSagaWorker` ở Order Service, `COMPENSATING` và compensation-result, basket quote endpoint và frontend checkout. Go unit tests/TypeScript check cũng chạy được. Phần **chưa đạt** là các invariant và trải nghiệm end-to-end bên dưới. Không viết lại toàn bộ Saga; sửa các điểm còn thiếu, thêm test chứng minh, rồi mới gọi là hoàn thành.

### 18.1. P0 — So sánh *mọi* giá hiện hành với quote đã chấp nhận, kể cả giá Flash Sale tăng

**Code:** `services/order-service/internal/service/order_service.go`, nhánh active Flash Sale khi còn stock (hiện gán `it.SalePrice` cho order item).

**Vì sao chưa đúng:** Q1 báo giá sale 80. Admin sửa `sale_price` thành 90 trong khi campaign vẫn ACTIVE và còn suất. Code hiện thấy còn suất thì gán giá 90 và tạo order, **không** so 90 với `quoted.QuotedPrice=80`, nên khách có thể đặt ở giá cao hơn giá vừa chấp nhận. Kiểm tra giá regular ở nhánh `PurchaseMode=REGULAR` không giải quyết nhánh sale này.

**Sửa:** Sau khi xác định purchase mode/giá hiện hành cho từng line, so sánh với quote server đã ký. Nếu giá hiện hành tăng hoặc mode/điều kiện đã chấp nhận thay đổi bất lợi, trả `PriceConflictError` + 409 + quote mới **trước Redis reserve và trước bất kỳ DB write nào**. Không lấy `quote.QuotedPrice` làm giá authoritative; lấy giá server hiện hành nhưng yêu cầu khách chấp nhận lại nếu cao hơn. Với giá sale giảm, chọn và ghi rõ policy (có thể tự áp giá thấp hơn), rồi test policy đó. Không bỏ qua `GenerateQuoteToken` error.

**Test nghiệm thu:** Q1 sale 80 → Admin đổi sale 90 → checkout(Q1) trả 409, không order/reservation; Q2 sale 90 → khách chấp nhận → checkout(Q2) tạo đúng một order giá 90. Thử cả quote sale 80 → campaign hết hoặc hết suất → Q2 regular 100; đường Q2 này phải mua được regular nếu regular stock còn.

### 18.2. P0 — Quote phải là điều kiện bắt buộc của checkout, FE không được submit khi chưa có quote

**Code:** `services/order-service/internal/service/order_service.go` (`CreateOrder`), `frontend/src/app/checkout/page.tsx`, `frontend/src/features/orders/services/order-service.ts`.

**Vì sao chưa đúng:** Code hiện `if req.QuoteToken != "" { Verify... }`, nghĩa là request không token đi qua. FE gọi `getBasketQuote` bất đồng bộ, nhưng `.catch(() => {})` và nút submit không chờ token; mạng chậm/lỗi vẫn có thể gửi `quote_token: undefined`. Cách này khiến API trực tiếp và FE đều có đường bỏ qua kiểm tra giá đã chấp nhận.

**Sửa:**

1. Với endpoint checkout áp dụng báo giá, thiếu/invalid/expired token phải bị từ chối **trước tạo order** bằng mã lỗi rõ để client lấy lại quote. Đừng chỉ bật rule cho request có Flash Sale theo trạng thái *hiện tại*: sale có thể vừa hết, lúc đó server không còn biết khách đã xem sale nếu thiếu token. Nếu cần duy trì API cũ không quote, tách route/version và nêu rõ route đó không dùng cho UI mixed checkout; không để đường cũ bypass policy sale-to-regular.
2. FE có trạng thái `quoteLoading/quoteReady/quoteError`, disable submit cho đến khi có quote đúng với basket hiện tại. Khi quantity/item thay đổi, xóa token cũ và lấy lại basket quote. Hiển thị line prices và `total` **từ cùng quote server** mà token đang đại diện, không hiển thị một tổng từ local cart nhưng gửi token của một tổng khác. Nếu lấy quote thất bại, cho retry lấy quote; không nuốt lỗi.
3. Trong backend, xác thực token gắn đúng authenticated user và toàn bộ `(product_id, quantity)`; kiểm tra giá hiện hành ở checkout. FE cần giữ idempotency key của cùng một lần submit/retry mạng, chỉ tạo key mới sau khi khách chấp nhận quote/basket mới.

**Test nghiệm thu:** submit khi quote API chậm/lỗi; gọi checkout trực tiếp không token; token hết hạn; basket đổi sau khi có token; khách xem sale nhưng campaign kết thúc trước submit. Không trường hợp nào tạo order giá cao hơn báo giá khách đã thấy.

### 18.3. P0 — Không hard-code khóa ký quote

**Code:** `services/order-service/internal/service/quote_token.go` và cấu hình/DI ở `cmd/main.go`.

**Vì sao chưa đúng:** `quoteSecret` là chuỗi cố định trong source. Người có source có thể tự ký một token trông hợp lệ; các môi trường dùng cùng khóa cũng không thể rotate an toàn. `VerifyQuoteToken` hiện cho qua điều kiện user ID rỗng trong payload.

**Sửa:** Đưa HMAC key vào secret config/env của Order Service; fail startup hoặc vô hiệu hóa checkout quote nếu secret thiếu/yếu, không dùng default cố định ở production. Truyền signer/verifier qua constructor hoặc interface để test dùng test-secret riêng. Kiểm tra `payload.UserID == authenticatedUserID` bắt buộc, kể cả payload rỗng. Giới hạn payload/token size, expiry hợp lý; hỗ trợ key rotation nếu cần rollout. Không log token/secret.

**Test nghiệm thu:** token user A không dùng được cho B; token có `user_id` rỗng bị từ chối; đổi một byte payload/signature bị từ chối; thiếu secret không cho hệ thống chạy ở mode chấp nhận checkout quote.

### 18.4. P1 — Frontend phải hiển thị và xác nhận *tổng tiền mới* của Q2

**Code:** `frontend/src/app/checkout/page.tsx`, cart store/DTO quote.

**Vì sao chưa đúng:** Nút “Chấp nhận mua giá thường” hiện chỉ set `newQuoteToken` và xóa cờ sale khỏi cart. UI chưa lấy `new_total`/new quote lines làm nguồn hiển thị. Khách có thể bấm xác nhận lần hai trong khi tổng tiền họ nhìn thấy vẫn là từ product/cart cache cũ, không khớp token Q2.

**Sửa:** Response 409 trả **đủ** quote mới: line prices, purchase modes, total, token, expiry. Modal hiển thị ví dụ `80 → 100` và tổng mới. Khi khách chọn chấp nhận, lưu nguyên quote Q2 và render checkout từ Q2; chỉ cho submit khi Q2 còn hiệu lực và basket không đổi. Không gọi `syncFlashSaleOffers({})` như một cách thay thế báo giá server. Nếu Q2 lại đổi, lặp lại 409/Q3. Nút back cho phép sửa cart và lấy quote khác.

**Test nghiệm thu:** sale 80 hết suất → 409 Q2 regular 100; trước lần submit thứ hai, UI hiển thị 100 và tổng mới; submit Q2 tạo order 100. Test quote lại thay đổi trong lúc modal đang mở.

### 18.5. P1 — Hoàn thiện Product result publication và crash recovery

**Code:** `services/product-service/internal/worker/mixed_order_stock_worker.go`, Product DB model/repository và một outbox publisher thuộc Product Service; Order compensation-result consumer.

**Hiện trạng và lựa chọn:** Product operation đã ghi trạng thái vào DB, nhưng stock-result và compensation-result vẫn publish Kafka **sau** commit, không có Product result-outbox. Retry input Kafka có thể phục hồi nếu offset chưa commit và message còn trong retention; nó không chứng minh đầy đủ case offset đã commit, publisher/downstream outage hoặc reconciliation chủ động. Phần 17.4 yêu cầu chọn rõ một trong hai thiết kế và chứng minh bằng test; ưu tiên **Product transactional outbox**.

**Sửa theo outbox:** Trong cùng Product DB transaction với `DEDUCTED`/`FAILED`/`COMPENSATED`/`CANCELLED_BEFORE_DEDUCT`, ghi result-outbox với ID ổn định theo `order_id + result_type`. Product publisher retry có lease lock, đánh dấu published sau Kafka ack. Duplicate request/compensate chỉ đọc trạng thái bền vững và bảo đảm result-outbox tồn tại; không phát một result khác với trạng thái hiện tại. Order consumer idempotent; chỉ chuyển `COMPENSATING → CANCELLED` khi result thành công thuộc đúng order/operation. Nếu cố ý giữ input-replay thay vì outbox, tài liệu hóa retention/offset/restart/reconciliation và viết integration tests cho mọi failure window; không tuyên bố “đã có outbox” khi chỉ có Order outbox.

**Test nghiệm thu:** crash sau Product DB commit trước publish; Kafka outage dài; publish thành công rồi crash trước mark published; duplicate request/result; Order Service restart; compensation-result đến lặp. Cuối cùng order và Product ledger khớp nhau, không trừ/hoàn lần hai.

### 18.6. Gate bàn giao

Thứ tự đề xuất: **18.1 + 18.2 → 18.3 → 18.4 → 18.5 → integration tests**. Các mục P0 là điều kiện bắt buộc trước khi bật mixed checkout cho người dùng. Bàn giao cho reviewer gồm: API mẫu Q1→409→Q2 với cả sale-price increase và sale-to-regular, test frontend cho tổng mới, test backend cho no-token/user-binding, test crash/race trên PostgreSQL + Redis + Kafka, và cập nhật README/ARCHITECTURE khi đổi config, model, topic hoặc Saga. Việc test Go/TypeScript pass hoặc có `mixed_checkout_test.go` là cần thiết nhưng **chưa đủ** để đánh dấu hoàn thành.

## 19. Kế hoạch sửa sau review vòng 5 — bốn lỗi còn lại của bản code mới

Phần 18 đã được sửa một phần: backend hiện bắt buộc quote, kiểm tra giá sale tăng và Product Service đã có outbox cho stock-result. **Không viết lại những phần này.** Tập trung vào bốn lỗi dưới đây; mixed checkout chỉ được bật sau khi các tiêu chí P0 pass. Vị trí file ghi tương đối so với `backend/`, trừ frontend.

### 19.1. P0 — Dừng vòng lặp gọi quote/offer API ở trang checkout

**Code hiện tại:** `../frontend/src/app/checkout/page.tsx` có `useEffect(..., [items])`; bên trong effect gọi `syncFlashSaleOffers(...)`. Hàm này trong `../frontend/src/features/cart/store/useCartStore.ts` luôn `map` ra một mảng `items` mới, dù giá trị offer không đổi.

**Vì sao lỗi:** response offer → cập nhật `items` → effect chạy lại → gọi quote/offer API tiếp → response offer → cập nhật `items`... Trang có thể tạo request storm, `quoteStatus` liên tục về `loading`, nút đặt hàng khó/không bấm được. TypeScript compile không phát hiện vòng lặp này.

**Cách sửa:**

1. Tách dữ liệu **basket identity** (`product_id + quantity`) khỏi metadata hiển thị (`isFlashSale`, `salePrice`). Chỉ gọi basket quote khi basket identity thay đổi hoặc khi người dùng chủ động refresh/re-quote. Không lấy toàn bộ object/mảng `items` làm dependency cho effect nếu chính effect sẽ cập nhật metadata của chúng.
2. `syncFlashSaleOffers` chỉ ghi Zustand state khi offer thật sự khác; nếu không khác, trả state cũ. Không gọi lại quote từ một thay đổi metadata đơn thuần.
3. Chặn stale async response: nếu basket đổi trong lúc request A còn chạy, response A không được gắn token/giá cũ cho basket B. Abort request hoặc so khớp basket fingerprint khi response về. Khi basket đổi, xóa token cũ và đưa quote về loading cho đến khi quote mới sẵn sàng.

**Test nghiệm thu:** mount checkout với 2 items, quote và offer endpoints chỉ được gọi số lần hữu hạn (không lặp sau response); thay sale metadata nhưng không đổi product/quantity không re-fetch quote; thay quantity thì lấy quote đúng 1 lần và bỏ token cũ; response cũ về muộn không ghi đè quote mới. Nút đặt hàng ổn định ở trạng thái ready sau khi API thành công.

### 19.2. P0 — Compensation-result outbox phải commit cùng transaction hoàn kho

**Code hiện tại:** `services/product-service/internal/worker/mixed_order_stock_worker.go` đổi operation sang `COMPENSATED`/`CANCELLED_BEFORE_DEDUCT` trong transaction đầu, rồi mới `w.db.Transaction(...)` lần hai để ghi `ProductOutboxEvent`; lỗi transaction thứ hai bị bỏ qua. Sau đó direct Kafka publish cũng chỉ log warning nếu thất bại.

**Vì sao lỗi:** stock đã được hoàn, Product operation đã terminal; nhưng outbox insert lỗi và Kafka publish lỗi → consumer trả `nil`, Kafka offset của compensation được commit; Order Service mãi ở `COMPENSATING` vì không bao giờ nhận result. Đây **chưa phải transactional outbox**, dù đã có bảng outbox.

**Cách sửa:**

1. Tạo `MixedOrderStockCompensateResultPayload` và `ProductOutboxEvent` **bên trong chính transaction** đang đổi Product operation/cộng stock. Nếu ghi outbox lỗi, rollback luôn việc hoàn stock và status, trả lỗi để Kafka retry. Kiểm tra mọi lỗi `json.Marshal`, insert, commit; không `_ =` với bước đảm bảo bền vững.
2. Với duplicate compensation khi operation đã terminal, bảo đảm outbox result tồn tại hoặc tái tạo nó idempotently theo **ID ổn định** (`order_id + result_type`), không tạo nhiều logical result do event ID/timestamp khác nhau.
3. Sau DB commit, có thể direct publish best-effort để giảm latency; correctness dựa vào outbox publisher. Không đánh dấu PUBLISHED bằng owner giả (`direct-pub`) khi row chưa được claim theo lease; nếu fast-path publish thành công, vẫn cho outbox phát lại và consumer Order phải idempotent, hoặc thiết kế cơ chế claim/mark hợp lệ. Không nuốt lỗi outbox publisher `MarkPublished`/`MarkFailed` mà không log/metric.

**Test nghiệm thu:** inject lỗi insert Product outbox → không được commit `COMPENSATED` hay cộng stock; retry xử lý thành công đúng một lần. Crash ngay sau commit → publisher phát result. Kafka down → order ở `COMPENSATING`, outbox PENDING/retry; Kafka hồi phục → Order nhận result và sang `CANCELLED`. Duplicate compensation/result không cộng kho hoặc đổi trạng thái lần hai.

### 19.3. P0 — Loại bỏ quote secret mặc định cố định; thiếu secret phải fail startup

**Code hiện tại:** `pkg/config/appConfig.go` fallback `QUOTE_SECRET` → `APP_SECRET` → chuỗi cố định; `services/order-service/internal/service/quote_token.go` vẫn có `DefaultQuoteSecret`, và `NewOrderService` dùng default này khi không truyền secret đủ dài.

**Vì sao lỗi:** triển khai thiếu env vẫn chạy bằng key nằm công khai trong source. Người đọc source có thể ký quote token; môi trường khác nhau còn có thể dùng chung key ngoài ý muốn. `len >= 16` không làm một chuỗi hard-code trở thành bí mật.

**Cách sửa:** Đọc secret từ cấu hình triển khai thực sự; không có hard-coded fallback ở config, constructor hay quote signer. Order Service kiểm tra cấu hình khi startup và **không chạy checkout** nếu secret thiếu/không đạt yêu cầu; constructor nên trả lỗi hoặc nhận signer đã validate. Nếu dùng `APP_SECRET` làm fallback có chủ đích, phải bảo đảm `APP_SECRET` cũng bắt buộc, đủ mạnh và không phải giá trị mặc định; nên ưu tiên secret riêng cho quote. Test dùng test-secret inject rõ ràng. Không in secret/token vào log. Cập nhật tài liệu env/deployment mà không commit secret thật.

**Test nghiệm thu:** xóa `QUOTE_SECRET`/`APP_SECRET` → order-service không start ở môi trường chạy thật; secret rỗng/ngắn bị từ chối; secret test riêng ký và verify được; token ký bằng secret A không hợp lệ với secret B; không còn chuỗi secret mặc định trong source.

### 19.4. P1 — Sau khi chấp nhận Q2, toàn bộ phần tóm tắt phải dùng Q2

**Code hiện tại:** modal 409 đã hiển thị `new_total`, nhưng nút chấp nhận chỉ `setQuoteToken(newQuoteToken)` và `syncFlashSaleOffers({})`; danh sách món/tổng checkout vẫn render từ local cart `getTotalPrice()`. Điều đó không chứng minh khách nhìn thấy đúng giá/tổng mà Q2 sẽ gửi lên backend.

**Cách sửa:** Response 409 trả **đầy đủ** quote Q2: token, từng line `unit_price`, `subtotal`, `purchase_mode`, `total`, expiry. FE lưu Q2 thành một object duy nhất `{token, basketFingerprint, lines, total, expiry}`. Sau khi khách chấp nhận, summary checkout render từ object Q2; token gửi khi submit chính là token của object đang hiển thị. Không dùng `syncFlashSaleOffers({})` để suy ra giá thường từ product cache. Nếu basket đổi hoặc Q2 hết hạn thì vô hiệu hóa Q2, lấy quote mới. Khi một Q3 409 xảy ra, lặp lại quy trình thay vì âm thầm đổi giá.

**Test nghiệm thu:** Q1 sale 80 → 409 Q2 regular 100 → modal và summary đều hiện 100 trước lần submit thứ hai → đơn tạo giá 100. Với giỏ hỗn hợp 2+ dòng, tổng phải đúng bằng tổng các line của Q2. Khi một item đổi quantity sau Q2, nút submit bị chặn cho đến khi quote lại.

### 19.5. Thứ tự và điều kiện bàn giao

Làm **19.1 + 19.4** cùng nhau để tránh vòng lặp khi đồng bộ Q2 vào UI; sau đó **19.2**, rồi **19.3**, cuối cùng chạy test end-to-end. P0 phải xong trước khi bật mixed checkout. Reviewer cần: test frontend hành vi effect/summary, test lỗi outbox insert và crash-after-commit bằng PostgreSQL/Kafka, cấu hình env không có default key, và kết quả `go test` + TypeScript/build. Nếu đổi model/topic/config/luồng Saga, cập nhật `backend/README.md` và `backend/ARCHITECTURE.md` theo `backend/AGENTS.md`.

## 20. Bổ sung sau review vòng 6 — không bỏ qua message lỗi; làm test đồng thời đáng tin cậy

Các điểm 19.1–19.4 đã có sửa trong code hiện tại, nhưng **chưa đủ điều kiện nghiệm thu**. Mục 20.1 là lỗi correctness của Kafka/Saga; mục 20.2 là lỗi kiểm chứng khiến kết quả test đồng thời không đáng tin. Không cần viết lại mixed checkout hay Product outbox đã sửa.

### 20.1. P0 — Retry đúng message Kafka đang lỗi, chỉ commit sau durable outcome

**Code cần sửa:** `services/product-service/internal/worker/mixed_order_stock_worker.go` (`listenRequests`, `listenCompensations`), `services/order-service/internal/worker/mixed_order_saga_worker.go` (`listenResults`, `listenCompensateResults`), `services/product-service/internal/worker/flash_sale_confirmation_consumer.go`; rà soát các consumer Flash Sale/stock khác dùng cùng mẫu `FetchMessage → process error → continue → FetchMessage`.

**Lỗi hiện tại:** Khi `processMessage(m0)` lỗi, code `continue` vòng ngoài rồi `FetchMessage` lấy `m1`, chứ không thử lại `m0`. `FetchMessage` của `kafka-go` không tự trả lại message vừa fetch chỉ vì chưa commit. Nếu `m1` ở **cùng partition** được commit, offset commit cao nhất cũng bao gồm `m0`; Kafka có thể không gửi lại `m0` cho consumer group. Câu log “retry sau” hiện không tương ứng với hành vi thực. `CommitMessages` còn bị bỏ qua lỗi (`_ = ...`); `CommitInterval: time.Second` làm commit bất đồng bộ, nên kiểm tra giá trị trả về cũng chưa chứng minh broker đã lưu offset.

**Ví dụ phải ngăn được:** partition có offset 10 là `mixed.stock.request` của order A, offset 11 là order B. Product DB tạm lỗi ở offset 10. Worker lấy offset 11, xử lý B thành công và commit 11. Khi DB hồi phục, A có thể không bao giờ được xử lý; Order A kẹt `PENDING`/`COMPENSATING` hoặc stock ledger lệch. Quy tắc này cũng áp dụng cho compensation-result và `flashsale.confirmed`.

**Cách sửa:**

1. Với từng consumer, giữ `m` trong một **vòng retry nội bộ**: lỗi tạm thời → backoff có jitter, kiểm tra `ctx`, xử lý lại **cùng `m`**; không fetch message kế tiếp trên partition đó. Chỉ rời vòng khi DB/outbox đã commit thành công, duplicate đã được xác minh idempotent, hoặc poison message đã được đưa vào DLQ **bền vững**. Không commit khi DLQ publish thất bại. Cần chính sách cảnh báo/điều tra cho lỗi vĩnh viễn; không tạo vòng retry nóng vô hạn.
2. Sau durable outcome mới commit offset. Với đường saga quan trọng, dùng synchronous commit (`CommitInterval: 0`) và xử lý lỗi `CommitMessages`: retry commit hoặc dừng/restart consumer theo chính sách rõ ràng; tuyệt đối không fetch/commit offset cao hơn trong cùng partition khi commit trước chưa được xác nhận. DB transaction và idempotency phải chịu được tình huống xử lý thành công nhưng commit offset thất bại rồi message được giao lại.
3. Nếu thiết kế muốn vẫn xử lý các partition khác khi một partition bị lỗi, phải có cơ chế pause/partition-scoped retry và **commit theo thứ tự trong từng partition**; không dùng một vòng `FetchMessage` tuần tự hiện tại để suy diễn an toàn. Với bản đầu, chặn consumer đang lỗi là cách đơn giản và đúng hơn; đánh đổi throughput này cần được ghi rõ.
4. Kiểm tra tất cả nhánh `processMessage` trả `nil`: chỉ được coi là thành công khi state/outbox cần thiết đã bền vững hoặc DLQ đã nhận thành công. Không đổi lỗi DB/Redis/Kafka thành `nil` chỉ để consumer tiến tiếp.

**Test nghiệm thu bắt buộc:** Kafka thật (testcontainers hoặc môi trường integration), ít nhất hai message **cùng partition** với offset liên tiếp; inject lỗi DB ở message đầu, message sau thành công nếu được xử lý; xác nhận message sau **không** được commit trước message đầu. Sau khi DB hồi phục, cả hai được xử lý đúng một lần về mặt hiệu ứng dù có redelivery. Lặp lại cho stock request, compensation, stock-result/compensation-result và `flashsale.confirmed`. Thêm ca DLQ outage, commit failure, crash sau DB/outbox commit nhưng trước offset commit; không mất event và không trừ/hoàn kho hai lần. Unit test mock `processMessage` một mình **không** kiểm chứng lỗi offset này.

### 20.2. P1 — Sửa test concurrent để không báo sai hoặc pass giả

**Code cần sửa:** `services/product-service/internal/worker/mixed_order_stock_worker_test.go`, đặc biệt `setupTestWorkerDB` và `TestMixedOrderStockWorker_DuplicateRequest_Concurrent`.

**Hiện trạng đã quan sát:** `go test` Product worker có lần lỗi `database table is locked`; chạy test riêng một lần thì pass, nhưng `-count=10` gặp `UNIQUE constraint failed: products.slug` vì SQLite shared-memory dùng lại tên database giữa các lần chạy mà connection chưa đóng. Test bỏ qua lỗi `db.Create`, bỏ qua lỗi từ cả hai goroutine và `db.First`, rồi quy mọi stock khác 8 thành “double deduction”. Vì vậy log hiện tại **không chứng minh production bị trừ hai lần**, và một lần test pass cũng chưa chứng minh an toàn trên PostgreSQL.

**Cách sửa:** tạo database name duy nhất cho mỗi test invocation (kể cả `-count=N`), đóng mọi `sql.DB` trong `t.Cleanup`, và kiểm tra lỗi setup/create/query. Thu thập lỗi trả về từ hai goroutine qua channel hoặc biến được đồng bộ. Test phải phân biệt lỗi SQLite lock tạm thời với lỗi nghiệp vụ; không bỏ qua lỗi rồi chỉ assert stock. Unit test có thể dùng SQLite để kiểm tra nhánh cơ bản, nhưng invariant row-lock/unique-conflict phải có integration test PostgreSQL với hai request cùng `order_id`/khác `event_id` chạy đồng thời và kiểm tra Product stock, operation row, outbox result; test cả request và compensation đua nhau.

**Lệnh/gate nghiệm thu:** `go test ./services/product-service/internal/worker -run '^TestMixedOrderStockWorker_DuplicateRequest_Concurrent$' -count=10` phải ổn định; `go test ./services/order-service/... ./services/product-service/... ./pkg/kafka/...` phải pass; chạy thêm `go test -race` cho các worker liên quan và integration tests PostgreSQL + Kafka. Không đánh dấu mục 20.1 hoàn thành chỉ vì bộ unit test này pass.

### 20.3. Thứ tự bàn giao

Sửa **20.1 trước** vì có nguy cơ mất bước saga; sửa **20.2** để kết quả kiểm thử đáng tin, rồi chạy toàn bộ gate. Reviewer cần xem diff consumer loop/commit policy, bằng chứng test hai offset cùng partition, kết quả test lặp và test PostgreSQL. Nếu thay đổi contract/topic/luồng Saga hoặc cấu hình consumer, cập nhật `backend/README.md` và `backend/ARCHITECTURE.md` theo `backend/AGENTS.md`.
