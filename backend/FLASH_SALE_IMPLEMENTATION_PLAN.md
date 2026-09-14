# Flash Sale Correctness Implementation Plan

## 1. Objective

Close the confirmed correctness and crash-recovery gaps in the Flash Sale flow while preserving Database-per-Service:

- Order Service owns campaigns, reservations, orders, its outbox, and the Redis Flash Sale projection.
- Product Service owns regular product stock and `product_stock_allocations`.
- Kafka provides at-least-once delivery; each consumer provides effectively-once business effects.
- Redis remains a fast, rebuildable projection. PostgreSQL remains the durable source of truth.

This plan does not introduce distributed transactions or a shared database.

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

Add the contract to `pkg/kafka/events.go` and the event constant to `pkg/kafka/topics.go` or the existing event constants file.

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

`FlashSaleWorker` commits the order and then directly publishes `ORDER_CREATED`. A crash between those operations loses the event.

### Changes

Inside the existing Order DB transaction in `flash_sale_worker.go`:

1. Insert the processed input event marker.
2. Create `orders` and `order_items`.
3. CAS the reservation from `RESERVED/PROCESSING` to `CONFIRMED`.
4. Move Order DB item quantity from `reserved_stock` to `sold_stock`.
5. Insert a `FLASH_SALE_ORDER_CONFIRMED` outbox row for allocation settlement and Redis projection.
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

`FlashSaleWorker` commits the Kafka message even when its database transaction fails.

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
  - **Current problem**: In `flash_sale_worker.go:L234`, any `dbErr != nil` currently triggers `redislock.ReleaseFlashSaleReservation(..., "CANCELLED")`.
  - **Fix**: Remove the cancellation call on generic DB errors. Only release or cancel when a terminal business condition is confirmed (e.g. reservation expired or invalid data). On transient DB errors, return `ProcessingRetryable` without committing the Kafka offset.
  - **Rationale**: If the database suffers a momentary connection timeout, pool exhaustion, or deadlock, immediately cancelling the reservation on Redis drops a legitimate customer order and gives their reserved item away to someone else. Infrastructure glitches must be retried, not penalized as customer order cancellations.

Validate payloads before indexing or slicing `reservation_id`. This removes the short-ID panic risk.

### Acceptance tests

- A transient database failure does not commit the Kafka offset.
- A retry after recovery creates one order.
- Invalid JSON is committed only after successful DLT publication.
- A reservation ID shorter than eight characters cannot panic the worker.

## 7. Phase 4 — Update Product DB effectively once

### Database migration

Add `processed_events` to Product DB:

```sql
CREATE TABLE processed_events (
    consumer_name VARCHAR(100) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_name, event_id)
);
```

If the project continues using GORM AutoMigrate, define the Product Service domain model and include it in `AutoMigrate`; also keep an explicit SQL migration for reproducible deployments.

### Consumer transaction

Add a Product Service consumer for `FLASH_SALE_ORDER_CONFIRMED`. In one Product DB transaction:

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

Add an Order Service consumer group for `FLASH_SALE_ORDER_CONFIRMED` that treats Redis as a projection:

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
  - **Current problem**: In `reconciliation_worker.go:L100-L103`, the worker scans Redis Expiry ZSets (`fs:{c:C:p:P}:expiry`) for expired reservation candidates. If a reservation is already `CONFIRMED` in PostgreSQL, the worker currently ignores it without taking any action.
  - **Fix**: If `resv.Status == CONFIRMED` and the Redis reservation still exists as `RESERVED/PROCESSING`, the worker must call `ConfirmFlashSaleReservation` so reserved/purchased counters, reservation status, and the expiry ZSet are changed atomically. A plain `ZREM` is allowed only when the reservation hash is absent or the projection has already been confirmed and the ZSet member is provably orphaned.
  - **Rationale**: If a worker crashes between Order DB commit and Redis confirmation, the reservation remains stuck in the Redis Expiry ZSet forever. Because `reconciliation_worker.go` previously only handled `EXPIRED` and `CANCELLED`, confirmed reservations were never pruned from the ZSet, leaking Redis memory and triggering wasteful PostgreSQL lookups on every reconciliation cycle indefinitely.

### Acceptance tests

- Crash after Order DB commit but before Redis confirmation is repaired after event redelivery.
- Duplicate confirmation does not change counters twice.
- Redis flush followed by rebuild produces counters matching Order DB.

### Missing Redis reservation during confirmation

If Redis was flushed, `ConfirmFlashSaleReservation` cannot reconstruct a missing reservation from the current confirmation event alone. Treat `INVALID_STATE`/missing reservation as a projection-rebuild case rather than retrying forever. Rebuild the item from Order DB campaign counters and active reservations, then retry confirmation, or mark the projection for a bounded reconciliation job.

## 8.1. Payment confirmation policy

The implementation must define when a reservation becomes a sale. The current API accepts `COD`, `MOMO`, `VNPAY`, and `BANKING`, but the current worker confirms all of them immediately while `payment_status` remains `PENDING`.

Choose one explicit first-release policy:

1. **Recommended portfolio scope — COD-only Flash Sale**: reject non-COD Flash Sale requests until a payment saga exists. Order creation confirms the sale.
2. **Online-payment scope**: use `RESERVED -> PROCESSING` when the order/payment intent is created. Only a verified payment webhook may perform `PROCESSING -> CONFIRMED`, write the confirmation Outbox events, and count the allocation as sold. Payment failure/timeout releases the reservation; a valid late payment requires an idempotent refund workflow.

Do not count `payment_status=PENDING` online orders as sold without defining cancellation, timeout, late-webhook, and refund behavior.

### Acceptance tests

- COD confirmation produces one sold unit.
- Non-COD is rejected when COD-only policy is enabled.
- If online payment is implemented, an unpaid order never increments allocation `sold_quantity`.
- Payment webhook replay confirms or refunds exactly once.

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
- Wait for or expire outstanding reservations according to policy.
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

The current `ReleaseStock` accepts `requestID` but does not persist or check it, and its read-calculate-write sequence does not lock the allocation row. Make release concurrency-safe inside Product DB by locking the allocation row with `SELECT ... FOR UPDATE` before calculating `to_release`, or by using an equivalent conditional atomic update plus a unique release-request ledger. Require the product stock increment and allocation update to commit in the same transaction.

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

Implement in this order to keep each pull request reviewable:

1. Event contract and Product DB `processed_events` migration.
2. Add real `event_id` to `FLASH_SALE_RESERVED`, then implement the order confirmation transaction with both Outbox events.
3. Kafka acknowledgement and poison-message handling.
4. Product allocation confirmation consumer.
5. Redis projection consumer and rebuild reconciliation.
6. Select and enforce the COD-only or online-payment policy.
7. Resumable activation.
8. Concurrency-safe release plus the settlement barrier and resumable ending.
9. Lease ownership hardening.
10. End-to-end fault-injection tests and documentation update.

Do not enable automatic campaign ending in production-like demos until phases 1–4 are complete, because the current missing `sold_quantity` update can restore sold inventory.

## 14. Definition of done

The change is complete when all of the following pass:

- Existing unit tests.
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
- COD/non-COD behavior according to the selected payment policy.
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
* **Vulnerability**: In the current implementation:
  ```go
  if dbErr != nil {
      _, _ = redislock.ReleaseFlashSaleReservation(reqCtx, w.redisClient, task.CampaignID, task.ProductID, task.ReservationID, "CANCELLED")
      return
  }
  ```
  If `dbErr` is caused by a transient infrastructure failure (e.g. database network hiccup, connection pool starvation, deadlock retry), the worker prematurely cancels the customer's reservation in Redis and releases the stock to other competing buyers.
* **Proposed Fix**: Never release or cancel reservations in Redis upon generic database errors. Instead, classify the error as retryable (`ProcessingRetryable`), abstain from committing the Kafka offset, and allow bounded exponential backoff retries. Only invoke `ReleaseFlashSaleReservation` upon deterministic terminal rejections (such as expired or corrupted reservations).
* **Rationale**: Temporary database pressure should not penalize customers by forfeiting their validly secured Flash Sale slots.

### 15.4. Pruning Stale Confirmed Reservations in `ReconciliationWorker`

* **Code Location**: `services/order-service/internal/worker/reconciliation_worker.go:L100-L103`
* **Vulnerability**: `ReconciliationWorker` periodically iterates through entries in `fs:{c:C:p:P}:expiry` (Redis Expiry ZSet) whose score is older than `now - 60s`. When inspecting the database:
  - If status is `EXPIRED` or `CANCELLED`, it cleans up Redis.
  - If status is `CONFIRMED`, the worker currently performs **no action**.
  If a worker dies after DB commit but before completing Redis confirmation, that reservation remains stuck in the Redis Expiry ZSet permanently, causing Redis memory leakage and issuing redundant PostgreSQL SELECT queries on every subsequent reconciliation cycle.
* **Proposed Fix**: When `resv.Status == CONFIRMED` and Redis still contains a `RESERVED/PROCESSING` reservation, the reconciliation worker must execute `ConfirmFlashSaleReservation` so all counters, status, and the expiry member change atomically. Use a direct `ZRem` only when the reservation hash is absent or the projection is already confirmed and the expiry entry is provably orphaned.
* **Rationale**: Closes the state reconciliation loop between PostgreSQL and Redis, ensuring the Redis Expiry ZSet remains strictly bounded and self-healing.
