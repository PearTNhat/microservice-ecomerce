# Flash Sale mixed-cart implementation — code review

Status: **not ready to merge/deploy**. This review compares the current working-tree implementation with [FLASH_SALE_IMPLEMENTATION_PLAN.md](FLASH_SALE_IMPLEMENTATION_PLAN.md). It identifies correctness gaps, not a request to discard the existing work. Keep the standalone Flash Sale flow working while fixing the mixed-cart path.

## P0 — Must fix before enabling mixed checkout

### 1. Mixed reservation is not persisted as a valid allocation reservation

**Evidence:** [`order_service.go` lines 285–305](services/order-service/internal/service/order_service.go#L285-L305) creates `flash_sale_reservations` after creating the order, assigns `FlashSaleItemID: rev.campaignID`, and ignores `Create` errors. `FlashSaleItemID` must be the ID of the matching `flash_sale_items` row, not the campaign ID. This path also never increments that item's `reserved_stock`. By contrast, [`CreateReservationWithOutbox`](services/order-service/internal/repository/flash_sale_repository.go#L129-L159) inserts the reservation and conditionally increments `reserved_stock` in one transaction. [`ConfirmReservationDB`](services/order-service/internal/repository/flash_sale_repository.go#L221-L266) later requires `reserved_stock >= quantity` and updates the row identified by `FlashSaleItemID`.

**Failure example:** A fresh campaign item has `reserved_stock = 0`. Mixed checkout reserves one unit in Redis, creates a PENDING order, then the stock-result worker tries to confirm it. Confirmation fails because the Order DB still has `reserved_stock = 0`. If the incorrect campaign ID happens to match a different item ID with reserved stock, the wrong item's counters may be modified instead. An insert error can also leave an order with no durable reservation.

**Required fix:** Carry the actual `FlashSaleItem.ID` through checkout. Atomically persist the order, every reservation, the corresponding conditional `reserved_stock` increments, and the mixed stock-request outbox in **one Order DB transaction**. Check every database error and compensate Redis reservations if the transaction fails. Add a test with `campaign.ID != flashSaleItem.ID` and a test with multiple Flash Sale lines.

### 2. The mixed stock request can disappear after the order commits

**Evidence:** [`order_service.go` lines 274–323](services/order-service/internal/service/order_service.go#L274-L323) commits the order first, then calls `PublishMixedOrderStockRequest`; its error is ignored. There is no stock-request outbox row in this path.

**Failure example:** Order DB commits, then Kafka is unavailable or the process crashes. The order remains PENDING, but no request exists for Product Service to process. Retrying the HTTP request is not a reliable repair mechanism.

**Required fix:** Write a stock-request outbox event in the same Order DB transaction as the PENDING order/reservations; publish it through the existing outbox worker. Use a stable event ID. Add a crash-after-DB-commit test that proves the request is eventually published once Kafka recovers.

### 3. Product Service marks a request processed before stock/result is durable

**Evidence:** [`mixed_order_stock_worker.go` lines 113–128](services/product-service/internal/worker/mixed_order_stock_worker.go#L113-L128) commits the `processed_events` marker before deducting stock. Deductions then occur one item at a time ([lines 135–163](services/product-service/internal/worker/mixed_order_stock_worker.go#L135-L163)); the result is published afterward ([lines 165–203](services/product-service/internal/worker/mixed_order_stock_worker.go#L165-L203)). [`InsertIfNew`](services/product-service/internal/repository/processed_event_repository.go#L25-L45) returns an error for a duplicate, so replay enters the error/retry loop rather than completing or acknowledging an already finished event.

**Failure example:** After the marker commits, the process crashes before deduction: replay cannot do the deduction. Or stock is deducted and result publication fails: replay cannot publish the missing success result. A failed `RevertStock` is also ignored and can leak regular stock.

**Required fix:** In one Product DB transaction, deduplicate by stable request/order identity, conditionally deduct **all** regular lines, persist the outcome and an outbox result. On business failure, commit a durable failure result without partial deductions. A duplicate must retrieve/re-emit the same durable result, not stall the partition. Add crash/replay and duplicate-delivery tests.

### 4. Cancellation/expiry does not compensate already deducted regular stock

**Evidence:** [`reservation_expiry_worker.go` lines 67–90](services/order-service/internal/worker/reservation_expiry_worker.go#L67-L90) expires a Flash Sale reservation and cancels its PENDING order, but creates no regular-stock compensation. [`mixed_order_saga_worker.go` lines 118–130](services/order-service/internal/worker/mixed_order_saga_worker.go#L118-L130) ignores a late success result once the order is CANCELLED. The failure handler ([lines 293–318](services/order-service/internal/worker/mixed_order_saga_worker.go#L293-L318)) releases only Flash Sale reservations.

**Failure example:** Product Service successfully deducts a regular item; its success result is delayed until after the Flash Sale reservation expires. Order Service cancels the order and then ignores the late success. The customer has no valid order, but regular stock stays deducted.

**Required fix:** Introduce a durable, idempotent regular-stock compensation request/result keyed by order and line (or an equivalent stock hold/commit/release protocol). Persist compensation intent when expiry/failure wins the race; reconcile late success before declaring the saga fully cancelled. Retry compensation after crashes. Test success-before-expiry, success-after-expiry, duplicate result, and crash during compensation.

### 5. The 100%-Flash-Sale path can report a confirmed order without a confirmed allocation

**Evidence:** [`order_service.go` lines 325–337](services/order-service/internal/service/order_service.go#L325-L337) changes the order to CONFIRMED first, ignores errors from `ConfirmReservationDB`, and does not write `FLASH_SALE_ORDER_CONFIRMED` or `ORDER_CREATED` outbox events. Product Service therefore may not increment its allocation `sold_quantity`; campaign ending can return unsold stock that was actually sold.

**Required fix:** Use one Order DB transaction to confirm the order and **all** reservations and write one Flash Sale confirmation outbox event per line plus the general order event. Only report CONFIRMED after the transaction succeeds. Keep the existing standalone path as a regression test.

### 6. Order Service commits Kafka stock results even when saga processing fails

**Evidence:** [`mixed_order_saga_worker.go` lines 91–93](services/order-service/internal/worker/mixed_order_saga_worker.go#L91-L93) commits every fetched message. `handleStockSuccess` logs a DB transaction error and returns ([lines 259–262](services/order-service/internal/worker/mixed_order_saga_worker.go#L259-L262)); `handleStockFailure` ignores order-update and reservation-release errors ([lines 299–316](services/order-service/internal/worker/mixed_order_saga_worker.go#L299-L316)). The caller cannot distinguish success from retryable failure.

**Required fix:** Return an error from processing/handlers and commit the Kafka offset only after the durable Order DB transition and outbox writes succeed. Use compare-and-swap/row locking for competing expiry and success transitions. Malformed messages should be committed only after durable dead-letter handling.

## P1 — Plan/behavior mismatches

### 7. Sale-to-regular price fallback is silent

**Evidence:** [`order_service.go` lines 153–205](services/order-service/internal/service/order_service.go#L153-L205) marks an item Flash Sale only if enough sale stock and quota remain; otherwise it leaves the item as a regular-price line and proceeds. [`checkout/page.tsx` lines 123–145](../frontend/src/app/checkout/page.tsx#L123-L145) sends only product IDs and quantities, no accepted quote/version token. [`order_handler.go` lines 49–59](services/order-service/internal/delivery/http/order_handler.go#L49-L59) maps checkout errors to 400, not the planned 409 re-quote protocol.

**Failure example:** Customer sees a discounted price, clicks place order just after the last discounted unit sells, and receives a regular-price order without explicitly accepting the higher total.

**Required fix:** Issue a server-verified quote bound to user, basket, prices and expiry. At checkout, compare it with the current offer. On any price increase, create **no order** and return HTTP 409 plus a new quote; frontend must ask the customer to accept the regular price or remove the item. Revalidate on resubmission. Test the race between displaying the cart and checkout.

### 8. `ORDER_CREATED` inventory routing is ambiguous; do not rely on the new per-item skip alone

**Evidence:** The mixed saga emits `ORDER_CREATED` with `IsFlashSale = true` for the whole mixed order ([`mixed_order_saga_worker.go` lines 212–255](services/order-service/internal/worker/mixed_order_saga_worker.go#L212-L255)). [`product_stock_worker.go` lines 125–140](services/product-service/internal/worker/product_stock_worker.go#L125-L140) currently skips the **whole** event before reaching its new per-item `IsFlashSale` check. Thus, the present code does **not** double-deduct mixed regular lines on this path, but the two checks contradict each other. Removing the order-level skip later would deduct regular lines a second time because `MixedOrderStockWorker` already did so.

**Required fix:** Define an explicit inventory contract. For mixed orders, the dedicated mixed stock request owns regular-stock deduction; `ORDER_CREATED` is notification-only for inventory. Make the consumer enforce that contract using an explicit order type/stock-handling field or separate topic/consumer, and add a test asserting exactly one Product DB deduction per regular line across both events.

### 9. Campaign ending still lacks the mixed-order drain/barrier required by the plan

**Evidence:** [`EndCampaign`](services/order-service/internal/service/flash_sale_service.go#L236-L306) moves to ENDING and checks only Product `sold_quantity >=` the campaign item's `sold_stock`; it does not check pending mixed orders/reservations before release. The plan requires waiting for pending mixed checkouts and equality of sold counters.

**Required fix:** Do not release allocations while any eligible mixed order can still confirm a Flash Sale line. Drain/expire them according to an explicit policy, then require `Product.sold_quantity == Order.sold_stock`; treat `>` as an inconsistency needing investigation, not success. Add a delayed-confirmation/end-campaign race test.

### 10. Offer API reads the wrong auth local for per-user eligibility

**Evidence:** [`flash_sale_handler.go` lines 302–339](services/order-service/internal/delivery/http/flash_sale_handler.go#L302-L339) reads `c.Locals("userId")`, while the existing authenticated order handler reads `c.Locals("userID")` ([`order_handler.go` lines 43–46](services/order-service/internal/delivery/http/order_handler.go#L43-L46)). Verify the auth middleware's exact key; if it is `userID`, the offer service receives an empty user ID and skips quota checks.

**Required fix:** Use the same authenticated user context key consistently and test an account that has exhausted its Flash Sale quota.

## Acceptance checklist for the next review

- Mixed checkout creates the order, all valid reservation records/counters, and the stock-request outbox atomically; no ignored DB/Kafka errors.
- Product stock processing is transactional and idempotent; duplicate Kafka delivery or a crash at every step cannot lose or double-deduct stock.
- Cancellation, expiry, and late success always reach a durable terminal state, including regular-stock compensation.
- A 100%-Flash-Sale cart emits the same durable allocation confirmation and downstream order notification as the standalone path.
- A price increase returns 409 and creates no order until the customer accepts a new server quote.
- Campaign ending waits for pending mixed orders and exact ledger reconciliation before release.
- Tests cover multiple Flash Sale lines, mixed lines, concurrent checkout, duplicate events, late results, crashes, and end-campaign races.

Review verification performed: `go test ./services/order-service/... ./services/product-service/... ./pkg/kafka/...` and frontend `tsc --noEmit` passed. Those checks establish compilation and existing unit-test health only; they do not exercise the failure scenarios above. This review does not modify application code.
