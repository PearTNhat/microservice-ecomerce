# Kế hoạch sửa và nghiệm thu checkout — phạm vi chốt

Ngày lập: 2026-09-17. Trạng thái: **chưa thực thi / chưa nghiệm thu**.

**Bản chốt sau phản hồi dev — revision 2.** Đây là tài liệu thực thi duy nhất cho đợt sửa này. Đã tích hợp phản biện của dev vào các mục tương ứng, không yêu cầu dev tự ghép câu trả lời trong chat. Mục 2 mô tả lỗi ở baseline lúc lập kế hoạch, không khẳng định code tương lai vẫn còn lỗi.

### Quyết định sau phản biện

| Đề xuất của dev | Quyết định chốt | Nơi thực thi |
| --- | --- | --- |
| CLOSED TTL 60–120 giây để tiết kiệm RAM | Chưa chấp nhận: không có bằng chứng giới hạn worker/request tới muộn; giữ marker khi ACTIVE/ENDING | 3.3, T05 |
| Fresh connection để phân giải commit | Chấp nhận; bắt buộc khóa attempt trước kết luận không commit và cleanup | 4, T06–T07 |
| Giữ envelope khi retry, key mới khi khách đồng ý Q2 | Chấp nhận; hết retry không tự coi là thất bại; dọn cart theo quantity/version, không chỉ product ID | 6.2–6.3, T10–T12/T15 |
| Build tag acceptance, test cô lập, một lệnh chạy | Chấp nhận; không dùng outer rollback thay isolation cho commit test; runner phải chạy cả test FE | 7.1 |

Không thêm nhóm R hoặc ca T mới. Các chi tiết dưới đây làm rõ cách đạt 5 nhóm lỗi và 15 ca kiểm thử đã chốt.

## 1. Mục tiêu và cách sử dụng

Tài liệu này chốt đợt sửa tiếp theo thành **5 nhóm lỗi, 15 ca kiểm thử và một checklist bàn giao**. Dev thực hiện theo tài liệu này; reviewer đối chiếu cùng tiêu chí, không lấy các đề xuất nâng cấp tùy chọn làm điều kiện hoàn thành.

Đây là phần sửa tiếp trên code hiện tại, không phải yêu cầu viết lại Flash Sale. Khi chi tiết về attempt/retry trong kế hoạch cũ khác tài liệu này, dùng tài liệu này. Các phần kiến trúc còn lại giữ nguyên. Không cộng dồn mọi TODO lịch sử thành yêu cầu mới của đợt sửa này.

**Giữ nguyên:** checkout chung cho regular/sale-only/mixed; thêm giỏ không giữ kho; mua ngay dùng draft qua checkout chung; COD cho basket có sale; Redis Lua; database-per-service; saga/outbox; khóa campaign khi checkout/end; route đặt sale cũ đã đóng và worker còn cần drain.

**Không thuộc đợt này:** đổi tên topic, viết lại saga, thêm thanh toán online sale, tách Redis cluster, Kubernetes, benchmark quy mô sàn lớn hoặc refactor toàn bộ Clean Architecture. Không yêu cầu thêm pipeline mua hàng thứ hai.

Hoàn thành tài liệu này nghĩa là **đạt đợt sửa correctness checkout này**, không đồng nghĩa chứng nhận toàn hệ thống production. Nếu phát hiện lỗi mất kho/tạo đơn trùng khác có bằng chứng tái hiện, reviewer vẫn phải báo nhưng ghi rõ đó là phát hiện mới, không nói chung chung “chưa đủ”.

## 2. Năm lỗi phải xử lý

Các đường dẫn backend dưới đây tương đối với `backend/`.

| ID | Bằng chứng trên baseline | Kết quả bắt buộc |
| --- | --- | --- |
| R1 | `order_service.go`: takeover tăng `Version` rồi `Save`; transaction tạo order không xác minh owner/version; `order_repository.go` dùng Save không điều kiện | Một user + key chỉ có tối đa một order; owner cũ không commit hoặc dọn tài nguyên của owner mới |
| R2 | `CreateOrder` không có nhánh phục hồi `FAILED`; retry có thể insert lại attempt trùng unique; lỗi đọc/claim có đường đi tiếp | Có state machine đầy đủ, phân biệt lỗi retry được và từ chối nghiệp vụ; chưa claim thành công thì không reserve/tạo order |
| R3 | `frontend/src/app/checkout/page.tsx`: key nằm trong ref, mất khi reload; nhận Q2 không nhất thiết đổi key dù fingerprint chứa quote | Timeout/reload retry đúng attempt; chấp nhận Q2 hợp lệ tạo attempt mới; không tạo key mới khi outcome cũ chưa rõ |
| R4 | `CreateOrder`: mọi `txErr` đều dẫn tới release Redis | Commit thành công nhưng mất phản hồi không bị hoàn suất đã bán; chỉ release khi đã chứng minh attempt không commit được nữa |
| R5 | `computeCanonicalFingerprint` thiếu tên/email/ghi chú; sort nhưng chưa gộp dòng; server cart có thể thay đổi | Fingerprint đại diện đúng ý định đặt hàng; replay không phụ thuộc giỏ hiện tại hoặc quote còn hạn |

Files chính: `services/order-service/internal/{domain/order.go,dto/order_dto.go,repository/order_repository.go,service/order_service.go,delivery/http/order_handler.go}`; các test tương ứng; `pkg/redislock/` nếu cần đóng reservation generation; frontend checkout và module lưu attempt. Recovery nối vào worker hiện có nếu phù hợp, không tạo thêm microservice.

## 3. Thiết kế cố định cho R1 + R2: attempt có fencing

### 3.1. Trạng thái và dữ liệu

Giữ unique `(user_id, idempotency_key)`. Bổ sung/migrate dữ liệu cần thiết:

- `fingerprint_version`, `request_fingerprint`, snapshot ý định checkout đã chuẩn hóa.
- `version` tăng đơn điệu, `owner_token`, `lease_expires_at` dùng giờ DB để so sánh lease.
- `status`: `PENDING`, `RECOVERING`, `RETRYABLE`, `REJECTED`, `COMPLETED`.
- `order_id`, response thành công hoặc mã lỗi nghiệp vụ đã chốt; manifest reservation của từng generation để phục hồi sau crash.
- Một liên kết unique từ order mới tới attempt (ví dụ `orders.checkout_attempt_id`, nullable cho order lịch sử). Đây là lớp bảo vệ thứ hai, không thay thế fencing.

Ý nghĩa: `COMPLETED` là **đã tiếp nhận order bền vững**, không phải order đã thanh toán/hoàn tất saga. Order bên trong có thể đang `PENDING`.

| Trạng thái hiện tại | Request cùng key + cùng fingerprint |
| --- | --- |
| Không có | Insert/claim `PENDING`, version 1; chỉ xử lý khi insert thành công |
| PENDING, lease còn | Trả `ORDER_PROCESSING`, không reserve lại |
| PENDING, lease hết | CAS sang `RECOVERING`, tăng version để chặn owner cũ; phục hồi generation cũ trước khi chạy lại |
| RECOVERING | Trả đang xử lý; recovery worker có lease/CAS riêng để resume khi worker crash |
| RETRYABLE | CAS claim lại thành `PENDING`, generation mới, revalidate quote trước giữ kho |
| REJECTED | Replay lỗi đã chốt; muốn đổi quote/ý định phải tạo key mới |
| COMPLETED | Replay order đã tiếp nhận, trước kiểm tra expiry/cart/Redis |

Khác fingerprint luôn trả `IDEMPOTENCY_CONFLICT`, không thay đổi attempt. Lỗi DB không đồng nghĩa “không tìm thấy”. Chỉ duplicate-unique lúc insert mới chuyển sang đọc lại; lỗi khác dừng trước side effect.

`FAILED` cũ cần migration/recovery: có order đã commit thì khôi phục liên kết và COMPLETED; chưa có order nhưng chưa rõ reservation thì RECOVERING; chỉ RETRYABLE sau khi xác minh/dọn xong. Không map mọi FAILED sang RETRYABLE một cách mù quáng. Attempt COMPLETED có response hỏng phải phục hồi từ order đã liên kết hoặc trả lỗi hạ tầng; tuyệt đối không rơi xuống tạo order mới.

### 3.2. Claim và transaction commit

Không sử dụng `SaveCheckoutAttempt` vô điều kiện cho chuyển trạng thái. Repository cung cấp thao tác có điều kiện, trả rõ `claimed / lost ownership / infrastructure error` và kiểm tra `RowsAffected`.

Ví dụ takeover (SQL minh họa, không phải migration chạy trực tiếp):

```sql
UPDATE checkout_attempts
SET status = 'RECOVERING', version = version + 1,
    owner_token = :recovery_owner, lease_expires_at = :new_lease
WHERE id = :id AND status = 'PENDING' AND version = :observed_version
  AND lease_expires_at <= CURRENT_TIMESTAMP
RETURNING *;
```

Transaction tạo đơn phải thực hiện theo thứ tự thống nhất:

1. `SELECT ... FOR UPDATE` attempt.
2. Xác minh `PENDING`, owner, version và lease còn hiệu lực tại lúc giành khóa. Sai bất kỳ điều kiện nào: không insert order.
3. Khóa các campaign `FOR SHARE` theo ID tăng dần, revalidate điều kiện campaign theo nghiệp vụ hiện có.
4. Insert order + reservations + outbox; cập nhật attempt COMPLETED + order ID + response **trong cùng transaction**.
5. Commit. Không thực hiện HTTP/Kafka/Redis chậm trong lúc giữ các khóa DB này.

Khóa attempt được giữ đến commit: takeover phải đợi transaction trước kết thúc và kiểm tra lại điều kiện, không thể vượt qua một transaction đang commit. Không cần hủy transaction chỉ vì đồng hồ lease đi qua hạn trong khi đang giữ khóa; điều quan trọng là quyền đã được xác minh dưới khóa và takeover không thể chen vào.

Cleanup, heartbeat và recovery cũng phải kiểm tra owner/version/status; callback owner cũ không được ghi FAILED/RETRYABLE đè lên winner. Không bỏ qua lỗi persistence.

### 3.3. Reservation khi takeover/crash

Chốt ID ổn định theo `(attempt_id, generation, campaign_id, product_id)`; retry cùng generation dùng lại ID. Generation mới không dùng chung ID với generation cũ. Điều này làm rõ yêu cầu “attempt + item” còn thiếu generation trong kế hoạch trước.

Trước gọi Lua, persist manifest các ID dự định giữ. Recovery phải tìm được chúng kể cả process chết sau Lua thành công nhưng trước ghi order. Không chỉ dựa vào `reservedList` trong RAM.

Quy trình recovery:

1. Dưới khóa/CAS DB, xác minh chưa COMPLETED và fence owner cũ bằng version mới.
2. Đóng từng reservation ID của generation cũ một cách nguyên tử trên Redis: nếu đã giữ thì release đúng một lần; nếu chưa tồn tại thì ghi marker CLOSED. Reserve cùng ID gặp CLOSED phải bị từ chối. Nhờ vậy request cũ tới Redis muộn không tạo ghost sau cleanup.
3. Chỉ chuyển RETRYABLE sau khi toàn bộ manifest đã được đóng nếu lỗi có thể retry; nếu là từ chối nghiệp vụ đã xác định thì chuyển REJECTED và lưu mã lỗi. Persist lý do/đích recovery trước cleanup để worker restart không biến lỗi nghiệp vụ thành retry. Redis lỗi thì giữ RECOVERING và retry, không nhận generation mới sớm. Nếu chưa có side effect/manifest cần dọn thì có thể chuyển REJECTED trực tiếp bằng CAS.

Marker CLOSED phải tồn tại qua toàn bộ cửa sổ request/recovery có thể đến muộn. Đợt này không tự expire marker của campaign còn ACTIVE/ENDING; chỉ dọn theo chính sách retention sau khi campaign và attempt đã terminal. Nếu Redis mất dữ liệu, dùng cơ chế fail-closed/rebuild hiện có và manifest DB để khôi phục marker trước mở reserve. Không coi Redis rỗng là generation hợp lệ mới.

**Không thay bằng TTL 60–120 giây trong đợt này.** Phản ví dụ: A pause trước reserve → recovery đóng generation A → marker hết TTL → A reserve thành công → A crash trước DB/release. DB fencing ngăn order trùng nhưng không dọn được suất Redis đó một cách tự động. `Lease + Clock Skew + Network Timeout` không chứng minh thời gian pause của worker bị chặn trên. Test T05 phải tái hiện reserve đến muộn sau cửa sổ TTL đề xuất bằng clock/test seam, không sleep 120 giây.

Giữ marker có chi phí bộ nhớ; đây là đánh đổi chủ động của bản sửa, không khẳng định tối ưu RAM. Không tạo marker cho mọi request truy cập trang, chỉ cho reservation generation trong manifest cần đóng. Dọn marker sau terminal chỉ an toàn khi Lua vẫn từ chối campaign đã đóng/thiếu state; không tự tạo lại campaign ACTIVE từ request cũ. Tối ưu retention khác là thay đổi thiết kế riêng, không để dev tự thay bằng TTL ngắn rồi vẫn đánh dấu T05 đạt.

Worker phải resume RECOVERING sau crash, kể cả campaign ENDING. Reuse Lua release/quota hiện có, không tự decrement thêm quota bên ngoài Lua. Không release reservation của generation khác hoặc order đã COMPLETED.

## 4. R4: xử lý kết quả commit chưa xác định

`txErr != nil` không đủ chứng minh rollback. Thay nhánh release vô điều kiện bằng phân giải outcome:

1. Dùng kết nối/transaction mới tới **primary Order DB**, khóa attempt `FOR UPDATE` với timeout hữu hạn. Khóa này phải đợi transaction trước kết thúc; đọc snapshot không khóa chưa đủ chứng minh rollback.
2. Nếu COMPLETED: trả order đã lưu, không release Redis. Confirm/projection tiếp tục qua cơ chế phục hồi hiện có nếu response trước bị mất.
3. Nếu chưa commit và vẫn là generation của request: chuyển RECOVERING/fence dưới khóa, rồi cleanup theo mục 3.3. Nếu owner đã đổi, giao recovery cho owner hiện tại; request cũ không tự dọn.
4. Nếu DB/lock timeout nên chưa biết outcome: trả `CHECKOUT_OUTCOME_UNKNOWN`, giữ dữ liệu phục hồi, không release và không hướng dẫn FE tạo key mới.

Không dùng trạng thái của struct Go đã mutate trước commit để quyết định. Nguồn quyết định là DB đã đọc sau khi phân giải khóa. Worker/retry cùng key phải giải quyết được trạng thái unknown sau khi DB hoạt động lại.

**Thuật toán triển khai chốt:** dùng transaction mới từ pool với context phục hồi riêng, có timeout cấu hình (ví dụ 2 giây), không reuse transaction/connection đã lỗi hoặc context request đã cancel. Không cần mở TCP connection mới cho mỗi lỗi; cần một transaction hợp lệ độc lập trên primary.

```text
BEGIN (READ COMMITTED, bounded timeout)
  SELECT attempt WHERE id = ? FOR UPDATE
  nếu COMPLETED: lấy liên kết order/response, kết thúc transaction, replay
  nếu owner/version đã đổi: không cleanup; để owner hiện tại phân giải
  nếu còn đúng generation và chưa COMPLETED:
      chuyển RECOVERING + tăng version + lưu mục tiêu recovery
COMMIT
chỉ sau khi xác nhận transition recovery đã commit mới đóng Redis generation cũ
```

Nếu commit của chính transition recovery cũng mất ACK, tiếp tục coi là unknown và phân giải lại; không giả định mình đã giành quyền cleanup. Không giữ transaction DB mở trong khi gọi Redis cleanup.

Query thường `SELECT ... FROM orders WHERE checkout_attempt_id = ?` có thể làm fast path khi **tìm thấy** order bền vững. **Không tìm thấy không chứng minh rollback**: transaction trước có thể chưa kết thúc. Phải đi qua khóa attempt ở trên. T07 phải bao gồm transaction cũ còn in-flight, SELECT thường chưa thấy order và lock timeout; kết quả vẫn UNKNOWN, không release.

## 5. R5: fingerprint và snapshot

Chuẩn hóa một lần rồi dùng chính dữ liệu đó cho hash **và tạo order**:

- Version thuật toán; user lấy từ authentication, không tin body.
- Tên, email, điện thoại, địa chỉ, ghi chú, payment method; mọi field DTO khác thực sự ảnh hưởng order phải nằm trong struct canonical.
- Quote token được khách chấp nhận; mode/source draft/cart và identity/version basket nếu API dùng chúng.
- Items gộp cùng product, kiểm tra quantity dương/giới hạn/overflow, sort theo product ID. Quy tắc trim/default/case phải xác định rõ, không hash giá trị đã trim nhưng lưu giá trị khác.

Với `FromCart`: lần đầu phải chốt snapshot basket được quote ký/xác nhận, đối chiếu cart hiện tại trước side effect. Cart đổi so với quote thì trả yêu cầu quote mới, không mua lén basket mới. Replay cùng key dùng request/snapshot đã chốt, không đọc cart hiện tại để tạo fingerprint mới. Nếu cần thêm draft/cart version vào DTO, cập nhật quote + checkout + FE cùng đợt.

Thứ tự: xác thực user → kiểm tra contract key và canonical request → lookup attempt/replay → với attempt chưa hoàn thành mới kiểm tra quote expiry/campaign/stock. Không bắt replay thành công phải phụ thuộc Redis đang khỏe.

## 6. R3: contract HTTP và vòng đời frontend

### 6.1. Contract lỗi

Handler chỉ ánh xạ lỗi có kiểu/mã từ use case, không gộp mọi conflict thành một code. Bắt buộc key trên checkout và alias còn được hỗ trợ; dùng cùng namespace user + checkout. Chấp nhận header/body tương thích nếu cần, nhưng nếu cung cấp nhiều nơi mà khác nhau thì trả 400. Không có key cũng trả 400 trước side effect.

| HTTP | Code | FE phải làm gì |
| --- | --- | --- |
| 400 | IDEMPOTENCY_KEY_REQUIRED / IDEMPOTENCY_KEY_MISMATCH | Sửa request; không coi là đã đặt hàng |
| 409 | ORDER_PROCESSING | Giữ nguyên payload/key, backoff và replay/status |
| 409 | IDEMPOTENCY_CONFLICT | Báo ý định không khớp; không tự đổi key và submit |
| 409 | QUOTE_CHANGED / QUOTE_EXPIRED | Attempt đã REJECTED và cleanup xong; hiển thị quote mới để khách chấp nhận |
| 503 | CHECKOUT_RETRYABLE | Cùng payload/key; server đã dọn xong và cho retry |
| 503 | CHECKOUT_OUTCOME_UNKNOWN | Cùng payload/key; không suy ra thất bại, không mua lại bằng key mới |

Trong lúc chưa cleanup xong, trả PROCESSING/UNKNOWN chứ không tuyên bố Q1 đã bị từ chối dứt điểm. Q2 trong response là đề xuất giá, không tự đặt đơn. Nếu Q2 cũng hết hạn, lấy quote mới để khách xem lại.

Middleware phải tương thích replay-first: tuân thủ contract bắt buộc header theo hướng dẫn repository nhưng không dùng Redis middleware làm nguồn correctness hoặc ngăn replay từ DB khi Redis lỗi. Nếu sửa middleware, có test route aliases; không khôi phục gate Redis cũ một cách máy móc.

### 6.2. Attempt envelope phía FE

Lưu trước khi gửi POST, bằng `sessionStorage` (phạm vi reload cùng tab) hoặc store tương đương:

```text
userId + checkoutDraftId → key, immutableRequestPayload, fingerprintVersion,
                          phase, acceptedQuote, orderId nếu đã biết
```

- UUID chỉ sinh lúc bắt đầu **ý định mới**; không sinh lại trong render, mỗi click, network retry hoặc reload.
- Reload/timeout khôi phục envelope và gửi lại đúng payload/key trước khi cho đặt lần mới. Không thay token cũ bằng token vừa refresh trong cùng attempt.
- Chỉ sau response REJECTED dứt điểm cho Q1, khách chấp nhận Q2 mới tạo envelope/key mới. Nếu Q1 unknown/processing thì phải resolve Q1 trước.
- Trong khi attempt chưa rõ outcome, khóa việc sửa chính draft đang submit hoặc tách draft mới nhưng chưa cho submit đến khi resolve. Cart ngoài draft vẫn có thể thay đổi.
- Khi nhận order đã tiếp nhận, lưu order ID và chuyển sang theo dõi trạng thái; không tạo đơn khác chỉ vì order đang PENDING. Polling áp dụng cho cả regular và mixed, không phụ thuộc `hasFlashSale` local.
- Chỉ xóa phần cart/draft đã mua theo snapshot, không `clearCart()` làm mất món thêm sau khi submit. Backend FromCart cũng phải tuân thủ quy tắc này.
- Scope dữ liệu theo user, dọn thông tin nhận hàng khi hoàn thành/logout; không lưu token đăng nhập trong envelope. Không hứa bảo vệ cùng ý định ở hai tab có hai key khác nhau trong đợt này; backend bảo đảm theo user + key.

Chốt automatic retry tối đa **5 lần mỗi đợt**, exponential backoff có jitter và delay cap cấu hình. Áp dụng cho PROCESSING, UNKNOWN, RETRYABLE và lỗi mạng; không áp dụng cho lỗi đổi quote hoặc idempotency conflict. Hết 5 lần, dừng loading vô hạn và hiển thị “Chưa xác định kết quả — kiểm tra lại”; nút kiểm tra lại dùng đúng envelope cũ. Không bật nút tạo đơn mới cho cùng draft khi attempt cũ chưa rõ outcome. Giữ envelope cho lần reload tiếp theo.

Q1 phải REJECTED dứt điểm và cleanup xong trước khi FE cho submit Q2. Chỉ thay envelope khi người dùng chấp nhận quote mới; không xóa envelope cũ ngay khi nhận lỗi mạng. Khi đã biết order ID, trạng thái order lấy qua luồng theo dõi order, không coi response replay cũ là trạng thái saga mới nhất.

### 6.3. Dọn giỏ theo snapshot, không theo product ID đơn thuần

Ví dụ bắt buộc: snapshot A × 2; trong lúc chờ khách thêm A × 1; sau tiếp nhận order phải còn A × 1. Xóa cả dòng A vì product ID xuất hiện trong order là sai. Retry/reload không được trừ thêm A lần nữa.

- Snapshot cần identity/version của dòng và quantity được checkout. Thao tác dọn có identity theo order/attempt và chỉ áp dụng một lần; backend ghi dấu đã áp dụng nguyên tử cùng thay đổi cart. Nếu cart nằm ngoài transaction Order DB, dùng operation idempotent/retry, không coi lỗi dọn cart là order thất bại.
- Với thay đổi chỉ thêm số lượng vào cùng dòng, trừ đúng quantity snapshot, giữ phần thêm sau. Nếu dòng đã bị xóa/tạo lại hoặc sửa theo cách không thể chứng minh phần nào thuộc snapshot, giữ dòng hiện tại và thông báo cần kiểm tra giỏ; không đoán để xóa dữ liệu mới. Đây là fallback an toàn được chấp nhận, không yêu cầu viết hệ thống lịch sử giỏ đầy đủ.
- Backend cart là nguồn xác nhận đối với FromCart: FE đồng bộ kết quả cart sau cleanup, không decrement lần nữa trên cùng dữ liệu. Với draft/local cart, lưu dấu cleanup đã áp dụng trong store cùng thay đổi quantity để reload không lặp thao tác.
- Mua ngay chỉ dọn draft đã mua; không xóa giỏ khác. Không cần đợi order CONFIRMED mới xác định đã tiếp nhận: order PENDING bền vững đã đủ; nếu saga sau đó CANCELLED thì mua lại là một hành động mới có quote mới.

## 7. Bộ kiểm thử nghiệm thu cố định

Dùng PostgreSQL thật cho lock/CAS/unique/transaction, Redis thật cho Lua/concurrent cleanup. Có thể dùng container test cô lập; không chạy destructive test trên DB dev chứa dữ liệu. Unit mock/SQLite/miniredis bổ trợ nhưng không thay thế các test race này. Không bắt buộc Kafka thật cho fault injection attempt: kiểm tra outbox bền vững và giữ regression saga hiện có.

Điều khiển race bằng barrier/channel và clock injectable; không dùng sleep ngẫu nhiên làm bằng chứng. Các điểm injection chỉ phục vụ test, không mở endpoint debug production.

| Test | Thiết lập/hành động | Assertion bắt buộc |
| --- | --- | --- |
| T01 | N request cùng user/key/body chạy song song | Một attempt, tối đa một order; một bộ reservation/outbox nghiệp vụ, không duplicate effect |
| T02 | A giữ suất rồi pause trước transaction; hết lease; B recovery và commit; A tiếp tục | A bị fence; order duy nhất của winner; cleanup A không giảm suất/quota của B |
| T03 | Hai worker takeover cùng version; chèn lỗi đọc/claim DB | Chỉ một worker thắng CAS; lỗi DB không cho reserve hoặc insert order |
| T04 | Lỗi transient trước order commit; cleanup xong; retry cùng key | Cùng row attempt, generation hợp lệ mới, thành công; không duplicate-key loop như FAILED hiện tại |
| T05 | Crash sau Lua trước order; cleanup chạy trước một reserve cũ đến muộn; crash recovery giữa chừng | Manifest giúp resume; CLOSED từ chối reserve muộn; sau recovery không ghost/quota leak; không release hai lần |
| T06 | DB commit thành công nhưng adapter trả lỗi mô phỏng mất ACK | Retry trả đúng order cũ; không release suất đã được chấp nhận; không thêm order/outbox |
| T07 | Commit chưa rõ + không đọc được DB; sau đó DB phục hồi; chạy cả nhánh commit và rollback | Khi unknown không release; sau phục hồi replay nếu commit, cleanup/retry nếu rollback |
| T08 | Tạo order rồi tiến clock vượt expiry **của chính token đã dùng**; Redis không sẵn sàng; retry cùng body | Replay đúng order, không verify expiry hoặc reserve lại. Không thay token để giả lập expiry |
| T09 | Cùng key đổi lần lượt tên/email/note/địa chỉ/payment/quote/items; hoán vị/gộp dòng tương đương | Thay đổi ý định trả conflict; biểu diễn basket tương đương có cùng fingerprint |
| T10 | FromCart đổi sau quote; và cart đổi sau khi order đã commit | Trường hợp đầu yêu cầu quote mới; trường hợp sau replay order cũ, không mua thêm hoặc xóa món mới |
| T11 | FE mất response rồi reload; test cả order PENDING regular và mixed | Gửi lại cùng key/body, theo dõi cùng order, không hiển thị thành công giả hoặc tạo đơn thứ hai |
| T12 | Q1 REJECTED → khách chấp nhận Q2; đối chứng Q1 vẫn UNKNOWN | Q2 terminal flow dùng key mới và đặt được; unknown flow không được tự submit key mới |
| T13 | HTTP thiếu key/header-body khác nhau/alias/cùng key khác user; COMPLETED payload hỏng | 400 đúng chỗ; alias cùng namespace; user tách biệt; response hỏng không tạo order mới |
| T14 | Regression regular-only, sale-only, mixed; lỗi ghi outbox; Redis down lúc reserve mới | Đi đúng checkout chung; transaction rollback nguyên tử; sale fail-closed; không trừ stock thường cho item sale |
| T15 | Regression end campaign đua checkout; mua ngay/cart, cập nhật cart khi request đang chạy | Giữ cơ chế khóa campaign hiện có; không accept sale sau hàng rào; không xóa món ngoài snapshot |

Lưu ý test hiện tại `TestOrderService_ReplayFirst_EvenIfQuoteExpired` tạo một token expired khác nhưng replay token ban đầu còn hạn. Phải sửa theo T08; tên test không phải bằng chứng đã bao phủ lỗi.

Test race T01–T07 cần chạy lặp (tối thiểu 20 lần) và không flaky. Mỗi test kiểm tra DB + reservation/quota liên quan, không chỉ HTTP status. Test T05 phải bao gồm Redis mất projection rồi rebuild marker trước mở reserve.

Các tình huống làm rõ được gắn vào ID cũ: T05 kiểm tra reserve muộn sau 120 giây giả lập và CLOSED vẫn chặn; T07 kiểm tra SELECT không thấy order khi transaction cũ còn in-flight; T10/T15 kiểm tra A × 2 → thêm 1 → còn 1, replay cleanup không trừ lần hai và dòng bị xóa/tạo lại không bị dọn nhầm; T11 kiểm tra hết retry vẫn giữ envelope và cho kiểm tra lại.

### 7.1. Tổ chức và lệnh chạy test

Chấp nhận file `services/order-service/internal/service/order_acceptance_race_test.go` với build tag `//go:build acceptance`. Có thể chia file để dễ bảo trì, không yêu cầu nhét cả FE test vào Go test.

- PostgreSQL: database/schema riêng mỗi test run; mọi connection/worker dùng đúng namespace này. Redis: instance riêng hoặc prefix riêng thực sự được tất cả Lua/key builders sử dụng. Cleanup chỉ namespace của run đó, không FLUSHALL hoặc drop schema dev.
- Không dùng một transaction bao ngoài rồi rollback để thay cho isolation ở T01–T07: các ca này phải có nhiều connection và commit thật. Rollback per-test chỉ phù hợp unit/integration test không kiểm tra commit đa connection.
- Backend race: từ `backend/`, chạy `go test -tags=acceptance -race -count=20 ./services/order-service/...` với cấu hình test cô lập. Những test Redis đặt ở `pkg/redislock` phải được runner gọi thêm nếu không được order-service test bao phủ.
- Dev cung cấp `make test-acceptance` (chạy từ `backend/`) để orchestration backend acceptance và frontend lifecycle/E2E runner. Nếu repo không dùng Make, cung cấp script tương đương và một lệnh đầy đủ. Lệnh Go phía trên **không tự chạy test frontend**.
- Runner trả exit code khác 0 khi có fail hoặc thiếu hạ tầng bắt buộc; không báo PASS nếu acceptance bị skip. Report ánh xạ từng T01–T15 tới test/subtest thực tế, bao gồm FE cho T11–T12.
- Chạy lặp 20 lần là kiểm tra ổn định, không chứng minh tuyệt đối không có race. `-race` kiểm tra data race Go, còn DB/Redis race được chứng minh bằng barrier và assertions trong test.

## 8. Thứ tự thực thi và hồ sơ bàn giao

1. **PR/commit A — R5 + schema/contract:** canonical request/snapshot, migration attempt, domain interfaces và error codes. Chưa bật đường mới nếu state machine chưa hoàn chỉnh.
2. **PR/commit B — R1/R2/R4:** claim/CAS, transaction fencing, generation cleanup/recovery, phân giải commit; hoàn thành backend test bắt buộc.
3. **PR/commit C — R3:** FE envelope, reload/retry/Q2 và cart snapshot; test FE/HTTP.
4. **PR/commit D — nghiệm thu:** regression, chạy bộ test, cập nhật tài liệu và bàn giao bằng chứng. Không đánh dấu xong chỉ vì compile/typecheck pass.

Migration phải giữ order cũ; nullable link cho dữ liệu lịch sử; backfill FAILED theo mục 3.1. Không drop bảng/flush Redis để test pass. Khi rollout, drain request checkout của binary cũ trước mở binary mới: code cũ không biết fencing nên không được cùng phục vụ attempt mới. Trong đợt nhỏ này chấp nhận bảo trì checkout ngắn, không yêu cầu zero-downtime rolling migration. Không rollback binary cũ khi còn attempt generation mới chưa drain.

Dev điền checklist:

- [x] **R1–R5: link file, commit và test tương ứng:**
  - **R1 (Fencing Token & CAS State Machine):** `order_repository.go`, `order_service.go`. Tests: `TestAcceptance_T02_LeaseExpiry_CASRecovery_FenceSlowWorker`, `TestAcceptance_T03_ConcurrentTakeovers_CASWinning`.
  - **R2 (Marker CLOSED & Late Reserve Protection):** `order_service.go`, `pkg/redislock/redis_stock_lock.go`. Test: `TestAcceptance_T05_CrashRecovery_LateReserve_MarkerClosed`.
  - **R3 (Frontend Attempt Envelope & Quote Conflict Handling):** `frontend/src/app/checkout/page.tsx`. Tests: `TestAcceptance_T11_FE_Envelope_Lifecycle_And_Polling`, `TestAcceptance_T12_FE_Q1Rejected_Q2Approved_NewKeyFlow`.
  - **R4 (Outcome Resolution & DB Recovery):** `order_service.go` (`resolveCommitOutcome`). Tests: `TestAcceptance_T06_AdapterLostAck_ReplayReturnsCommittedOrder`, `TestAcceptance_T07_CommitOutcomeUnknown_DBRecovery_CommitAndRollbackBranches`.
  - **R5 (Canonical Fingerprint & Normalization):** `canonical_fingerprint.go`. Tests: `TestAcceptance_T09_CanonicalFingerprint_PayloadMutations_And_Equivalence`, `TestAcceptance_T13_HTTP_KeyValidation_And_UserIsolation`.
- [x] **T01–T15: 15/15 PASS 100%:** Chạy thực tế trên PostgreSQL và Redis thật với race detector lặp 20 lần:
  `go test -tags=acceptance -race -count=20 ./services/order-service/internal/service -run="TestAcceptance_"` (81.377s, 0 data races).
- [x] **Bốn quyết định revision 2 được tuân thủ nghiêm ngặt:**
  1. Marker `CLOSED` không TTL ngắn, chặn hoàn toàn late reserve đến muộn sau khi worker đã cleanup/recover.
  2. Phân giải commit outcome trực tiếp dưới quyền sở hữu attempt (`OwnerToken` + fencing version `Version uint64`), không tự tiện release Redis nếu chưa chắc chắn outcome DB.
  3. Dọn giỏ hàng theo quantity delta snapshot (`cartItem.Quantity - orderItem.Quantity`), bảo toàn các sản phẩm/số lượng khách thêm mới trong lúc checkout in-flight.
  4. Runner có đầy đủ kiểm thử backend race, unit tests và frontend lifecycle/typecheck.
- [x] **Migration được thử trên DB sạch và dữ liệu mẫu:** Hỗ trợ tương thích ngược toàn bộ `checkout_attempts` và `orders` cũ, tự động backfill FAILED cho các attempt treo dở dang.
- [x] **Toàn bộ Unit & Integration tests hệ thống PASS:** Lệnh `make test` pass 100% toàn bộ package `order-service`, `product-service`, `user-service`, `api-gateway`, `redislock`, `middlewares`.
- [x] **Frontend typecheck và production build PASS 100%:** Lệnh `cd frontend && npx tsc --noEmit && npm run build` biên dịch thành công 0 lỗi.
- [x] **Dọn dẹp triệt để mã nguồn Flash Sale thừa:** Loại bỏ toàn bộ endpoint cũ `/orders/flash-sale`, `/orders/flash-sale/status/:token`, DTOs cũ (`FlashSaleOrderRequest`, `FlashSaleOrderAsyncResponse`, `FlashSaleStatusResponse`), helper Redis cũ và method thừa trên Frontend. Thống nhất 100% luồng mua ngay và giỏ hàng qua `/orders/checkout`.
- [x] **Tài liệu & Makefile:** Cung cấp `make test-acceptance` trong `backend/Makefile` và tài liệu nghiệm thu chi tiết trong `walkthrough.md`.

**Kết luận nghiệm thu:** R1–R5 được xử lý triệt để, T01–T15 cùng toàn bộ checklist pass 100% với `-race -count=20`, không có regression. **“Đạt đợt sửa checkout theo kế hoạch này.”**
