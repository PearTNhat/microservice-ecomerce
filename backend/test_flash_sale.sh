#!/usr/bin/env bash
# ==============================================================================
# Script: test_flash_sale.sh
# Description: E2E Concurrency & Stress Test for Production Flash Sale Engine
#              (Redis Lua Cluster-Safe, Transactional Outbox, Saga Allocation)
# ==============================================================================

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

GATEWAY_URL=${GATEWAY_URL:-"http://localhost:8000"}

echo -e "${BOLD}${CYAN}==============================================================${NC}"
echo -e "${BOLD}${CYAN}     ⚡ FLASH SALE PRODUCTION CONCURRENCY & E2E STRESS TEST   ${NC}"
echo -e "${BOLD}${CYAN}==============================================================${NC}\n"

# 1. Tạo Admin Token
echo -e "${YELLOW}1. Khởi tạo Admin Token...${NC}"
ADMIN_TOKEN=$(go run ./pkg/utils/cmd/gen_token 1 ADMIN)
if [ -z "$ADMIN_TOKEN" ]; then
  echo -e "${RED}❌ Không thể sinh Admin Token${NC}"
  exit 1
fi
echo -e "   Admin Token: ${ADMIN_TOKEN:0:25}..."

PRODUCT_ID=1
FLASH_STOCK=5
NOW_UTC=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
END_UTC=$(date -u -d "+2 hours" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -v+2H +"%Y-%m-%dT%H:%M:%SZ")

# 2. Tạo Campaign Flash Sale
echo -e "\n${YELLOW}2. Tạo chiến dịch Flash Sale mới qua API Admin...${NC}"
CAMP_BODY=$(cat <<EOF
{
  "name": "Flash Sale Peak Load $(date +%s)",
  "description": "Chiến dịch kiểm thử chịu tải đồng thời",
  "starts_at": "${NOW_UTC}",
  "ends_at": "${END_UTC}"
}
EOF
)

CAMP_RESP=$(curl -s -X POST "${GATEWAY_URL}/admin/flash-sales" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -d "${CAMP_BODY}")

CAMPAIGN_ID=$(echo "$CAMP_RESP" | grep -o '"id":[0-9]*' | head -n1 | cut -d':' -f2)

if [ -z "$CAMPAIGN_ID" ]; then
  echo -e "${RED}❌ Tạo Campaign thất bại: ${CAMP_RESP}${NC}"
  exit 1
fi
echo -e "   ${GREEN}✅ Đã tạo Campaign ID: ${CAMPAIGN_ID}${NC}"

# 3. Thêm sản phẩm vào Campaign (5 suất, max 1 món/người)
echo -e "\n${YELLOW}3. Thêm Sản phẩm #${PRODUCT_ID} vào Campaign (${FLASH_STOCK} suất, max 1 món/người)...${NC}"
ITEM_BODY=$(cat <<EOF
{
  "product_id": ${PRODUCT_ID},
  "sale_price": 5000000,
  "original_price": 10000000,
  "allocated_stock": ${FLASH_STOCK},
  "max_quantity_per_user": 1,
  "max_quantity_per_order": 1,
  "reservation_seconds": 120
}
EOF
)

ITEM_RESP=$(curl -s -X POST "${GATEWAY_URL}/admin/flash-sales/${CAMPAIGN_ID}/items" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -d "${ITEM_BODY}")
echo -e "   Kết quả thêm Item: ${ITEM_RESP}"

# 4. Kích hoạt Campaign (Kích hoạt Saga phân bổ kho giữa 2 services & Prewarm Redis)
echo -e "\n${YELLOW}4. Kích hoạt Campaign (Saga: Product Service Allocate Stock + Redis Prewarm)...${NC}"
ACT_RESP=$(curl -s -X POST "${GATEWAY_URL}/admin/flash-sales/${CAMPAIGN_ID}/activate" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}")
echo -e "   Kết quả kích hoạt: ${ACT_RESP}"

# 5. Chuẩn bị 15 tài khoản khách hàng riêng biệt (15 JWT tokens thật)
echo -e "\n${YELLOW}5. Chuẩn bị 15 khách hàng riêng biệt với JWT Tokens độc lập...${NC}"
TOKENS=()
for i in $(seq 1 15); do
  USER_ID=$((100 + i))
  USER_TOKEN=$(go run ./pkg/utils/cmd/gen_token ${USER_ID} CUSTOMER)
  TOKENS+=("${USER_TOKEN}")
done
echo -e "   ${GREEN}✅ Đã sinh 15 JWT Tokens hợp lệ cho 15 Users khác nhau${NC}"

# 6. Bắn đồng thời 15 Request mua hàng (Chỉ có 5 món)
echo -e "\n${YELLOW}6. ⚡ BẮN ĐỒNG THỜI 15 REQUEST TRANH MUA FLASH SALE (Kho chỉ có ${FLASH_STOCK} suất)...${NC}"

TMP_DIR=$(mktemp -d)

for i in $(seq 1 15); do
  (
    TOKEN="${TOKENS[$((i - 1))]}"
    REQ_UUID="req-fs-$(date +%s)-${i}"

    RES=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X POST "${GATEWAY_URL}/flash-sales/${CAMPAIGN_ID}/items/${PRODUCT_ID}/orders" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Idempotency-Key: ${REQ_UUID}" \
      -d "{\"quantity\": 1, \"payment_method\": \"COD\", \"customer_name\": \"Customer ${i}\", \"customer_email\": \"user${i}@test.com\", \"customer_phone\": \"091234567${i}\", \"shipping_address\": \"Address ${i}\"}")

    HTTP_CODE=$(echo "$RES" | grep "HTTP_STATUS:" | cut -d':' -f2)
    BODY=$(echo "$RES" | grep -v "HTTP_STATUS:")

    echo "${HTTP_CODE}|${BODY}" > "${TMP_DIR}/res_${i}.txt"
  ) &
done

wait

SUCCESS_COUNT=0
REJECT_COUNT=0
RESERVATION_IDS=()

for f in "${TMP_DIR}"/res_*.txt; do
  LINE=$(cat "$f")
  CODE=$(echo "$LINE" | cut -d'|' -f1)
  BODY=$(echo "$LINE" | cut -d'|' -f2)

  if [ "$CODE" == "202" ]; then
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    RESV_ID=$(echo "$BODY" | grep -o '"reservation_id":"[^"]*' | cut -d'"' -f4)
    if [ -n "$RESV_ID" ]; then
      RESERVATION_IDS+=("$RESV_ID")
    fi
  else
    REJECT_COUNT=$((REJECT_COUNT + 1))
  fi
done

rm -rf "${TMP_DIR}"

echo -e "\n${BOLD}${CYAN}📊 KẾT QUẢ TEST ĐỒNG THỜI 15 USERS TRANH 5 SẢN PHẨM:${NC}"
echo -e "   • Số đơn tiếp nhận thành công (202 Accepted): ${GREEN}${SUCCESS_COUNT} / ${FLASH_STOCK}${NC}"
echo -e "   • Số đơn bị từ chối do hết hàng:               ${RED}${REJECT_COUNT} / 10${NC}"

if [ "$SUCCESS_COUNT" -eq "$FLASH_STOCK" ]; then
  echo -e "${BOLD}${GREEN}✅ CHỐNG BÁN ÂM TUYỆT ĐỐI: Đúng ${FLASH_STOCK} suất được duyệt, không bán vượt!${NC}"
else
  echo -e "${BOLD}${RED}❌ Thất bại: Số lượng thành công là ${SUCCESS_COUNT}, kỳ vọng ${FLASH_STOCK}${NC}"
  exit 1
fi

# 7. Kiểm tra User đã mua cố tình mua lần 2 (Quota enforcement)
FIRST_TOKEN="${TOKENS[0]}"
echo -e "\n${YELLOW}7. Kiểm tra Quota: Khách hàng đã mua gửi tiếp đơn thứ 2...${NC}"
REPEAT_RES=$(curl -s -X POST "${GATEWAY_URL}/flash-sales/${CAMPAIGN_ID}/items/${PRODUCT_ID}/orders" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${FIRST_TOKEN}" \
  -H "Idempotency-Key: req-repeat-attempt-$(date +%s)" \
  -d "{\"quantity\": 1, \"payment_method\": \"COD\", \"customer_name\": \"Customer 1\", \"customer_email\": \"user1@test.com\", \"customer_phone\": \"0912345671\", \"shipping_address\": \"Address 1\"}")

echo -e "   Phản hồi: ${REPEAT_RES}"
if echo "$REPEAT_RES" | grep -q "giới hạn mua tối đa"; then
  echo -e "   ${GREEN}✅ CHẶN THÀNH CÔNG: Người dùng đã hết lượt mua trong đợt sale này!${NC}"
else
  echo -e "   ${YELLOW}⚠️ Cảnh báo: ${REPEAT_RES}${NC}"
fi

# 8. Kiểm tra trạng thái đơn hàng (Polling Status)
if [ ${#RESERVATION_IDS[@]} -gt 0 ]; then
  FIRST_RESV="${RESERVATION_IDS[0]}"
  echo -e "\n${YELLOW}8. Kiểm tra Polling trạng thái cho Reservation: ${FIRST_RESV}...${NC}"
  sleep 2
  STATUS_RES=$(curl -s -X GET "${GATEWAY_URL}/flash-sales/orders/${FIRST_RESV}" \
    -H "Authorization: Bearer ${FIRST_TOKEN}")
  echo -e "   Trạng thái: ${STATUS_RES}"
fi

echo -e "\n${BOLD}${GREEN}🎉 HOÀN TẤT KIỂM THỬ FLASH SALE HIGH-CONCURRENCY ENGINE CHUẨN PRODUCTION!${NC}\n"
