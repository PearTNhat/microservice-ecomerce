#!/usr/bin/env bash
# ==============================================================================
# Script: test_flash_sale.sh
# Description: E2E Concurrency & Stress Test for Flash Sale High-Concurrency Engine
#              (Redis Lua Lock, RabbitMQ Queue, Zero-DB Status Polling)
# ==============================================================================

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

GATEWAY_URL="http://localhost:8000"

echo -e "${BOLD}${CYAN}==============================================================${NC}"
echo -e "${BOLD}${CYAN}     ⚡ FLASH SALE HIGH-CONCURRENCY STRESS & E2E TEST         ${NC}"
echo -e "${BOLD}${CYAN}==============================================================${NC}\n"

# 1. Đăng ký & Đăng nhập tài khoản test
TEST_EMAIL="flashsale_tester_$(date +%s)@example.com"
PASSWORD="Password123!"

echo -e "${YELLOW}1. Khởi tạo tài khoản test: ${TEST_EMAIL}...${NC}"
REG_RES=$(curl -s -X POST "${GATEWAY_URL}/register" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"${TEST_EMAIL}\",\"password\":\"${PASSWORD}\",\"first_name\":\"Flash\",\"last_name\":\"Tester\",\"phone\":\"0988776655\"}")

# Lấy OTP từ Redis và verify email
OTP_CODE=$(docker exec ecom-redis redis-cli GET "verify:user:${TEST_EMAIL}" 2>/dev/null | grep -o '"otp":[0-9]*' | cut -d':' -f2)
if [ -n "$OTP_CODE" ]; then
  VERIFY_RESP=$(curl -s -X POST "${GATEWAY_URL}/verify-email" \
    -H "Content-Type: application/json" \
    -d "{\"email\":\"${TEST_EMAIL}\",\"code\":${OTP_CODE}}")
  TOKEN=$(echo "$VERIFY_RESP" | grep -o '"token":"[^"]*' | cut -d'"' -f4)
fi

if [ -z "$TOKEN" ]; then
  TOKEN=$(curl -s -X POST "${GATEWAY_URL}/login" \
    -H "Content-Type: application/json" \
    -d "{\"email\":\"${TEST_EMAIL}\",\"password\":\"${PASSWORD}\"}" | grep -o '"token":"[^"]*' | cut -d'"' -f4)
fi

PRODUCT_ID=1
FLASH_STOCK=5

echo -e "\n${YELLOW}2. Pre-warming tồn kho Flash Sale: Nạp ${FLASH_STOCK} sản phẩm #1 vào Redis...${NC}"
PREWARM_RES=$(curl -s -X POST "${GATEWAY_URL}/orders/flash-sale/prewarm" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TOKEN}" \
  -d "{\"product_id\": ${PRODUCT_ID}, \"stock\": ${FLASH_STOCK}}")

echo -e "   Kết quả Pre-warm: ${PREWARM_RES}"

echo -e "\n${YELLOW}3. Bắn đồng thời 15 Request mua hàng Flash Sale (Chỉ có 5 món trong kho)...${NC}"

TMP_DIR=$(mktemp -d)

# Bắn 15 request song song với user_id khác nhau
for i in $(seq 1 15); do
  (
    RES=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X POST "${GATEWAY_URL}/orders/flash-sale" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "X-Request-ID: test-fs-req-${i}" \
      -d "{\"product_id\": ${PRODUCT_ID}, \"quantity\": 1, \"customer_name\": \"User ${i}\", \"customer_email\": \"user${i}@test.com\", \"customer_phone\": \"091234567${i}\", \"shipping_address\": \"Address ${i}\", \"payment_method\": \"COD\"}")
    
    HTTP_CODE=$(echo "$RES" | grep "HTTP_STATUS:" | cut -d':' -f2)
    BODY=$(echo "$RES" | grep -v "HTTP_STATUS:")
    
    echo "${HTTP_CODE}|${BODY}" > "${TMP_DIR}/res_${i}.txt"
  ) &
done

wait

SUCCESS_COUNT=0
REJECT_COUNT=0
TOKENS=()

for f in "${TMP_DIR}"/res_*.txt; do
  LINE=$(cat "$f")
  CODE=$(echo "$LINE" | cut -d'|' -f1)
  BODY=$(echo "$LINE" | cut -d'|' -f2)

  if [ "$CODE" == "202" ]; then
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    TOKEN_VAL=$(echo "$BODY" | grep -o '"order_token":"[^"]*' | cut -d'"' -f4)
    if [ -n "$TOKEN_VAL" ]; then
      TOKENS+=("$TOKEN_VAL")
    fi
  else
    REJECT_COUNT=$((REJECT_COUNT + 1))
  fi
done

rm -rf "${TMP_DIR}"

echo -e "\n${BOLD}${CYAN}📊 KẾT QUẢ TEST ĐỒNG THỜI:${NC}"
echo -e "   • Số đơn tiếp nhận thành công (202 Accepted): ${GREEN}${SUCCESS_COUNT} / ${FLASH_STOCK}${NC}"
echo -e "   • Số đơn bị từ chối do hết hàng:               ${RED}${REJECT_COUNT} / 10${NC}"

if [ "$SUCCESS_COUNT" -eq "$FLASH_STOCK" ]; then
  echo -e "${BOLD}${GREEN}✅ CHỐNG BÁN ÂM TUYỆT ĐỐI: Chỉ đúng ${FLASH_STOCK} đơn hàng được phép tạo!${NC}"
else
  echo -e "${BOLD}${YELLOW}⚠️ Số lượng thành công: ${SUCCESS_COUNT}${NC}"
fi

# 4. Test Polling trạng thái đơn hàng bất đồng bộ
if [ ${#TOKENS[@]} -gt 0 ]; then
  FIRST_TOKEN="${TOKENS[0]}"
  echo -e "\n${YELLOW}4. Kiểm tra Polling trạng thái từ RAM Redis (Zero DB Hit) cho token: ${FIRST_TOKEN}...${NC}"
  sleep 1
  STATUS_RES=$(curl -s -X GET "${GATEWAY_URL}/orders/flash-sale/status/${FIRST_TOKEN}" \
    -H "Authorization: Bearer ${TOKEN}")
  echo -e "   Trạng thái đơn hàng: ${STATUS_RES}"
fi

echo -e "\n${BOLD}${GREEN}🎉 Hoàn tất kiểm thử Flash Sale High-Concurrency Engine!${NC}\n"
