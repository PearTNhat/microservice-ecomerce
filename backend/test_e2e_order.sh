#!/usr/bin/env bash
# ==============================================================================
# SCRIPT KIỂM THỬ TỰ ĐỘNG TOÀN DIỆN (END-TO-END E-COMMERCE MICROSERVICES TEST)
# ==============================================================================

GATEWAY_URL=${GATEWAY_URL:-"http://localhost:8000"}
TEST_EMAIL="e2e_test_$(date +%s)@example.com"
TEST_PASSWORD="Password123@"
IDEMPOTENCY_KEY="idemp-test-$(date +%s)"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}==================================================================${NC}"
echo -e "${BLUE}🚀 BẮT ĐẦU KIỂM THỬ E2E HỆ THỐNG E-COMMERCE MICROSERVICES${NC}"
echo -e "${BLUE}   API Gateway: ${GATEWAY_URL}${NC}"
echo -e "${BLUE}==================================================================${NC}"

# Hàm kiểm tra HTTP status
assert_status() {
    local expected=$1
    local actual=$2
    local step_name=$3
    if [ "$actual" -eq "$expected" ]; then
        echo -e "  [${GREEN}PASS${NC}] $step_name (HTTP $actual)"
    else
        echo -e "  [${RED}FAIL${NC}] $step_name (Kỳ vọng HTTP $expected, thực tế: $actual)"
    fi
}

# 1. Kiểm tra Gateway Healthcheck
echo -e "\n${YELLOW}1. Kiểm tra trạng thái API Gateway...${NC}"
HEALTH_RESP=$(curl -s -w "\n%{http_code}" "${GATEWAY_URL}/health")
HEALTH_CODE=$(echo "$HEALTH_RESP" | tail -n1)
assert_status 200 "$HEALTH_CODE" "API Gateway Health Check"

# 2. Đăng ký tài khoản kiểm thử
echo -e "\n${YELLOW}2. Đăng ký tài khoản khách hàng mới (${TEST_EMAIL})...${NC}"
REG_BODY=$(cat <<EOF
{
  "email": "${TEST_EMAIL}",
  "password": "${TEST_PASSWORD}",
  "first_name": "Nguyen",
  "last_name": "Test"
}
EOF
)
REG_RESP=$(curl -s -w "\n%{http_code}" -X POST "${GATEWAY_URL}/register" \
  -H "Content-Type: application/json" \
  -d "$REG_BODY")
REG_CODE=$(echo "$REG_RESP" | tail -n1)
assert_status 201 "$REG_CODE" "Đăng ký tài khoản"

# 3. Đăng nhập lấy Token
echo -e "\n${YELLOW}3. Đăng nhập lấy JWT Access Token...${NC}"
LOGIN_BODY=$(cat <<EOF
{
  "email": "${TEST_EMAIL}",
  "password": "${TEST_PASSWORD}"
}
EOF
)
LOGIN_RESP=$(curl -s -w "\n%{http_code}" -X POST "${GATEWAY_URL}/login" \
  -H "Content-Type: application/json" \
  -d "$LOGIN_BODY")
LOGIN_CODE=$(echo "$LOGIN_RESP" | tail -n1)
LOGIN_JSON=$(echo "$LOGIN_RESP" | sed '$d')

TOKEN=$(echo "$LOGIN_JSON" | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
if [ -z "$TOKEN" ]; then
    # Thử parse data.access_token
    TOKEN=$(echo "$LOGIN_JSON" | grep -o '"token":"[^"]*' | cut -d'"' -f4)
fi

if [ -n "$TOKEN" ]; then
    echo -e "  [${GREEN}PASS${NC}] Đăng nhập thành công, nhận JWT Token"
else
    echo -e "  [${RED}FAIL${NC}] Không lấy được JWT Token: $LOGIN_JSON"
fi

# 4. Lấy danh sách sản phẩm từ Product Service
echo -e "\n${YELLOW}4. Lấy danh sách sản phẩm điện máy (Product Service - Port 8002)...${NC}"
PROD_RESP=$(curl -s -w "\n%{http_code}" "${GATEWAY_URL}/products?page=1&limit=5")
PROD_CODE=$(echo "$PROD_RESP" | tail -n1)
assert_status 200 "$PROD_CODE" "Lấy danh sách sản phẩm"

# 5. Thêm sản phẩm vào giỏ hàng (Order Service - Port 8003)
echo -e "\n${YELLOW}5. Thêm sản phẩm ID=1 vào Giỏ hàng...${NC}"
CART_ADD_BODY='{"product_id": 1, "quantity": 1}'
CART_RESP=$(curl -s -w "\n%{http_code}" -X POST "${GATEWAY_URL}/cart/add" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TOKEN}" \
  -d "$CART_ADD_BODY")
CART_CODE=$(echo "$CART_RESP" | tail -n1)
assert_status 200 "$CART_CODE" "Thêm vào giỏ hàng"

# 6. Đặt hàng lần 1 kèm Idempotency-Key
echo -e "\n${YELLOW}6. Đặt hàng qua Checkout API (POST /orders/checkout) với X-Idempotency-Key: ${IDEMPOTENCY_KEY}...${NC}"
ORDER_BODY=$(cat <<EOF
{
  "customer_name": "Nguyen Test",
  "customer_email": "${TEST_EMAIL}",
  "customer_phone": "0987654321",
  "shipping_address": "789 Đường Thử Nghiệm, TP.HCM",
  "payment_method": "COD",
  "from_cart": false,
  "items": [
    {"product_id": 1, "quantity": 1}
  ]
}
EOF
)
ORDER_RESP=$(curl -s -w "\n%{http_code}" -X POST "${GATEWAY_URL}/orders/checkout" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "X-Idempotency-Key: ${IDEMPOTENCY_KEY}" \
  -d "$ORDER_BODY")
ORDER_CODE=$(echo "$ORDER_RESP" | tail -n1)
ORDER_JSON=$(echo "$ORDER_RESP" | sed '$d')
assert_status 201 "$ORDER_CODE" "Tạo đơn hàng thành công (Lần 1)"

ORDER_ID=$(echo "$ORDER_JSON" | grep -o '"id":[0-9]*' | head -n1 | cut -d':' -f2)
ORDER_NUM=$(echo "$ORDER_JSON" | grep -o '"order_code":"[^"]*' | head -n1 | cut -d'"' -f4)
echo -e "  [${GREEN}INFO${NC}] Mã đơn hàng tạo ra: ${ORDER_NUM} (ID: ${ORDER_ID})"

# 7. Kiểm tra tính năng Idempotency (Gửi lại đúng Key đó lần 2 -> Phải bị chặn 409 Conflict)
echo -e "\n${YELLOW}7. Kiểm tra chống gửi trùng đơn (Gửi lại cùng X-Idempotency-Key)...${NC}"
RETRY_RESP=$(curl -s -w "\n%{http_code}" -X POST "${GATEWAY_URL}/orders/checkout" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "X-Idempotency-Key: ${IDEMPOTENCY_KEY}" \
  -d "$ORDER_BODY")
RETRY_CODE=$(echo "$RETRY_RESP" | tail -n1)
assert_status 409 "$RETRY_CODE" "Chống gửi trùng đơn (Idempotency Key Lock - HTTP 409)"

# 8. Xem danh sách đơn hàng của User
echo -e "\n${YELLOW}8. Xem lịch sử đơn hàng của người dùng (GET /orders)...${NC}"
LIST_RESP=$(curl -s -w "\n%{http_code}" -X GET "${GATEWAY_URL}/orders?page=1&limit=10" \
  -H "Authorization: Bearer ${TOKEN}")
LIST_CODE=$(echo "$LIST_RESP" | tail -n1)
assert_status 200 "$LIST_CODE" "Lấy danh sách đơn hàng đã mua"

# 9. Xem chi tiết đơn hàng vừa tạo
if [ -n "$ORDER_ID" ]; then
    echo -e "\n${YELLOW}9. Xem chi tiết đơn hàng ID=${ORDER_ID} (GET /orders/${ORDER_ID})...${NC}"
    DETAIL_RESP=$(curl -s -w "\n%{http_code}" -X GET "${GATEWAY_URL}/orders/${ORDER_ID}" \
      -H "Authorization: Bearer ${TOKEN}")
    DETAIL_CODE=$(echo "$DETAIL_RESP" | tail -n1)
    assert_status 200 "$DETAIL_CODE" "Lấy chi tiết đơn hàng"
fi

echo -e "\n${BLUE}==================================================================${NC}"
echo -e "${GREEN}🎉 HOÀN THÀNH KIỂM THỬ HỆ THỐNG MICROSERVICES!${NC}"
echo -e "${BLUE}==================================================================${NC}"
