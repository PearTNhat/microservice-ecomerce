#!/usr/bin/env bash
# ==============================================================================
# Script: backend/start_all.sh
# Usage:
#   ./start_all.sh [run|start] [-d]  : Khởi động 4 Go Microservices (mặc định)
#   ./start_all.sh stop             : Dừng sạch sẽ toàn bộ 4 Microservices
#   ./start_all.sh restart [-d]      : Khởi động lại toàn bộ services
#   ./start_all.sh status           : Kiểm tra trạng thái các port và services
#   ./start_all.sh logs [service]   : Xem log theo thời gian thực (tail -f)
# ==============================================================================

# ANSI Color Codes
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
MAGENTA='\033[0;35m'
BLUE='\033[0;34m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Determine directories
BACKEND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGS_DIR="${BACKEND_DIR}/logs"
PID_FILE="${LOGS_DIR}/.services.pid"

PORTS=(8000 8001 8002 8003 50051 50052 50053)

# ------------------------------------------------------------------------------
# 1. FUNCTION: STOP ALL SERVICES
# ------------------------------------------------------------------------------
stop_services() {
  echo -e "${BOLD}${YELLOW}🛑 Đang dừng toàn bộ Backend Microservices...${NC}"

  # 1. Kill bằng danh sách PID đã lưu
  if [[ -f "${PID_FILE}" ]]; then
    while IFS= read -r pid; do
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null
      fi
    done < "${PID_FILE}"
    rm -f "${PID_FILE}"
  fi

  # 2. Giải phóng các cổng mạng (Port)
  for port in "${PORTS[@]}"; do
    local pids
    pids=$(lsof -ti :"$port" 2>/dev/null || fuser "$port"/tcp 2>/dev/null)
    if [[ -n "$pids" ]]; then
      echo -e "   ${RED}• Giải phóng cổng :${port} (PID: ${pids})${NC}"
      kill -9 $pids 2>/dev/null
    fi
  done

  # 3. Quét sạch các tiến trình Go chạy từ bin/, services/ hoặc cmd/
  pkill -f "bin/api-gateway" 2>/dev/null
  pkill -f "bin/user-service" 2>/dev/null
  pkill -f "bin/product-service" 2>/dev/null
  pkill -f "bin/order-service" 2>/dev/null
  pkill -f "services/api-gateway" 2>/dev/null
  pkill -f "services/user-service" 2>/dev/null
  pkill -f "services/product-service" 2>/dev/null
  pkill -f "services/order-service" 2>/dev/null

  sleep 0.5
  echo -e "${BOLD}${GREEN}✅ Đã dừng thành công toàn bộ Microservices!${NC}"
}

# ------------------------------------------------------------------------------
# 2. FUNCTION: CHECK STATUS
# ------------------------------------------------------------------------------
check_status() {
  echo -e "${BOLD}${BLUE}==============================================================${NC}"
  echo -e "${BOLD}${BLUE}           📊 Trạng Thái Hệ Thống Microservices               ${NC}"
  echo -e "${BOLD}${BLUE}==============================================================${NC}"

  printf "%-18s %-10s %-12s %-15s\n" "SERVICE" "PORT" "STATUS" "PID"
  echo "--------------------------------------------------------------"

  check_port() {
    local name="$1"
    local port="$2"
    local pids
    pids=$(lsof -ti :"$port" 2>/dev/null)

    if [[ -n "$pids" ]]; then
      local first_pid
      first_pid=$(echo "$pids" | head -n1)
      printf "%-18s %-10s ${GREEN}%-12s${NC} %-15s\n" "$name" ":$port" "RUNNING" "PID: $first_pid"
    else
      printf "%-18s %-10s ${RED}%-12s${NC} %-15s\n" "$name" ":$port" "STOPPED" "-"
    fi
  }

  check_port "API Gateway" 8000
  check_port "User Service" 8001
  check_port "Product Service" 8002
  check_port "Order Service" 8003
  echo "--------------------------------------------------------------"
}

# ------------------------------------------------------------------------------
# 3. FUNCTION: START SERVICES
# ------------------------------------------------------------------------------
start_services() {
  local daemon_mode=false
  if [[ "$1" == "-d" ]] || [[ "$2" == "-d" ]]; then
    daemon_mode=true
  fi

  # Kiểm tra xem có service nào đang chạy chưa
  local running_pids
  running_pids=$(lsof -ti :8000 -ti :8001 -ti :8002 -ti :8003 2>/dev/null)
  if [[ -n "$running_pids" ]]; then
    echo -e "${YELLOW}⚠️ Có service đang chạy trên cổng 8000-8003. Tiến hành dọn dẹp trước...${NC}"
    stop_services
    sleep 1
  fi

  echo -e "${BOLD}${BLUE}==============================================================${NC}"
  echo -e "${BOLD}${BLUE}       🚀 Khởi Động E-Commerce Backend Microservices          ${NC}"
  echo -e "${BOLD}${BLUE}==============================================================${NC}"

  # Khởi tạo thư mục logs
  rm -rf "${LOGS_DIR}"
  mkdir -p "${LOGS_DIR}"
  touch "${PID_FILE}"
  echo -e "${GREEN}📁 Thư mục log đã khởi tạo: ${LOGS_DIR}${NC}"

  # Đảm bảo hạ tầng Docker Compose sẵn sàng (Chỉ dùng PostgreSQL, Redis và Apache Kafka)
  echo -e "${YELLOW}🔍 Đảm bảo hạ tầng Docker (PostgreSQL, Redis, Kafka)...${NC}"
  (cd "${BACKEND_DIR}" && docker compose up -d postgres redis kafka)

  PIDS=()

  # Handler khi người dùng bấm Ctrl+C ở chế độ foreground
  cleanup() {
    trap - SIGINT SIGTERM EXIT
    echo -e "\n${BOLD}${YELLOW}🛑 Nhận tín hiệu dừng từ bàn phím...${NC}"
    stop_services
    exit 0
  }

  if [[ "$daemon_mode" == false ]]; then
    trap cleanup SIGINT SIGTERM EXIT
  fi

  # Helper chạy service
  run_service() {
    local name="$1"
    local color="$2"
    local cmd="$3"
    local log_file="${LOGS_DIR}/$4"

    touch "$log_file"
    if [[ "$daemon_mode" == true ]]; then
      # Chế độ chạy ngầm (detached với nohup & disown)
      cd "${BACKEND_DIR}" && nohup "$cmd" > "$log_file" 2>&1 < /dev/null &
      local pid=$!
      disown "$pid"
      sleep 0.3
      PIDS+=("$pid")
      echo "$pid" >> "${PID_FILE}"
    else
      # Chế độ stream log ra màn hình
      (
        cd "${BACKEND_DIR}" || exit 1
        eval "$cmd" 2>&1 | while IFS= read -r line; do
          echo "$line" >> "$log_file"
          echo -e "${color}[${name}]${NC} $line"
        done
      ) &
      local pid=$!
      PIDS+=("$pid")
      echo "$pid" >> "${PID_FILE}"
    fi
  }

  mkdir -p "${BACKEND_DIR}/bin"
  echo -e "${YELLOW}🔨 Đang biên dịch 4 Microservices...${NC}"
  go build -o "${BACKEND_DIR}/bin/api-gateway" "${BACKEND_DIR}/services/api-gateway/cmd/main.go"
  go build -o "${BACKEND_DIR}/bin/user-service" "${BACKEND_DIR}/services/user-service/cmd/main.go"
  go build -o "${BACKEND_DIR}/bin/product-service" "${BACKEND_DIR}/services/product-service/cmd/main.go"
  go build -o "${BACKEND_DIR}/bin/order-service" "${BACKEND_DIR}/services/order-service/cmd/main.go"
  echo -e "${GREEN}✅ Biên dịch hoàn tất! Khởi động 4 Microservices...${NC}\n"

  run_service "GATEWAY"   "${CYAN}"    "./bin/api-gateway"     "api-gateway.log"
  run_service "USER-SVC"  "${GREEN}"   "./bin/user-service"    "user-service.log"
  run_service "PRODUCT"   "${YELLOW}"  "./bin/product-service" "product-service.log"
  run_service "ORDER-SVC" "${MAGENTA}" "./bin/order-service"   "order-service.log"

  if [[ "$daemon_mode" == true ]]; then
    sleep 2
    echo -e "\n${BOLD}${GREEN}⚡ Toàn bộ 4 Microservices đã chạy ngầm thành công!${NC}"
    check_status
    echo -e "${BOLD}${CYAN}📄 Xem log: ./start_all.sh logs [gateway|user|product|order]${NC}"
    echo -e "${BOLD}${RED}🛑 Để dừng: ./start_all.sh stop${NC}\n"
    exit 0
  else
    echo -e "\n${BOLD}${GREEN}⚡ Toàn bộ 4 Microservices đang chạy!${NC}"
    echo -e "${BOLD}${YELLOW}👉 Đang stream log trực tiếp (Nhấn Ctrl+C để dừng tất cả):${NC}\n"
    wait
  fi
}

# ------------------------------------------------------------------------------
# 4. FUNCTION: VIEW LOGS
# ------------------------------------------------------------------------------
view_logs() {
  local target="$1"
  case "$target" in
    gateway|api-gateway)
      tail -f "${LOGS_DIR}/api-gateway.log"
      ;;
    user|user-service)
      tail -f "${LOGS_DIR}/user-service.log"
      ;;
    product|product-service)
      tail -f "${LOGS_DIR}/product-service.log"
      ;;
    order|order-service)
      tail -f "${LOGS_DIR}/order-service.log"
      ;;
    *)
      echo -e "${YELLOW}Theo dõi toàn bộ logs (nhấn Ctrl+C để thoát)...${NC}"
      tail -f "${LOGS_DIR}"/*.log
      ;;
  esac
}

# ------------------------------------------------------------------------------
# 5. CLI ROUTER
# ------------------------------------------------------------------------------
ACTION="${1:-run}"

case "$ACTION" in
  start|run)
    start_services "$2"
    ;;
  stop)
    stop_services
    ;;
  restart)
    stop_services
    sleep 1
    start_services "$2"
    ;;
  status)
    check_status
    ;;
  logs)
    view_logs "$2"
    ;;
  help|--help|-h)
    echo -e "${BOLD}Hướng dẫn sử dụng start_all.sh:${NC}"
    echo -e "  ${GREEN}./start_all.sh${NC}             : Chạy 4 services và stream log (Ctrl+C để dừng)"
    echo -e "  ${GREEN}./start_all.sh run -d${NC}      : Chạy 4 services ở chế độ ngầm (Background)"
    echo -e "  ${GREEN}./start_all.sh stop${NC}        : Dừng sạch sẽ toàn bộ 4 services và giải phóng port"
    echo -e "  ${GREEN}./start_all.sh restart${NC}     : Khởi động lại toàn bộ services"
    echo -e "  ${GREEN}./start_all.sh status${NC}      : Kiểm tra trạng thái chạy của từng service"
    echo -e "  ${GREEN}./start_all.sh logs${NC}        : Xem live log của tất cả services"
    ;;
  *)
    echo -e "${RED}Lệnh không hợp lệ: $ACTION${NC}"
    echo -e "Sử dụng: ${GREEN}./start_all.sh [run|stop|restart|status|logs]${NC}"
    exit 1
    ;;
esac
