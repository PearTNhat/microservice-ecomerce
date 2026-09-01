#!/usr/bin/env bash
# ==============================================================================
# Script: backend/start_all.sh
# Description: Starts all 4 Go Backend Microservices concurrently, streams
#              color-coded real-time logs to terminal, automatically clears old
#              logs on start, saves logs per service into backend/logs/, and
#              gracefully stops all services on Ctrl+C.
# ==============================================================================

# ANSI Color Codes for clean log formatting
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
MAGENTA='\033[0;35m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Determine directories
BACKEND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGS_DIR="${BACKEND_DIR}/logs"

echo -e "${BOLD}${BLUE}==============================================================${NC}"
echo -e "${BOLD}${BLUE}       🚀 Starting E-Commerce Backend Microservices           ${NC}"
echo -e "${BOLD}${BLUE}==============================================================${NC}"

# 1. Clear old logs & create clean logs directory
echo -e "${YELLOW}🧹 Clearing old logs in ${LOGS_DIR}...${NC}"
rm -rf "${LOGS_DIR}"
mkdir -p "${LOGS_DIR}"
echo -e "${GREEN}📁 Logs directory initialized: ${LOGS_DIR}${NC}\n"

# 2. Ensure Docker infrastructure is running
echo -e "${YELLOW}🔍 Checking Docker infrastructure...${NC}"
(cd "${BACKEND_DIR}" && docker compose up -d)

# Array to keep track of spawned process PIDs
PIDS=()

# Cleanup handler on EXIT / Ctrl+C
cleanup() {
  # Disable trap to avoid recursive loops
  trap - SIGINT SIGTERM EXIT
  echo -e "\n${BOLD}${YELLOW}🛑 Shutting down all backend microservices...${NC}"
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null
    fi
  done
  echo -e "${GREEN}✅ All backend microservices stopped. Logs saved in ${LOGS_DIR}/${NC}"
  exit 0
}

trap cleanup SIGINT SIGTERM EXIT

# Helper function to run a service: writes to log file + streams with color to terminal
run_service() {
  local name="$1"
  local color="$2"
  local cmd="$3"
  local log_file="${LOGS_DIR}/$4"

  touch "$log_file"
  (
    cd "${BACKEND_DIR}" || exit 1
    eval "$cmd" 2>&1 | while IFS= read -r line; do
      # 1. Append raw line to service-specific log file
      echo "$line" >> "$log_file"
      # 2. Print color-coded line to terminal
      echo -e "${color}[${name}]${NC} $line"
    done
  ) &
  PIDS+=($!)
}

echo -e "${GREEN}Starting 4 Go Microservices in parallel...${NC}\n"

# 1. API Gateway (Port 8000) -> backend/logs/api-gateway.log
run_service "GATEWAY" "${CYAN}" "go run cmd/api-gateway/main.go" "api-gateway.log"

# 2. User Microservice (Port 8001) -> backend/logs/user-service.log
run_service "USER-SVC" "${GREEN}" "go run cmd/user-service/main.go" "user-service.log"

# 3. Product Microservice (Port 8002) -> backend/logs/product-service.log
run_service "PRODUCT" "${YELLOW}" "go run cmd/product-service/main.go" "product-service.log"

# 4. Order Microservice (Port 8003) -> backend/logs/order-service.log
run_service "ORDER-SVC" "${MAGENTA}" "go run cmd/order-service/main.go" "order-service.log"

echo -e "\n${BOLD}${GREEN}⚡ All 4 Backend Microservices are running!${NC}"
echo -e "${BOLD}${CYAN}📄 Log files are being written to:${NC}"
echo -e "   • ${LOGS_DIR}/api-gateway.log"
echo -e "   • ${LOGS_DIR}/user-service.log"
echo -e "   • ${LOGS_DIR}/product-service.log"
echo -e "   • ${LOGS_DIR}/order-service.log"
echo -e "${BOLD}${YELLOW}👉 Streaming live logs below (Press Ctrl+C to stop all):${NC}\n"

# Wait for all background jobs
wait
