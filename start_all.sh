#!/usr/bin/env bash
# ==============================================================================
# Script: start_all.sh
# Description: Starts all 4 Go Microservices + Next.js Frontend concurrently with
#              color-coded real-time log output and graceful shutdown on Ctrl+C.
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
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="${PROJECT_ROOT}/backend"
FRONTEND_DIR="${PROJECT_ROOT}/frontend"

echo -e "${BOLD}${BLUE}==============================================================${NC}"
echo -e "${BOLD}${BLUE}   🚀 Starting E-Commerce Microservices & Frontend Ecosystem   ${NC}"
echo -e "${BOLD}${BLUE}==============================================================${NC}"

# Ensure Docker infra is up
echo -e "${YELLOW}🔍 Checking Docker containers...${NC}"
if [ -d "${BACKEND_DIR}" ]; then
  (cd "${BACKEND_DIR}" && docker compose up -d)
fi

# Array to keep track of spawned process PIDs
PIDS=()

# Cleanup handler on EXIT / Ctrl+C
cleanup() {
  # Disable trap to avoid recursive loops
  trap - SIGINT SIGTERM EXIT
  echo -e "\n${BOLD}${YELLOW}🛑 Shutting down all microservices and frontend...${NC}"
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null
    fi
  done
  echo -e "${GREEN}✅ All services stopped successfully.${NC}"
  exit 0
}

trap cleanup SIGINT SIGTERM EXIT

# Helper function to run a service and prefix its stdout/stderr with color
run_service() {
  local name="$1"
  local color="$2"
  local cmd="$3"
  local dir="$4"

  (
    cd "$dir" || exit 1
    eval "$cmd" 2>&1 | while IFS= read -r line; do
      echo -e "${color}[${name}]${NC} $line"
    done
  ) &
  PIDS+=($!)
}

echo -e "${GREEN}Starting services in parallel...${NC}\n"

# 1. API Gateway (Port 8000)
run_service "GATEWAY" "${CYAN}" "go run cmd/api-gateway/main.go" "${BACKEND_DIR}"

# 2. User Microservice (Port 8001)
run_service "USER-SVC" "${GREEN}" "go run cmd/user-service/main.go" "${BACKEND_DIR}"

# 3. Product Microservice (Port 8002)
run_service "PRODUCT" "${YELLOW}" "go run cmd/product-service/main.go" "${BACKEND_DIR}"

# 4. Order Microservice (Port 8003)
run_service "ORDER-SVC" "${MAGENTA}" "go run cmd/order-service/main.go" "${BACKEND_DIR}"

# 5. Frontend Next.js (Port 3000)
if [ -d "${FRONTEND_DIR}" ]; then
  run_service "FRONTEND" "${BLUE}" "npm run dev" "${FRONTEND_DIR}"
fi

echo -e "\n${BOLD}${GREEN}⚡ All services launched! Logs are streaming below (Press Ctrl+C to stop all):${NC}\n"

# Wait for all background jobs
wait
