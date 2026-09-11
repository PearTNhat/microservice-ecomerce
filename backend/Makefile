.PHONY: proto test build-all run-all run-gateway run-user run-product run-order run-docker

# Chạy toàn bộ 4 Microservices và Frontend cùng lúc (kèm log màu)
run-all:
	./start_all.sh

# Chạy từng Microservice độc lập
run-gateway:
	go run services/api-gateway/cmd/main.go

run-user:
	go run services/user-service/cmd/main.go

run-product:
	go run services/product-service/cmd/main.go

run-order:
	go run services/order-service/cmd/main.go

# Chạy kiểm thử tự động
test:
	go test ./services/api-gateway/... ./services/user-service/... ./services/product-service/... ./services/order-service/... ./pkg/...

test-e2e:
	./test_e2e_order.sh

test-flash-sale:
	./test_flash_sale.sh

# Biên dịch tất cả binaries
build-all:
	@mkdir -p bin
	go build -o bin/api-gateway ./services/api-gateway/cmd/main.go
	go build -o bin/user-service ./services/user-service/cmd/main.go
	go build -o bin/product-service ./services/product-service/cmd/main.go
	go build -o bin/order-service ./services/order-service/cmd/main.go
	@echo "✅ Đã build thành công 4 Microservices vào thư mục bin/!"

# Khởi động hạ tầng Docker
run-docker:
	docker compose up -d

down-docker:
	docker compose down

stop-docker:
	docker compose stop