.PHONY: proto test build-all run-gateway run-user run-product run-order run-docker

# Chạy từng Microservice độc lập
run-gateway:
	go run cmd/api-gateway/main.go

run-user:
	go run cmd/user-service/main.go

run-product:
	go run cmd/product-service/main.go

run-order:
	go run cmd/order-service/main.go

# Chạy kiểm thử tự động
test:
	go test -v ./...

# Biên dịch tất cả binaries
build-all:
	@mkdir -p bin
	go build -o bin/api-gateway ./cmd/api-gateway
	go build -o bin/user-service ./cmd/user-service
	go build -o bin/product-service ./cmd/product-service
	go build -o bin/order-service ./cmd/order-service
	@echo "✅ Đã build thành công 4 Microservices vào thư mục bin/!"

# Sinh mã Protobuf cho gRPC
proto:
	protoc --go_out=. --go-grpc_out=. api/proto/*.proto

# Khởi động hạ tầng Docker
run-docker:
	docker compose up -d

down-docker:
	docker compose down

stop-docker:
	docker compose stop