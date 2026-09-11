# Multi-stage Dockerfile cho Order Service
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy dependency configs
COPY go.mod go.sum ./
RUN go mod download

# Copy config, pkg và source của order-service
COPY pkg/ ./pkg/
COPY services/order-service/ ./services/order-service/

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/order-service ./services/order-service/cmd/main.go

# Minimal production image
FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/bin/order-service .

EXPOSE 8003

CMD ["./order-service"]
