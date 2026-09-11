# Multi-stage Dockerfile cho Product Service
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy dependency configs
COPY go.mod go.sum ./
RUN go mod download

# Copy config, pkg và source của product-service
COPY pkg/ ./pkg/
COPY services/product-service/ ./services/product-service/

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/product-service ./services/product-service/cmd/main.go

# Minimal production image
FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/bin/product-service .

EXPOSE 8002

CMD ["./product-service"]
