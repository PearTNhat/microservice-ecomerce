# Multi-stage Dockerfile cho User Service
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy dependency configs
COPY go.mod go.sum ./
RUN go mod download

# Copy config, pkg và source của user-service
COPY pkg/ ./pkg/
COPY services/user-service/ ./services/user-service/

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/user-service ./services/user-service/cmd/main.go

# Minimal production image
FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/bin/user-service .

EXPOSE 8001 50051

CMD ["./user-service"]
