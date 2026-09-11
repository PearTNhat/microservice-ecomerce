# Multi-stage Dockerfile cho API Gateway
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy dependency configs
COPY go.mod go.sum ./
RUN go mod download

# Copy pkg và source của api-gateway
COPY pkg/ ./pkg/
COPY services/api-gateway/ ./services/api-gateway/

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/api-gateway ./services/api-gateway/cmd/main.go

# Minimal production image
FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/bin/api-gateway .

EXPOSE 8000

CMD ["./api-gateway"]
