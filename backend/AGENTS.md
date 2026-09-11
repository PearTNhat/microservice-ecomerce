# 🤖 AI Agent Guidelines: Clean Architecture & Event-Driven Microservices

This document serves as the **Standard Operating Procedure (SOP)** for any AI agents (or human developers) modifying or expanding this Go E-Commerce Microservices repository.

---

## 🏛 1. Core Architectural Pattern: Multi-Module Clean Architecture

This project strictly follows the **Clean Architecture** principles within a **Multi-Module Monorepo** managed by `go.work`.

### 🚨 The Fundamental Dependency Rule
> **Dependencies MUST ALWAYS point inwards.**
> Outer layers (`delivery`, `repository`, `client`, `worker`) depend on inner layers (`service`, `domain`).
> Inner layers MUST NEVER depend on outer layers.

```
services/<service-name>/
├── cmd/                             # Composition Root (Dependency Injection only)
│   └── <service-name>/main.go
└── internal/
    ├── domain/                      # 1. Enterprise Core: Entities & Interfaces (ZERO external imports)
    ├── dto/                         # Data Transfer Objects
    ├── service/                     # 2. Use Cases: Business logic (depends ONLY on domain interfaces)
    ├── repository/                  # 3. Adapters: Database implementations (GORM/PostgreSQL)
    ├── client/                      # 3. Adapters: External service clients (HTTP/gRPC)
    ├── worker/                      # 3. Adapters: Kafka event consumers
    └── delivery/                    # 4. Frameworks & Drivers
        ├── http/handlers/           # Fiber HTTP REST Handlers
        └── grpc/handlers/           # gRPC Handlers (if applicable)
```

---

## 📂 2. Layer Guidelines & Strict Constraints

### 1. Domain Layer (`services/*/internal/domain/`)
- **Role:** Pure business entities and repository/client contracts (Interfaces).
- **Rules:**
  - 🚫 **MUST NOT** import any external framework (NO Fiber, Gin, GORM, Postgres, Kafka, Asynq).
  - 🚫 **MUST NOT** import any sibling packages (`service`, `repository`, `delivery`).
  - ✅ **DO** define structs (e.g., `Order`, `Product`, `User`) and interfaces (e.g., `OrderRepository`, `ProductClient`).

### 2. Service (Use Case) Layer (`services/*/internal/service/`)
- **Role:** Implements all core business rules and use cases.
- **Rules:**
  - ✅ **DO** import `domain`, `dto`, and shared utilities (`pkg/config`, `pkg/logger`, `pkg/utils`).
  - 🚫 **MUST NOT** import `delivery` (NO `*fiber.Ctx`, NO Protobuf request/response structs).
  - 🚫 **MUST NOT** import `repository/postgres` directly — ONLY depend on `domain.<Interface>`.
  - All business validation, transaction handling, and business calculations MUST live here, NEVER in handlers.

### 3. Repository Layer (`services/*/internal/repository/postgres/`)
- **Role:** Implements domain persistence interfaces using PostgreSQL and GORM.
- **Rules:**
  - 🚫 **MUST NOT** contain business logic. Its only job is CRUD on the service's own isolated database.
  - 🚫 **MUST NEVER** connect to or query another service's database.

### 4. Client Layer (`services/*/internal/client/`)
- **Role:** Implements inter-service communication (REST HTTP with Redis cache, or gRPC).
- **Rules:**
  - Implements `domain.<ServiceClient>` interface to allow mocking in unit tests.
  - Must include short timeouts, circuit breaker/fallback patterns, and pass `X-Trace-ID` for distributed tracing.

### 5. Delivery Layer (`services/*/internal/delivery/`)
- **Role:** Transport entry points (REST Fiber handlers and gRPC handlers).
- **Rules:**
  - 🚫 **MUST NOT** contain business logic.
  - ✅ **DO** act as a thin adapter: Receive Request $\rightarrow$ Parse/Validate DTO $\rightarrow$ Call Service $\rightarrow$ Format JSON response via `pkg/response`.

### 6. Shared Module (`pkg/`)
- **Role:** Code shared across multiple microservices. Has its own `go.mod` (`ecomerce-service/pkg`).
- Contains: `config/`, `kafka/`, `logger/`, `middlewares/`, `redislock/`, `response/`, `server/`.
- Must remain clean, tested, and general-purpose.

---

## ⚡ 3. Distributed Transactions & Concurrency Rules

1. **Database-per-Service:** Each service owns its database (`ecom_user_db`, `ecom_product_db`, `ecom_order_db`). Cross-database queries are STRICTLY FORBIDDEN.
2. **100% Apache Kafka Event-Driven Backbone:**
   - RabbitMQ is completely deprecated and removed. All asynchronous operations must use Kafka.
   - Topics, Event Structs, and Consumer Groups must be registered in [pkg/kafka/topics.go](pkg/kafka/topics.go).
   - Distributed transactions must follow **Saga Choreography** (Compensating transactions on failure).
3. **High-Concurrency Stock Deductions:**
   - Flash sale or high-traffic inventory operations must use the atomic Redis Lua lock in [pkg/redislock/redis_stock_lock.go](pkg/redislock/redis_stock_lock.go).
4. **Idempotency:**
   - Critical mutating endpoints (like checkout and order creation) must enforce the `Idempotency-Key` header via [pkg/middlewares/idempotency_middleware.go](pkg/middlewares/idempotency_middleware.go).

---

## 🔄 4. Mandatory Documentation Update Rule

> [!IMPORTANT]
> **MANDATORY FOR ALL AI AGENTS:**
> Whenever you add, modify, or remove any:
> - Microservice, port, or route
> - Kafka topic or Saga event
> - Database model or migration
> - Architecture layer or shared module
> 
> You **MUST IMMEDIATELY UPDATE** both:
> 1. [README.md](README.md): To ensure user documentation reflects the current state.
> 2. [ARCHITECTURE.md](ARCHITECTURE.md): To ensure architectural diagrams and design decisions remain accurate.
> 
> Never leave documentation stale after making structural code changes.
