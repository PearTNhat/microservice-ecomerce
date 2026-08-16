# 🤖 AI Agent Guidelines: Clean Architecture in Go

This document serves as the **Standard Operating Procedure (SOP)** for any AI agents (or human developers) modifying or expanding this Go Microservice.

## 🏛 Core Architectural Pattern: Clean Architecture
This project strictly follows the **Clean Architecture** principles. The fundamental rule is: **Dependencies MUST point inwards**. 
Outer layers (API, Database) depend on inner layers (Service, Domain). Inner layers MUST NEVER depend on outer layers.

---

### 1. Domain Layer (`internal/core/domain`)
- **Role:** The absolute core of the system. Contains business entities (Structs) and Repository Interfaces.
- **Rules:**
  - 🚫 **MUST NOT** import any other package from `internal/*`.
  - 🚫 **MUST NOT** import any framework-specific libraries (like Fiber, gRPC, Asynq).
  - ✅ **DO** define structs (e.g., `User`) and repository interfaces here.

### 2. Service (Use Case) Layer (`internal/core/service`)
- **Role:** Contains all the business logic and application rules.
- **Rules:**
  - ✅ **DO** import `domain`, `dto`, and generic utilities (`config`, `pkg/utils`).
  - 🚫 **MUST NOT** import `api` (no `*fiber.Ctx`, no `*pb.Request`).
  - 🚫 **MUST NOT** import `repository/postgres` directly (only depend on the interface from `domain`).
  - Business logic MUST be placed here, NEVER in handlers.

### 3. Data/Repository Layer (`internal/repository/...`)
- **Role:** Implements the interfaces defined in the Domain layer. Interacts with the actual Database (PostgreSQL via GORM).
- **Rules:**
  - ✅ **DO** import `domain`.
  - 🚫 **MUST NOT** contain business rules. Its only job is to Create, Read, Update, and Delete data.

### 4. Delivery/API Layer (`internal/api`)
- **Role:** The entry points of the application (REST & gRPC).
- **Sub-folders:**
  - `rest/handlers`: Handles HTTP parsing, validation using Fiber, and calls the Service layer. Returns JSON.
  - `grpc/handlers`: Handles Protobuf parsing and calls the SAME Service layer. Returns Protobuf responses.
- **Rules:**
  - 🚫 **MUST NOT** contain any business logic (No IF/ELSE checking passwords, no DB queries).
  - ✅ **DO** act as a "Thin layer": Receive Request -> Parse to DTO -> Call Service -> Format Response.

### 5. DTO Layer (`internal/dto`)
- **Role:** Data Transfer Objects. Defines the exact shape of data moving between the API layer and the Service layer.
- **Rules:**
  - ✅ **DO** use DTOs to decouple the Service layer from HTTP (Fiber) or gRPC (Protobuf) specific structures.

### 6. Worker Layer (`internal/worker`)
- **Role:** Handles asynchronous background jobs (using Asynq/Redis).
- **Rules:**
  - `Distributor`: Pushes jobs to the queue.
  - `Processor`: Pulls jobs and executes them. Can call Services or external APIs (e.g., SMTP).

---

## 🚦 Important Coding Standards for Agents
1. **Dependency Injection (DI):** Always use DI. Never use global variables for DB connections or services. Pass dependencies via constructor functions (e.g., `NewUserService(repo, config)`).
2. **Global 404 Handler:** A catch-all route is implemented at the bottom of the Fiber setup in `cmd/server/main.go`. Do not overwrite it.
3. **Database Migrations:** Do not rely on GORM's `AutoMigrate` for deleting columns. Remember that GORM does not automatically drop columns.
4. **Environment Variables:** Never hardcode secrets. Always use the `config.AppConfig` struct.

> **To AI Agent:** Acknowledge this architecture in your thought process before writing or modifying any Go code in this project.
