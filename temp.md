    graph TB
        Client[Web App / Mobile App / Admin Dashboard] --> APIGateway[API Gateway / BFF - Kong, KrakenD hoặc Go Fiber]

        subgraph Core Business Services
            APIGateway --> UserService[1. User & Auth Service]
            APIGateway --> ProductService[2. Product & Catalog Service]
            APIGateway --> InventoryService[3. Inventory & Warehouse Service]
            APIGateway --> OrderService[4. Order & Checkout Service]
            APIGateway --> PaymentService[5. Payment Service]
            APIGateway --> InstallationService[6. Delivery & Installation Service]
            APIGateway --> WarrantyService[7. E-Warranty & After-Sales Service]
            APIGateway --> PromotionService[8. Promotion & Installment Service]
        end

        subgraph Async & Shared Infrastructure
            OrderService -.-> Kafka[Message Broker - Kafka / RabbitMQ / Redis Streams]
            PaymentService -.-> Kafka
            InventoryService -.-> Kafka
            Kafka --> NotificationService[9. Notification Service - Asynq/SMTP/SMS]
            Kafka --> SearchService[10. Search & Filter Service - Elasticsearch]
        end
