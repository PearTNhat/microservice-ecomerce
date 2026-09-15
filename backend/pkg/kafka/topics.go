package kafka

// Danh sách các Kafka Topics chuẩn trong hệ thống Microservices
const (
	TopicOrderEvents        = "order.events"
	TopicStockEvents        = "stock.events"
	TopicFlashSaleOrders    = "flashsale.orders"
	TopicFlashSaleConfirmed = "flashsale.confirmed"
	TopicProductViews       = "product-views"
	TopicOrdersDLT          = "orders.dead_letter"
)

// Các loại sự kiện (Event Types)
const (
	EventOrderCreated            = "ORDER_CREATED"
	EventOrderPaid               = "ORDER_PAID"
	EventOrderCancelled          = "ORDER_CANCELLED"
	EventFlashSaleOrderConfirmed = "FLASH_SALE_ORDER_CONFIRMED"
	EventStockDeductedSuccess    = "STOCK_DEDUCTED_SUCCESS"
	EventStockDeductedFailed     = "STOCK_DEDUCTED_FAILED"
)

// Danh sách Consumer Group IDs chuẩn
const (
	ConsumerGroupProductStock          = "product-stock-consumer-group"
	ConsumerGroupOrderSaga             = "order-saga-consumer-group"
	ConsumerGroupOrderEmail            = "order-email-consumer-group"
	ConsumerGroupFlashSale             = "flashsale-order-consumer-group"
	ConsumerGroupProductFSConfirmation = "product-flashsale-confirmation-group"
	ConsumerGroupOrderRedisProjection  = "order-redis-projection-group"
	ConsumerGroupProductViews          = "product-view-counter-group"
)
