package kafka

// Danh sách các Kafka Topics chuẩn trong hệ thống Microservices
const (
	TopicOrderEvents     = "order.events"
	TopicStockEvents     = "stock.events"
	TopicFlashSaleOrders = "flashsale.orders"
	TopicProductViews    = "product-views"
	TopicOrdersDLT       = "orders.dead_letter"
)

// Các loại sự kiện (Event Types)
const (
	EventOrderCreated        = "ORDER_CREATED"
	EventOrderPaid           = "ORDER_PAID"
	EventOrderCancelled      = "ORDER_CANCELLED"
	EventStockDeductedSuccess = "STOCK_DEDUCTED_SUCCESS"
	EventStockDeductedFailed  = "STOCK_DEDUCTED_FAILED"
)

// Danh sách Consumer Group IDs chuẩn
const (
	ConsumerGroupProductStock = "product-stock-consumer-group"
	ConsumerGroupOrderSaga    = "order-saga-consumer-group"
	ConsumerGroupOrderEmail   = "order-email-consumer-group"
	ConsumerGroupFlashSale    = "flashsale-order-consumer-group"
	ConsumerGroupProductViews = "product-view-counter-group"
)
