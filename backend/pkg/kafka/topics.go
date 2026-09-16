package kafka

// Danh sách các Kafka Topics chuẩn trong hệ thống Microservices
const (
	TopicOrderEvents             = "order.events"
	TopicStockEvents             = "stock.events"
	TopicFlashSaleOrders         = "flashsale.orders"
	TopicFlashSaleConfirmed      = "flashsale.confirmed"
	TopicMixedOrderStockRequest          = "order.mixed-stock.request"
	TopicMixedOrderStockResult           = "order.mixed-stock.result"
	TopicMixedOrderStockCompensate       = "order.mixed-stock.compensate"
	TopicMixedOrderStockCompensateResult = "order.mixed-stock.compensate-result"
	TopicProductViews                    = "product-views"
	TopicOrdersDLT                       = "orders.dead_letter"
)

// Các loại sự kiện (Event Types)
const (
	EventOrderCreated                  = "ORDER_CREATED"
	EventOrderPaid                     = "ORDER_PAID"
	EventOrderCancelled                = "ORDER_CANCELLED"
	EventFlashSaleOrderConfirmed       = "FLASH_SALE_ORDER_CONFIRMED"
	EventStockDeductedSuccess          = "STOCK_DEDUCTED_SUCCESS"
	EventStockDeductedFailed           = "STOCK_DEDUCTED_FAILED"
	EventMixedStockDeductRequest       = "MIXED_STOCK_DEDUCT_REQUEST"
	EventMixedStockDeductResult        = "MIXED_STOCK_DEDUCT_RESULT"
	EventMixedStockCompensate          = "MIXED_STOCK_COMPENSATE_REQUEST"
	EventMixedStockCompensateResult    = "MIXED_STOCK_COMPENSATE_RESULT"
)

// Danh sách Consumer Group IDs chuẩn
const (
	ConsumerGroupProductStock          = "product-stock-consumer-group"
	ConsumerGroupOrderSaga             = "order-saga-consumer-group"
	ConsumerGroupOrderEmail            = "order-email-consumer-group"
	ConsumerGroupFlashSale             = "flashsale-order-consumer-group"
	ConsumerGroupProductFSConfirmation = "product-flashsale-confirmation-group"
	ConsumerGroupOrderRedisProjection  = "order-redis-projection-group"
	ConsumerGroupMixedStockProduct     = "mixed-stock-product-consumer-group"
	ConsumerGroupMixedStockOrder       = "mixed-stock-order-consumer-group"
	ConsumerGroupMixedCompensateOrder  = "mixed-compensate-order-consumer-group"
	ConsumerGroupProductViews          = "product-view-counter-group"
)

