export type OrderStatus =
  | "PENDING"
  | "CONFIRMED"
  | "PROCESSING"
  | "SHIPPING"
  | "COMPLETED"
  | "CANCELLED"
  | "COMPENSATING";

export type PaymentStatus = "PENDING" | "PAID" | "FAILED" | "REFUNDED";

export type PaymentMethod = "COD" | "VNPAY" | "MOMO" | "BANK_TRANSFER";

export interface OrderItem {
  id?: number;
  product_id: number;
  product_name: string;
  product_slug: string;
  thumbnail: string;
  price: number;
  quantity: number;
  subtotal: number;
}

export interface Order {
  id: number;
  order_code: string;
  user_id: string;
  customer_name: string;
  customer_email: string;
  customer_phone: string;
  shipping_address: string;
  note?: string;
  payment_method: PaymentMethod;
  payment_status: PaymentStatus;
  order_status: OrderStatus;
  total_amount: number;
  items: OrderItem[];
  created_at: string;
}

export interface CreateOrderItemPayload {
  product_id: number;
  quantity: number;
}

export interface CreateOrderPayload {
  customer_name: string;
  customer_email: string;
  customer_phone: string;
  shipping_address: string;
  note?: string;
  payment_method: PaymentMethod;
  from_cart?: boolean;
  quote_token?: string;
  items?: CreateOrderItemPayload[];
}

export interface OrderListResponse {
  orders: Order[];
  total: number;
  page: number;
  limit: number;
  total_pages: number;
}

export interface BasketQuoteItem {
  product_id: number;
  quantity: number;
}

export interface BasketQuoteRequest {
  items?: BasketQuoteItem[];
  from_cart?: boolean;
}

export interface QuoteLineDTO {
  product_id: number;
  product_name: string;
  quantity: number;
  unit_price: number;
  subtotal: number;
  is_flash_sale: boolean;
  campaign_id?: number;
  purchase_mode: "FLASH_SALE" | "REGULAR";
}

export interface BasketQuoteResponse {
  quote_token: string;
  total: number;
  expires_at: number;
  items: QuoteLineDTO[];
}

export interface PriceConflictItem {
  product_id: number;
  product_name: string;
  was_flash_sale: boolean;
  quoted_price: number;
  updated_price: number;
  reason: string;
}

export interface PriceConflictResponse {
  error_code: string;
  message: string;
  new_quote_token: string;
  new_total: number;
  affected_items: PriceConflictItem[];
  new_items?: QuoteLineDTO[];
}

