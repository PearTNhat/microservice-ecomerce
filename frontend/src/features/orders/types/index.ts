export type OrderStatus =
  | "PENDING"
  | "CONFIRMED"
  | "PROCESSING"
  | "SHIPPING"
  | "COMPLETED"
  | "CANCELLED";

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
  items?: CreateOrderItemPayload[];
}

export interface OrderListResponse {
  orders: Order[];
  total: number;
  page: number;
  limit: number;
  total_pages: number;
}
