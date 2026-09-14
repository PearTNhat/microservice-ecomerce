export interface ActiveCampaignItem {
  id: number;
  campaign_id: number;
  product_id: number;
  product_name: string;
  product_thumbnail?: string;
  sale_price: number;
  original_price: number;
  discount_percentage: number;
  allocated_stock: number;
  reserved_stock: number;
  sold_stock: number;
  remaining_stock: number;
  max_quantity_per_user: number; // 1: Deal sốc (1 lần); >1: Mua nhiều lần; 0: không giới hạn
  max_quantity_per_order: number;
  reservation_seconds: number;
}

export interface ActiveCampaign {
  id: number;
  name: string;
  description: string;
  starts_at: string;
  ends_at: string;
  status: "DRAFT" | "ALLOCATING" | "ACTIVE" | "PAUSED" | "ENDED" | "CANCELLED" | "ACTIVATION_FAILED";
  remaining_seconds: number;
  items: ActiveCampaignItem[];
}

export interface FlashSaleReservationResponse {
  reservation_id: string;
  status: "RESERVED" | "CONFIRMED" | "FAILED" | "EXPIRED" | "CANCELLED";
  expires_at: string;
  status_url: string;
  stream_url: string;
}

export interface FlashSaleOrderStatus {
  reservation_id: string;
  status: "RESERVED" | "CONFIRMED" | "FAILED" | "EXPIRED" | "CANCELLED";
  order_id?: number;
  order_code?: string;
  expires_at: string;
  failure_reason?: string;
  updated_at: string;
}

export interface CreateFlashSaleOrderPayload {
  quantity: number;
  payment_method: "COD" | "VNPAY" | "MOMO" | "BANK_TRANSFER";
  customer_name: string;
  customer_email: string;
  customer_phone: string;
  shipping_address: string;
}

// ---------------- Admin Types ----------------
export interface AdminCampaignItem {
  id: number;
  campaign_id: number;
  product_id: number;
  sale_price: number;
  original_price: number;
  allocated_stock: number;
  reserved_stock: number;
  sold_stock: number;
  max_quantity_per_user: number;
  max_quantity_per_order: number;
  reservation_seconds: number;
}

export interface AdminCampaign {
  id: number;
  name: string;
  description: string;
  starts_at: string;
  ends_at: string;
  status: "DRAFT" | "ALLOCATING" | "ACTIVE" | "PAUSED" | "ENDED" | "CANCELLED" | "ACTIVATION_FAILED";
  items?: AdminCampaignItem[];
  created_at: string;
  updated_at: string;
}

export interface AdminCreateCampaignPayload {
  name: string;
  description?: string;
  starts_at: string;
  ends_at: string;
}

export interface AdminAddCampaignItemPayload {
  product_id: number;
  sale_price: number;
  original_price: number;
  allocated_stock: number;
  max_quantity_per_user: number;
  max_quantity_per_order: number;
  reservation_seconds?: number;
}

export interface AdminCampaignListResponse {
  campaigns: AdminCampaign[];
  total: number;
  page: number;
  limit: number;
}

