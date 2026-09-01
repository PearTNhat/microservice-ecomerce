import { apiClient } from "@/lib/api-client";
import { ApiResponse } from "@/types";
import { CreateOrderPayload, Order, OrderListResponse } from "../types";

export const orderService = {
  /**
   * Tạo đơn hàng từ giỏ hàng (kèm header X-Idempotency-Key chống click đúp)
   */
  async createOrder(
    payload: CreateOrderPayload,
    idempotencyKey?: string
  ): Promise<ApiResponse<Order>> {
    const key = idempotencyKey || (typeof crypto !== "undefined" && crypto.randomUUID ? crypto.randomUUID() : `ecom-${Date.now()}`);
    
    return apiClient<ApiResponse<Order>>("/orders/checkout", {
      method: "POST",
      headers: {
        "X-Idempotency-Key": key,
      },
      body: JSON.stringify(payload),
    });
  },

  /**
   * Mua ngay 1 sản phẩm trực tiếp (Direct checkout)
   */
  async createDirectOrder(
    payload: CreateOrderPayload,
    idempotencyKey?: string
  ): Promise<ApiResponse<Order>> {
    const key = idempotencyKey || (typeof crypto !== "undefined" && crypto.randomUUID ? crypto.randomUUID() : `ecom-${Date.now()}`);

    return apiClient<ApiResponse<Order>>("/orders/direct", {
      method: "POST",
      headers: {
        "X-Idempotency-Key": key,
      },
      body: JSON.stringify(payload),
    });
  },

  /**
   * Lấy danh sách lịch sử đơn hàng của người dùng hiện tại
   */
  async getUserOrders(page = 1, limit = 10): Promise<ApiResponse<OrderListResponse>> {
    return apiClient<ApiResponse<OrderListResponse>>("/orders", {
      method: "GET",
      params: { page, limit },
    });
  },

  /**
   * Lấy chi tiết một đơn hàng theo ID
   */
  async getOrderByID(orderID: number): Promise<ApiResponse<Order>> {
    return apiClient<ApiResponse<Order>>(`/orders/${orderID}`, {
      method: "GET",
    });
  },
};
