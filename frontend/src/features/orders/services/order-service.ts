import { apiClient } from "@/lib/api-client";
import { ApiResponse } from "@/types";
import {
  CreateOrderPayload,
  FlashSaleAsyncResponse,
  FlashSaleOrderPayload,
  FlashSaleStatusResponse,
  Order,
  OrderListResponse,
} from "../types";

export const orderService = {
  /**
   * Tạo đơn hàng từ giỏ hàng (kèm header X-Idempotency-Key chống click đúp)
   */
  async createOrder(
    payload: CreateOrderPayload,
    idempotencyKey?: string
  ): Promise<ApiResponse<Order>> {
    const key =
      idempotencyKey ||
      (typeof crypto !== "undefined" && crypto.randomUUID
        ? crypto.randomUUID()
        : `ecom-${Date.now()}`);

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
    const key =
      idempotencyKey ||
      (typeof crypto !== "undefined" && crypto.randomUUID
        ? crypto.randomUUID()
        : `ecom-${Date.now()}`);

    return apiClient<ApiResponse<Order>>("/orders/direct", {
      method: "POST",
      headers: {
        "X-Idempotency-Key": key,
      },
      body: JSON.stringify(payload),
    });
  },

  /**
   * Đặt mua sản phẩm Flash Sale bất đồng bộ (trả về 202 Accepted + order_token)
   */
  async createFlashSaleOrder(
    payload: FlashSaleOrderPayload
  ): Promise<ApiResponse<FlashSaleAsyncResponse>> {
    return apiClient<ApiResponse<FlashSaleAsyncResponse>>("/orders/flash-sale", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  },

  /**
   * Polling kiểm tra trạng thái đơn hàng Flash Sale từ Redis RAM (Zero DB Hit)
   */
  async getFlashSaleStatus(
    orderToken: string
  ): Promise<ApiResponse<FlashSaleStatusResponse>> {
    return apiClient<ApiResponse<FlashSaleStatusResponse>>(
      `/orders/flash-sale/status/${orderToken}`,
      {
        method: "GET",
      }
    );
  },

  /**
   * Lấy danh sách lịch sử đơn hàng của người dùng hiện tại
   */
  async getUserOrders(
    page = 1,
    limit = 10
  ): Promise<ApiResponse<OrderListResponse>> {
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
