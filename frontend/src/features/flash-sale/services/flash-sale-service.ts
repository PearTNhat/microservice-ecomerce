import { apiClient } from "@/lib/api-client";
import { ApiResponse } from "@/types";
import {
  ActiveCampaign,
  AdminAddCampaignItemPayload,
  AdminCampaign,
  AdminCampaignItem,
  AdminCampaignListResponse,
  AdminCreateCampaignPayload,
  BatchOfferResponse,
  CreateFlashSaleOrderPayload,
  FlashSaleOrderStatus,
  FlashSaleReservationResponse,
  ProductOfferResponse,
} from "../types";

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export const flashSaleService = {
  /**
   * Lấy thông tin chiến dịch Flash Sale đang diễn ra (Công khai cho toàn bộ khách hàng)
   */
  async getActiveCampaign(): Promise<ApiResponse<ActiveCampaign | null>> {
    return apiClient<ApiResponse<ActiveCampaign | null>>("/flash-sales/active", {
      method: "GET",
    });
  },

  /**
   * Lấy thông tin ưu đãi Flash Sale cho 1 sản phẩm
   */
  async getProductOffer(productId: number): Promise<ApiResponse<ProductOfferResponse>> {
    return apiClient<ApiResponse<ProductOfferResponse>>(`/flash-sales/offers/${productId}`, {
      method: "GET",
    });
  },

  /**
   * Lấy danh sách ưu đãi Flash Sale theo lô (Batch)
   */
  async getBatchOffers(productIds: number[]): Promise<ApiResponse<BatchOfferResponse>> {
    if (!productIds || productIds.length === 0) {
      return { status: 200, message: "OK", data: { offers: {} } } as unknown as ApiResponse<BatchOfferResponse>;
    }
    return apiClient<ApiResponse<BatchOfferResponse>>("/flash-sales/offers/batch", {
      method: "POST",
      body: JSON.stringify({ product_ids: productIds }),
    });
  },

  /**
   * Khách hàng đặt mua Flash Sale qua Hot Path (kèm header Idempotency-Key UUID v4)
   */
  async reserveOrder(
    campaignId: number,
    productId: number,
    payload: CreateFlashSaleOrderPayload,
    idempotencyKey?: string
  ): Promise<ApiResponse<FlashSaleReservationResponse>> {
    const key =
      idempotencyKey ||
      (typeof crypto !== "undefined" && crypto.randomUUID
        ? crypto.randomUUID()
        : `fs-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`);

    return apiClient<ApiResponse<FlashSaleReservationResponse>>(
      `/flash-sales/${campaignId}/items/${productId}/orders`,
      {
        method: "POST",
        headers: {
          "Idempotency-Key": key,
        },
        body: JSON.stringify(payload),
      }
    );
  },

  /**
   * Polling kiểm tra trạng thái đơn hàng từ RAM Redis
   */
  async getOrderStatus(
    reservationId: string
  ): Promise<ApiResponse<FlashSaleOrderStatus>> {
    return apiClient<ApiResponse<FlashSaleOrderStatus>>(
      `/flash-sales/orders/${reservationId}`,
      {
        method: "GET",
      }
    );
  },

  /**
   * Mở kết nối Server-Sent Events (SSE) để nhận kết quả xác nhận đơn hàng realtime
   */
  createOrderStatusEventSource(reservationId: string): EventSource | null {
    if (typeof window === "undefined" || !window.EventSource) {
      return null;
    }
    const url = `${API_BASE_URL}/flash-sales/orders/${reservationId}/stream`;
    return new EventSource(url);
  },

  // ==================== ADMIN APIS ====================

  /**
   * Lấy danh sách các chiến dịch Flash Sale
   */
  async listCampaigns(
    status?: string,
    page = 1,
    limit = 10
  ): Promise<ApiResponse<AdminCampaignListResponse>> {
    return apiClient<ApiResponse<AdminCampaignListResponse>>("/admin/flash-sales", {
      method: "GET",
      params: { status, page, limit },
    });
  },

  /**
   * Xem chi tiết chiến dịch Flash Sale
   */
  async getCampaign(campaignId: number): Promise<ApiResponse<AdminCampaign>> {
    return apiClient<ApiResponse<AdminCampaign>>(`/admin/flash-sales/${campaignId}`, {
      method: "GET",
    });
  },

  /**
   * Tạo chiến dịch Flash Sale mới
   */
  async createCampaign(
    payload: AdminCreateCampaignPayload
  ): Promise<ApiResponse<AdminCampaign>> {
    return apiClient<ApiResponse<AdminCampaign>>("/admin/flash-sales", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  },

  /**
   * Thêm sản phẩm vào chiến dịch (cấu hình tồn kho phân bổ, giá sale và hạn mức)
   */
  async addCampaignItem(
    campaignId: number,
    payload: AdminAddCampaignItemPayload
  ): Promise<ApiResponse<AdminCampaignItem>> {
    return apiClient<ApiResponse<AdminCampaignItem>>(
      `/admin/flash-sales/${campaignId}/items`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      }
    );
  },

  /**
   * Kích hoạt chiến dịch (Saga phân bổ tồn kho Product Service & Prewarm Redis)
   */
  async activateCampaign(campaignId: number): Promise<ApiResponse<null>> {
    return apiClient<ApiResponse<null>>(`/admin/flash-sales/${campaignId}/activate`, {
      method: "POST",
    });
  },

  /**
   * Nhân bản cấu hình chiến dịch sang đợt mới (Clone Campaign)
   */
  async cloneCampaign(campaignId: number): Promise<ApiResponse<AdminCampaign>> {
    return apiClient<ApiResponse<AdminCampaign>>(`/admin/flash-sales/${campaignId}/clone`, {
      method: "POST",
    });
  },

  /**
   * Kết thúc chiến dịch sớm và hoàn trả tồn kho thừa về Product Service
   */
  async endCampaign(campaignId: number): Promise<ApiResponse<null>> {
    return apiClient<ApiResponse<null>>(`/admin/flash-sales/${campaignId}/end`, {
      method: "POST",
    });
  },
};
