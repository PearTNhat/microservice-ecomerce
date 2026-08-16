import { apiClient } from "@/lib/api-client";
import { ApiResponse } from "@/types";
import { Brand, Category, PaginatedProducts, Product, ProductFilterParams } from "../types";

export const productService = {
  async getProducts(params?: ProductFilterParams): Promise<ApiResponse<PaginatedProducts>> {
    return apiClient<ApiResponse<PaginatedProducts>>("/products", {
      method: "GET",
      params: params as any,
      next: { revalidate: 60 }, // Cache SSR trong 60 giây
    });
  },

  async getProductDetail(id: number): Promise<ApiResponse<Product>> {
    return apiClient<ApiResponse<Product>>(`/products/${id}`, {
      method: "GET",
      next: { revalidate: 60 },
    });
  },

  async getCategories(): Promise<ApiResponse<Category[]>> {
    return apiClient<ApiResponse<Category[]>>("/categories", {
      method: "GET",
      next: { revalidate: 3600 },
    });
  },

  async getBrands(): Promise<ApiResponse<Brand[]>> {
    return apiClient<ApiResponse<Brand[]>>("/brands", {
      method: "GET",
      next: { revalidate: 3600 },
    });
  },
};
