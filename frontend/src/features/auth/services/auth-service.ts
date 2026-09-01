import { apiClient } from "@/lib/api-client";
import { ApiResponse } from "@/types";
import { AuthResponseData, LoginCredentials, RegisterInput, User, VerifyOtpInput } from "../types";

export const authService = {
  async register(data: RegisterInput): Promise<ApiResponse<{ message: string }>> {
    return apiClient<ApiResponse<{ message: string }>>("/register", {
      method: "POST",
      body: JSON.stringify(data),
    });
  },

  async verifyEmail(data: VerifyOtpInput): Promise<ApiResponse<{ message: string }>> {
    return apiClient<ApiResponse<{ message: string }>>("/verify-email", {
      method: "POST",
      body: JSON.stringify({
        email: data.email,
        code: parseInt(data.code, 10),
      }),
    });
  },

  async login(credentials: LoginCredentials): Promise<ApiResponse<AuthResponseData>> {
    return apiClient<ApiResponse<AuthResponseData>>("/login", {
      method: "POST",
      body: JSON.stringify(credentials),
    });
  },

  async getProfile(): Promise<ApiResponse<User>> {
    return apiClient<ApiResponse<User>>("/user/profile", {
      method: "GET",
    });
  },
};
