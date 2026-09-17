const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

interface FetchOptions extends RequestInit {
  params?: Record<string, string | number | boolean | undefined>;
}

export async function apiClient<T>(endpoint: string, options: FetchOptions = {}): Promise<T> {
  const { params, headers, ...customConfig } = options;

  let url = `${API_BASE_URL}${endpoint}`;
  if (params) {
    const searchParams = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== "") {
        searchParams.append(key, String(value));
      }
    });
    const queryString = searchParams.toString();
    if (queryString) {
      url += (url.includes("?") ? "&" : "?") + queryString;
    }
  }

  const defaultHeaders: Record<string, string> = {
    "Content-Type": "application/json",
  };

  // Tự động gắn token JWT nếu có trong localStorage (Client-side)
  if (typeof window !== "undefined") {
    const token = localStorage.getItem("access_token");
    if (token) {
      defaultHeaders["Authorization"] = `Bearer ${token}`;
    }
  }

  const config: RequestInit = {
    ...customConfig,
    headers: {
      ...defaultHeaders,
      ...headers,
    },
  };

  try {
    const response = await fetch(url, config);
    let data: any = null;
    try {
      data = await response.json();
    } catch {
      data = { message: response.statusText || `Lỗi máy chủ (${response.status})` };
    }

    if (!response.ok) {
      const error: any = new Error(data?.message || `Lỗi yêu cầu máy chủ: ${response.status}`);
      error.status = response.status;
      error.response = { status: response.status, data };
      error.code = data?.code;
      throw error;
    }

    return data;
  } catch (error) {
    if (error instanceof Error) {
      throw error;
    }
    throw new Error("Có lỗi xảy ra khi kết nối máy chủ");
  }
}
