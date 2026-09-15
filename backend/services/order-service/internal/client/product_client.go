package client

import (
	"context"
	"ecomerce-service/pkg/logger"
	"ecomerce-service/services/order-service/internal/dto"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ProductClient interface cho các service khác gọi sang Product Service mà không cần đụng DB
type ProductClient interface {
	GetProduct(ctx context.Context, productID uint) (*dto.ProductDetailResponse, error)
	AllocateFlashSaleStock(ctx context.Context, campaignID, productID uint, requestID string, quantity int) error
	ReleaseFlashSaleStock(ctx context.Context, campaignID, productID uint, requestID string) error
	GetStockAllocation(ctx context.Context, campaignID, productID uint) (*dto.StockAllocationResponse, error)
}

type productClient struct {
	baseURL     string
	redisClient *redis.Client
	httpClient  *http.Client
}

// NewProductClient khởi tạo Internal Client gọi sang Product Service
func NewProductClient(baseURL string, redisClient *redis.Client) ProductClient {
	if baseURL == "" {
		baseURL = "http://localhost:8002"
	}

	return &productClient{
		baseURL:     baseURL,
		redisClient: redisClient,
		httpClient: &http.Client{
			Timeout: 3 * time.Second, // Timeout ngắn 3s tránh nghẽn luồng
		},
	}
}

func (c *productClient) GetProduct(ctx context.Context, productID uint) (*dto.ProductDetailResponse, error) {
	cacheKey := fmt.Sprintf("cache:product:%d", productID)

	// 1. Kiểm tra Redis Cache trước (Fast path < 1ms)
	if c.redisClient != nil {
		cachedJSON, err := c.redisClient.Get(ctx, cacheKey).Result()
		if err == nil && cachedJSON != "" {
			var prod dto.ProductDetailResponse
			if err := json.Unmarshal([]byte(cachedJSON), &prod); err == nil && prod.ID > 0 {
				return &prod, nil
			}
		}
	}

	// 2. Cache Miss: Gọi HTTP REST sang Product Service (:8002)
	reqURL := fmt.Sprintf("%s/products/%d", c.baseURL, productID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("lỗi khởi tạo request tới Product Service: %w", err)
	}

	traceID := logger.GetTraceID(ctx)
	if traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("không thể kết nối tới Product Service (%s): %w", reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("sản phẩm #%d không tồn tại", productID)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Product Service trả về lỗi HTTP %d", resp.StatusCode)
	}

	var apiResp struct {
		Status  string                     `json:"status"`
		Message string                     `json:"message"`
		Data    *dto.ProductDetailResponse `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("lỗi parse JSON từ Product Service: %w", err)
	}

	if apiResp.Data == nil || apiResp.Data.ID == 0 {
		return nil, fmt.Errorf("dữ liệu sản phẩm #%d không hợp lệ", productID)
	}

	// 3. Nạp lại vào Redis Cache để các request sau dùng lại (TTL 15 phút)
	if c.redisClient != nil {
		if b, err := json.Marshal(apiResp.Data); err == nil {
			_ = c.redisClient.Set(ctx, cacheKey, b, 15*time.Minute)
		}
	}

	return apiResp.Data, nil
}

func (c *productClient) AllocateFlashSaleStock(ctx context.Context, campaignID, productID uint, requestID string, quantity int) error {
	reqURL := fmt.Sprintf("%s/internal/stock-allocations", c.baseURL)
	body := fmt.Sprintf(`{"campaign_id":%d,"product_id":%d,"quantity":%d}`, campaignID, productID, quantity)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", requestID)
	if traceID := logger.GetTraceID(ctx); traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("không thể kết nối tới Product Service (%s): %w", reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp struct {
			Message string `json:"message"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("lỗi allocate từ Product Service (HTTP %d): %s", resp.StatusCode, errResp.Message)
	}

	return nil
}

func (c *productClient) ReleaseFlashSaleStock(ctx context.Context, campaignID, productID uint, requestID string) error {
	reqURL := fmt.Sprintf("%s/internal/stock-allocations/%d/%d/release", c.baseURL, campaignID, productID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Idempotency-Key", requestID)
	if traceID := logger.GetTraceID(ctx); traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("không thể kết nối tới Product Service (%s): %w", reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp struct {
			Message string `json:"message"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("lỗi release từ Product Service (HTTP %d): %s", resp.StatusCode, errResp.Message)
	}

	return nil
}

func (c *productClient) GetStockAllocation(ctx context.Context, campaignID, productID uint) (*dto.StockAllocationResponse, error) {
	reqURL := fmt.Sprintf("%s/internal/stock-allocations/%d/%d", c.baseURL, campaignID, productID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	if traceID := logger.GetTraceID(ctx); traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("không thể kết nối tới Product Service (%s): %w", reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp struct {
			Message string `json:"message"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("lỗi lấy thông tin allocation từ Product Service (HTTP %d): %s", resp.StatusCode, errResp.Message)
	}

	var apiResp struct {
		Success bool                         `json:"success"`
		Data    *dto.StockAllocationResponse `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("lỗi parse json allocation response: %w", err)
	}

	return apiResp.Data, nil
}

