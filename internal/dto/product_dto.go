package dto

import "encoding/json"

// ProductFilterRequest nhận query parameters từ HTTP Request
type ProductFilterRequest struct {
	CategoryID uint    `query:"category_id"`
	BrandID    uint    `query:"brand_id"`
	MinPrice   float64 `query:"min_price"`
	MaxPrice   float64 `query:"max_price"`
	Keyword    string  `query:"keyword"`
	SortBy     string  `query:"sort_by"` // price_asc, price_desc, newest, views
	Page       int     `query:"page"`
	Limit      int     `query:"limit"`
}

// CategoryResponse định dạng trả về cho danh mục
type CategoryResponse struct {
	ID       uint                `json:"id"`
	Name     string              `json:"name"`
	Slug     string              `json:"slug"`
	ParentID *uint               `json:"parent_id,omitempty"`
	Icon     string              `json:"icon,omitempty"`
	Children []*CategoryResponse `json:"children,omitempty"`
}

// BrandResponse định dạng trả về cho thương hiệu
type BrandResponse struct {
	ID   uint   `json:"id"`
	Name string `json:"name"`
	Slug string `json:"slug"`
	Logo string `json:"logo,omitempty"`
}

// ProductListItemResponse định dạng sản phẩm trong danh sách
type ProductListItemResponse struct {
	ID            uint              `json:"id"`
	Name          string            `json:"name"`
	Slug          string            `json:"slug"`
	Price         float64           `json:"price"`
	DiscountPrice float64           `json:"discount_price,omitempty"`
	Stock         int               `json:"stock"`
	Thumbnail     string            `json:"thumbnail,omitempty"`
	Category      *CategoryResponse `json:"category,omitempty"`
	Brand         *BrandResponse    `json:"brand,omitempty"`
	Rating        float64           `json:"rating"`
	Views         int64             `json:"views"`
}

// ProductDetailResponse định dạng chi tiết sản phẩm kèm thông số kỹ thuật điện máy
type ProductDetailResponse struct {
	ID             uint                   `json:"id"`
	Name           string                 `json:"name"`
	Slug           string                 `json:"slug"`
	Description    string                 `json:"description"`
	Price          float64                `json:"price"`
	DiscountPrice  float64                `json:"discount_price,omitempty"`
	Stock          int                    `json:"stock"`
	Thumbnail      string                 `json:"thumbnail,omitempty"`
	Images         []string               `json:"images,omitempty"`
	Specifications map[string]interface{} `json:"specifications,omitempty"`
	Category       *CategoryResponse      `json:"category,omitempty"`
	Brand          *BrandResponse         `json:"brand,omitempty"`
	Rating         float64                `json:"rating"`
	Views          int64                  `json:"views"`
}

// PaginatedProductResponse định dạng danh sách sản phẩm phân trang
type PaginatedProductResponse struct {
	Total       int64                      `json:"total"`
	Page        int                        `json:"page"`
	Limit       int                        `json:"limit"`
	TotalPages  int                        `json:"total_pages"`
	Products    []*ProductListItemResponse `json:"products"`
}

// Helper chuyển đổi Specifications JSON string sang Map
func ParseSpecifications(rawJSON string) map[string]interface{} {
	if rawJSON == "" {
		return make(map[string]interface{})
	}
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(rawJSON), &result); err != nil {
		return make(map[string]interface{})
	}
	return result
}
