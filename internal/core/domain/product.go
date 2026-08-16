package domain

import "time"

// Category đại diện cho danh mục sản phẩm (Điện Lạnh, Thiết Bị Nhà Bếp, Gia Dụng...)
type Category struct {
	ID        uint      `json:"id" gorm:"primaryKey"`
	Name      string    `json:"name" gorm:"not null"`
	Slug      string    `json:"slug" gorm:"uniqueIndex;not null"`
	ParentID  *uint     `json:"parent_id,omitempty"`
	Icon      string    `json:"icon,omitempty"`
	CreatedAt time.Time `json:"created_at" gorm:"default:current_timestamp"`
	UpdatedAt time.Time `json:"updated_at" gorm:"default:current_timestamp"`
}

// Brand đại diện cho thương hiệu điện máy (Daikin, Panasonic, Samsung, LG, Bosch, Electrolux...)
type Brand struct {
	ID        uint      `json:"id" gorm:"primaryKey"`
	Name      string    `json:"name" gorm:"not null"`
	Slug      string    `json:"slug" gorm:"uniqueIndex;not null"`
	Logo      string    `json:"logo,omitempty"`
	CreatedAt time.Time `json:"created_at" gorm:"default:current_timestamp"`
	UpdatedAt time.Time `json:"updated_at" gorm:"default:current_timestamp"`
}

// Product đại diện cho sản phẩm điện máy & gia dụng
type Product struct {
	ID             uint      `json:"id" gorm:"primaryKey"`
	Name           string    `json:"name" gorm:"not null;index"`
	Slug           string    `json:"slug" gorm:"uniqueIndex;not null"`
	Description    string    `json:"description" gorm:"type:text"`
	Price          float64   `json:"price" gorm:"not null;index"`
	DiscountPrice  float64   `json:"discount_price,omitempty"`
	Stock          int       `json:"stock" gorm:"not null;default:0"`
	CategoryID     uint      `json:"category_id" gorm:"not null;index"`
	Category       *Category `json:"category,omitempty" gorm:"foreignKey:CategoryID"`
	BrandID        uint      `json:"brand_id" gorm:"not null;index"`
	Brand          *Brand    `json:"brand,omitempty" gorm:"foreignKey:BrandID"`
	Thumbnail      string    `json:"thumbnail,omitempty"`
	Images         string    `json:"images,omitempty" gorm:"type:text"` // Danh sách ảnh (JSON / URLs)
	Specifications string    `json:"specifications,omitempty" gorm:"type:jsonb"` // Thông số kỹ thuật động JSONB
	Rating         float64   `json:"rating" gorm:"default:5.0"`
	Views          int64     `json:"views" gorm:"default:0"`
	CreatedAt      time.Time `json:"created_at" gorm:"default:current_timestamp"`
	UpdatedAt      time.Time `json:"updated_at" gorm:"default:current_timestamp"`
}

// ProductFilter chứa các tiêu chí lọc sản phẩm
type ProductFilter struct {
	CategoryID uint
	BrandID    uint
	MinPrice   float64
	MaxPrice   float64
	Keyword    string
	SortBy     string // price_asc, price_desc, newest, views
	Page       int
	Limit      int
}

// ProductRepository định nghĩa interface thao tác dữ liệu sản phẩm
type ProductRepository interface {
	FindAll(filter ProductFilter) ([]*Product, int64, error)
	FindById(id uint) (*Product, error)
	FindBySlug(slug string) (*Product, error)
	FindCategories() ([]*Category, error)
	FindBrands() ([]*Brand, error)
	CreateProduct(product *Product) error
	CreateCategory(category *Category) error
	CreateBrand(brand *Brand) error
	IncrementViews(id uint) error
	Count() (int64, error)
}
