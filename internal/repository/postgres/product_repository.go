package postgres

import (
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/pkg/logger"
	"errors"

	"gorm.io/gorm"
)

type productRepository struct {
	db *gorm.DB
}

func NewProductRepository(db *gorm.DB) domain.ProductRepository {
	return &productRepository{
		db: db,
	}
}

func (r *productRepository) FindAll(filter domain.ProductFilter) ([]*domain.Product, int64, error) {
	var products []*domain.Product
	var total int64

	query := r.db.Model(&domain.Product{})

	// 1. Áp dụng bộ lọc
	if filter.CategoryID > 0 {
		query = query.Where("category_id = ?", filter.CategoryID)
	}

	if filter.BrandID > 0 {
		query = query.Where("brand_id = ?", filter.BrandID)
	}

	if filter.MinPrice > 0 {
		query = query.Where("price >= ?", filter.MinPrice)
	}

	if filter.MaxPrice > 0 {
		query = query.Where("price <= ?", filter.MaxPrice)
	}

	if filter.Keyword != "" {
		keyword := "%" + filter.Keyword + "%"
		query = query.Where("name ILIKE ? OR description ILIKE ?", keyword, keyword)
	}

	// 2. Đếm tổng số bản ghi
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// 3. Sắp xếp (Sorting)
	switch filter.SortBy {
	case "price_asc":
		query = query.Order("price ASC")
	case "price_desc":
		query = query.Order("price DESC")
	case "views":
		query = query.Order("views DESC")
	default:
		query = query.Order("created_at DESC")
	}

	// 4. Phân trang (Pagination)
	page := filter.Page
	if page <= 0 {
		page = 1
	}
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 12
	}
	offset := (page - 1) * limit

	err := query.Preload("Category").Preload("Brand").Offset(offset).Limit(limit).Find(&products).Error
	if err != nil {
		return nil, 0, err
	}

	return products, total, nil
}

func (r *productRepository) FindById(id uint) (*domain.Product, error) {
	var product domain.Product
	err := r.db.Preload("Category").Preload("Brand").First(&product, id).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &product, nil
}

func (r *productRepository) FindBySlug(slug string) (*domain.Product, error) {
	var product domain.Product
	err := r.db.Preload("Category").Preload("Brand").Where("slug = ?", slug).First(&product).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &product, nil
}

func (r *productRepository) FindCategories() ([]*domain.Category, error) {
	var categories []*domain.Category
	err := r.db.Order("name ASC").Find(&categories).Error
	return categories, err
}

func (r *productRepository) FindBrands() ([]*domain.Brand, error) {
	var brands []*domain.Brand
	err := r.db.Order("name ASC").Find(&brands).Error
	return brands, err
}

func (r *productRepository) CreateProduct(product *domain.Product) error {
	return r.db.Create(product).Error
}

func (r *productRepository) CreateCategory(category *domain.Category) error {
	return r.db.Create(category).Error
}

func (r *productRepository) CreateBrand(brand *domain.Brand) error {
	return r.db.Create(brand).Error
}

func (r *productRepository) IncrementViews(id uint) error {
	return r.db.Model(&domain.Product{}).Where("id = ?", id).
		UpdateColumn("views", gorm.Expr("views + 1")).Error
}

func (r *productRepository) Count() (int64, error) {
	var count int64
	err := r.db.Model(&domain.Product{}).Count(&count).Error
	return count, err
}

// SeedSampleData tạo dữ liệu mẫu thực tế về sản phẩm điện máy nếu DB chưa có
func SeedSampleData(db *gorm.DB) {
	var count int64
	db.Model(&domain.Category{}).Count(&count)
	if count > 0 {
		return // Đã có dữ liệu
	}

	logger.Info("🌱 Bắt đầu tạo dữ liệu mẫu cho danh mục, thương hiệu và sản phẩm điện máy...")

	// 1. Tạo Danh Mục
	catAirConditioner := domain.Category{Name: "Máy Lạnh - Điều Hòa", Slug: "may-lanh-dieu-hoa", Icon: "ac_unit"}
	catFridge := domain.Category{Name: "Tủ Lạnh", Slug: "tu-lanh", Icon: "kitchen"}
	catWashingMachine := domain.Category{Name: "Máy Giặt - Máy Sấy", Slug: "may-giat-may-say", Icon: "local_laundry_service"}
	catKitchen := domain.Category{Name: "Bếp Từ - Thiết Bị Bếp", Slug: "bep-tu-thiet-bi-bep", Icon: "countertops"}
	catTV := domain.Category{Name: "Smart Tivi", Slug: "smart-tivi", Icon: "tv"}

	db.Create(&catAirConditioner)
	db.Create(&catFridge)
	db.Create(&catWashingMachine)
	db.Create(&catKitchen)
	db.Create(&catTV)

	// 2. Tạo Thương Hiệu
	brandDaikin := domain.Brand{Name: "Daikin", Slug: "daikin", Logo: "https://example.com/daikin.png"}
	brandPanasonic := domain.Brand{Name: "Panasonic", Slug: "panasonic", Logo: "https://example.com/panasonic.png"}
	brandSamsung := domain.Brand{Name: "Samsung", Slug: "samsung", Logo: "https://example.com/samsung.png"}
	brandLG := domain.Brand{Name: "LG", Slug: "lg", Logo: "https://example.com/lg.png"}
	brandBosch := domain.Brand{Name: "Bosch", Slug: "bosch", Logo: "https://example.com/bosch.png"}

	db.Create(&brandDaikin)
	db.Create(&brandPanasonic)
	db.Create(&brandSamsung)
	db.Create(&brandLG)
	db.Create(&brandBosch)

	// 3. Tạo Sản Phẩm Điện Máy với Thông số Kỹ thuật JSONB
	products := []domain.Product{
		{
			Name:           "Máy Lạnh Daikin Inverter 1.5 HP FTKF35XVMV",
			Slug:           "may-lanh-daikin-inverter-1-5-hp-ftkf35xvmv",
			Description:    "Máy lạnh Daikin 1.5 HP trang bị công nghệ lọc khí Streamer độc quyền, tiết kiệm điện Inverter và chuẩn làm lạnh nhanh Coanda.",
			Price:          13500000,
			DiscountPrice:  12190000,
			Stock:          50,
			CategoryID:     catAirConditioner.ID,
			BrandID:        brandDaikin.ID,
			Thumbnail:      "https://images.unsplash.com/photo-1621905251189-08b45d6a269e?w=600",
			Rating:         4.9,
			Views:          1250,
			Specifications: `{"btu":11900,"inverter":true,"power_hp":"1.5 HP","gas_type":"R32","power_consumption_kwh":"1.16 kWh","warranty_months":12,"compressor_warranty_years":5}`,
		},
		{
			Name:           "Máy Lạnh Panasonic Inverter 1.0 HP CU/CS-XPU9XKH-8",
			Slug:           "may-lanh-panasonic-inverter-1-0-hp-cu-cs-xpu9xkh-8",
			Description:    "Công nghệ Nanoe-X ức chế 99% vi khuẩn, virus và mùi ẩm mốc, làm lạnh cực nhanh với cánh đảo gió kép Aerowings.",
			Price:          11200000,
			DiscountPrice:  9990000,
			Stock:          40,
			CategoryID:     catAirConditioner.ID,
			BrandID:        brandPanasonic.ID,
			Thumbnail:      "https://images.unsplash.com/photo-1585338107529-13afc5f02586?w=600",
			Rating:         4.8,
			Views:          980,
			Specifications: `{"btu":9000,"inverter":true,"power_hp":"1.0 HP","gas_type":"R32","air_purification":"Nanoe-X","warranty_months":12}`,
		},
		{
			Name:           "Tủ Lạnh Samsung Inverter 488 Lít 4 Cửa RF48A4010B4",
			Slug:           "tu-lanh-samsung-inverter-488-lit-rf48a4010b4",
			Description:    "Tủ lạnh 4 cửa Multidoor sang trọng, 2 dàn lạnh độc lập Twin Cooling Plus giữ thực phẩm tươi ngon không bị lẫn mùi.",
			Price:          24900000,
			DiscountPrice:  18490000,
			Stock:          25,
			CategoryID:     catFridge.ID,
			BrandID:        brandSamsung.ID,
			Thumbnail:      "https://images.unsplash.com/photo-1571175443880-49e1d25b2bc5?w=600",
			Rating:         4.9,
			Views:          2100,
			Specifications: `{"capacity_liters":488,"door_type":"Multidoor 4 Cửa","inverter":true,"cooling_technology":"Twin Cooling Plus","dimensions_mm":"833 x 1793 x 740","warranty_months":24}`,
		},
		{
			Name:           "Bếp Từ Đôi Bosch Serie 4 PPI82560MS",
			Slug:           "bep-tu-doi-bosch-serie-4-ppi82560ms",
			Description:    "Bếp từ đôi Bosch nhập khẩu chính hãng, mặt kính Schott Ceran chịu nhiệt cao cấp, công suất gia nhiệt nhanh PowerBoost 3500W.",
			Price:          16500000,
			DiscountPrice:  13200000,
			Stock:          30,
			CategoryID:     catKitchen.ID,
			BrandID:        brandBosch.ID,
			Thumbnail:      "https://images.unsplash.com/photo-1556911220-e15b29be8c8f?w=600",
			Rating:         5.0,
			Views:          1450,
			Specifications: `{"total_power_w":3500,"cooking_zones":2,"glass_type":"Schott Ceran (Đức)","booster":true,"auto_cut_off":true,"warranty_months":24}`,
		},
		{
			Name:           "Smart Tivi Samsung 4K 65 Inch QLED QA65Q60B",
			Slug:           "smart-tivi-samsung-4k-65-inch-qled-qa65q60b",
			Description:    "Màn hình QLED hiển thị 100% dải màu Quantum Dot sống động, bộ xử lý Quantum 4K Lite tối ưu âm thanh và hình ảnh thông minh.",
			Price:          21900000,
			DiscountPrice:  16990000,
			Stock:          15,
			CategoryID:     catTV.ID,
			BrandID:        brandSamsung.ID,
			Thumbnail:      "https://images.unsplash.com/photo-1593784991095-a205069470b6?w=600",
			Rating:         4.8,
			Views:          3200,
			Specifications: `{"screen_size_inch":65,"resolution":"4K (3840 x 2160)","display_tech":"QLED Quantum Dot","operating_system":"Tizen OS","refresh_rate_hz":60,"warranty_months":24}`,
		},
	}

	for _, p := range products {
		db.Create(&p)
	}

	logger.Info("✅ Đã seed thành công dữ liệu mẫu đồ điện máy!")
}
