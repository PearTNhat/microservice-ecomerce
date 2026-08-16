package service

import (
	"context"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/dto"
	"ecomerce-service/pkg/kafka"
	"ecomerce-service/pkg/logger"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type ProductService struct {
	repo          domain.ProductRepository
	redisClient   *redis.Client
	kafkaProducer kafka.EventProducer
	sfGroup       singleflight.Group
}

func NewProductService(repo domain.ProductRepository, rClient *redis.Client, producer kafka.EventProducer) *ProductService {
	return &ProductService{
		repo:          repo,
		redisClient:   rClient,
		kafkaProducer: producer,
	}
}

// GetProducts lấy danh sách sản phẩm theo bộ lọc và phân trang
func (s *ProductService) GetProducts(filterReq dto.ProductFilterRequest) (*dto.PaginatedProductResponse, error) {
	filter := domain.ProductFilter{
		CategoryID: filterReq.CategoryID,
		BrandID:    filterReq.BrandID,
		MinPrice:   filterReq.MinPrice,
		MaxPrice:   filterReq.MaxPrice,
		Keyword:    filterReq.Keyword,
		SortBy:     filterReq.SortBy,
		Page:       filterReq.Page,
		Limit:      filterReq.Limit,
	}

	if filter.Page <= 0 {
		filter.Page = 1
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 12
	}

	products, total, err := s.repo.FindAll(filter)
	if err != nil {
		return nil, fmt.Errorf("không thể lấy danh sách sản phẩm: %w", err)
	}

	var productItems []*dto.ProductListItemResponse
	for _, p := range products {
		var catResp *dto.CategoryResponse
		if p.Category != nil {
			catResp = &dto.CategoryResponse{
				ID:   p.Category.ID,
				Name: p.Category.Name,
				Slug: p.Category.Slug,
				Icon: p.Category.Icon,
			}
		}

		var brandResp *dto.BrandResponse
		if p.Brand != nil {
			brandResp = &dto.BrandResponse{
				ID:   p.Brand.ID,
				Name: p.Brand.Name,
				Slug: p.Brand.Slug,
				Logo: p.Brand.Logo,
			}
		}

		productItems = append(productItems, &dto.ProductListItemResponse{
			ID:            p.ID,
			Name:          p.Name,
			Slug:          p.Slug,
			Price:         p.Price,
			DiscountPrice: p.DiscountPrice,
			Stock:         p.Stock,
			Thumbnail:     p.Thumbnail,
			Category:      catResp,
			Brand:         brandResp,
			Rating:        p.Rating,
			Views:         p.Views,
		})
	}

	totalPages := int(total) / filter.Limit
	if int(total)%filter.Limit > 0 {
		totalPages++
	}

	return &dto.PaginatedProductResponse{
		Total:      total,
		Page:       filter.Page,
		Limit:      filter.Limit,
		TotalPages: totalPages,
		Products:   productItems,
	}, nil
}

// GetProductDetail lấy chi tiết sản phẩm áp dụng Cache-Aside + Singleflight + Kafka Tracking
func (s *ProductService) GetProductDetail(ctx context.Context, id uint, userID string) (*dto.ProductDetailResponse, error) {
	cacheKey := fmt.Sprintf("cache:product:%d", id)

	// 1. Kiểm tra trong Redis Cache trước
	if s.redisClient != nil {
		cachedData, err := s.redisClient.Get(ctx, cacheKey).Result()
		if err == nil && cachedData != "" {
			var resp dto.ProductDetailResponse
			if err := json.Unmarshal([]byte(cachedData), &resp); err == nil {
				// Bắn event xem sản phẩm vào Kafka ngầm
				s.publishViewEvent(ctx, id, userID)
				return &resp, nil
			}
		}
	}

	// 2. Cache Miss: Áp dụng Singleflight để chỉ cho đúng 1 query đâm xuống Database
	sfKey := fmt.Sprintf("get_product_%d", id)
	v, err, _ := s.sfGroup.Do(sfKey, func() (interface{}, error) {
		p, err := s.repo.FindById(id)
		if err != nil {
			return nil, err
		}
		if p == nil {
			return nil, fmt.Errorf("sản phẩm không tồn tại")
		}

		var catResp *dto.CategoryResponse
		if p.Category != nil {
			catResp = &dto.CategoryResponse{
				ID:   p.Category.ID,
				Name: p.Category.Name,
				Slug: p.Category.Slug,
				Icon: p.Category.Icon,
			}
		}

		var brandResp *dto.BrandResponse
		if p.Brand != nil {
			brandResp = &dto.BrandResponse{
				ID:   p.Brand.ID,
				Name: p.Brand.Name,
				Slug: p.Brand.Slug,
				Logo: p.Brand.Logo,
			}
		}

		var images []string
		if p.Images != "" {
			images = strings.Split(p.Images, ",")
		}

		specs := dto.ParseSpecifications(p.Specifications)

		detail := &dto.ProductDetailResponse{
			ID:             p.ID,
			Name:           p.Name,
			Slug:           p.Slug,
			Description:    p.Description,
			Price:          p.Price,
			DiscountPrice:  p.DiscountPrice,
			Stock:          p.Stock,
			Thumbnail:      p.Thumbnail,
			Images:         images,
			Specifications: specs,
			Category:       catResp,
			Brand:          brandResp,
			Rating:         p.Rating,
			Views:          p.Views + 1,
		}

		// 3. Lưu vào Redis với TTL 30 phút
		if s.redisClient != nil {
			if bytesData, err := json.Marshal(detail); err == nil {
				s.redisClient.Set(context.Background(), cacheKey, bytesData, 30*time.Minute)
			}
		}

		// Tăng lượt view trong DB
		go s.repo.IncrementViews(id)

		return detail, nil
	})

	if err != nil {
		return nil, err
	}

	// 4. Bắn event xem sản phẩm vào Kafka
	s.publishViewEvent(ctx, id, userID)

	return v.(*dto.ProductDetailResponse), nil
}

func (s *ProductService) publishViewEvent(ctx context.Context, productID uint, userID string) {
	if s.kafkaProducer != nil {
		go func() {
			if err := s.kafkaProducer.PublishProductViewed(ctx, productID, userID); err != nil {
				logger.WarnContext(ctx, "Không thể bắn event view vào Kafka", "error", err.Error())
			}
		}()
	}
}

// GetCategories lấy danh mục sản phẩm (có Cache Redis 1 giờ)
func (s *ProductService) GetCategories(ctx context.Context) ([]*dto.CategoryResponse, error) {
	cacheKey := "cache:categories"

	if s.redisClient != nil {
		val, err := s.redisClient.Get(ctx, cacheKey).Result()
		if err == nil && val != "" {
			var cachedCategories []*dto.CategoryResponse
			if err := json.Unmarshal([]byte(val), &cachedCategories); err == nil {
				return cachedCategories, nil
			}
		}
	}

	categories, err := s.repo.FindCategories()
	if err != nil {
		return nil, err
	}

	var resp []*dto.CategoryResponse
	for _, c := range categories {
		resp = append(resp, &dto.CategoryResponse{
			ID:       c.ID,
			Name:     c.Name,
			Slug:     c.Slug,
			ParentID: c.ParentID,
			Icon:     c.Icon,
		})
	}

	if s.redisClient != nil {
		if bytesData, err := json.Marshal(resp); err == nil {
			s.redisClient.Set(context.Background(), cacheKey, bytesData, 1*time.Hour)
		}
	}

	return resp, nil
}

// GetBrands lấy danh sách thương hiệu (có Cache Redis 1 giờ)
func (s *ProductService) GetBrands(ctx context.Context) ([]*dto.BrandResponse, error) {
	cacheKey := "cache:brands"

	if s.redisClient != nil {
		val, err := s.redisClient.Get(ctx, cacheKey).Result()
		if err == nil && val != "" {
			var cachedBrands []*dto.BrandResponse
			if err := json.Unmarshal([]byte(val), &cachedBrands); err == nil {
				return cachedBrands, nil
			}
		}
	}

	brands, err := s.repo.FindBrands()
	if err != nil {
		return nil, err
	}

	var resp []*dto.BrandResponse
	for _, b := range brands {
		resp = append(resp, &dto.BrandResponse{
			ID:   b.ID,
			Name: b.Name,
			Slug: b.Slug,
			Logo: b.Logo,
		})
	}

	if s.redisClient != nil {
		if bytesData, err := json.Marshal(resp); err == nil {
			s.redisClient.Set(context.Background(), cacheKey, bytesData, 1*time.Hour)
		}
	}

	return resp, nil
}
