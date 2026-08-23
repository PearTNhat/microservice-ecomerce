package service

import (
	"context"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/internal/dto"
	"ecomerce-service/pkg/elasticsearch"
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
	esClient      elasticsearch.SearchClient
	sfGroup       singleflight.Group
}

func NewProductService(
	repo domain.ProductRepository,
	rClient *redis.Client,
	producer kafka.EventProducer,
	esClient elasticsearch.SearchClient,
) *ProductService {
	return &ProductService{
		repo:          repo,
		redisClient:   rClient,
		kafkaProducer: producer,
		esClient:      esClient,
	}
}

// GetProducts lấy danh sách sản phẩm (có tích hợp Elasticsearch Full-Text Search)
func (s *ProductService) GetProducts(ctx context.Context, filterReq dto.ProductFilterRequest) (*dto.PaginatedProductResponse, error) {
	page := filterReq.Page
	if page <= 0 {
		page = 1
	}
	limit := filterReq.Limit
	if limit <= 0 || limit > 100 {
		limit = 12
	}

	// 1. TÌM KIẾM BẰNG ELASTICSEARCH NẾU CÓ TỪ KHÓA
	if filterReq.Keyword != "" && s.esClient != nil {
		esIDs, err := s.esClient.SearchProducts(ctx, filterReq.Keyword, filterReq.MinPrice, filterReq.MaxPrice, limit)
		if err == nil && len(esIDs) > 0 {
			logger.InfoContext(ctx, "🔍 Tìm thấy sản phẩm từ Elasticsearch", "keyword", filterReq.Keyword, "count", len(esIDs))
			products, err := s.repo.FindByIds(esIDs)
			if err == nil && len(products) > 0 {
				return s.formatPaginatedResponse(products, int64(len(products)), page, limit), nil
			}
		}
	}

	// 2. FALLBACK VỀ POSTGRESQL NẾU KHÔNG DÙNG ES HOẶC ES TRỐNG
	filter := domain.ProductFilter{
		CategoryID: filterReq.CategoryID,
		BrandID:    filterReq.BrandID,
		MinPrice:   filterReq.MinPrice,
		MaxPrice:   filterReq.MaxPrice,
		Keyword:    filterReq.Keyword,
		SortBy:     filterReq.SortBy,
		Page:       page,
		Limit:      limit,
	}

	products, total, err := s.repo.FindAll(filter)
	if err != nil {
		return nil, fmt.Errorf("không thể lấy danh sách sản phẩm: %w", err)
	}

	return s.formatPaginatedResponse(products, total, page, limit), nil
}

func (s *ProductService) formatPaginatedResponse(products []*domain.Product, total int64, page, limit int) *dto.PaginatedProductResponse {
	productItems := make([]*dto.ProductListItemResponse, 0)
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

	totalPages := int(total) / limit
	if int(total)%limit > 0 {
		totalPages++
	}

	return &dto.PaginatedProductResponse{
		Total:      total,
		Page:       page,
		Limit:      limit,
		TotalPages: totalPages,
		Products:   productItems,
	}
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
