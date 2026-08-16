package elasticsearch

import (
	"bytes"
	"context"
	"ecomerce-service/internal/core/domain"
	"ecomerce-service/pkg/logger"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
)

const ProductIndex = "products"

// ProductDocument là cấu trúc tài liệu lưu trong Elasticsearch
type ProductDocument struct {
	ID             uint    `json:"id"`
	Name           string  `json:"name"`
	Slug           string  `json:"slug"`
	Description    string  `json:"description"`
	Price          float64 `json:"price"`
	DiscountPrice  float64 `json:"discount_price"`
	CategoryID     uint    `json:"category_id"`
	BrandID        uint    `json:"brand_id"`
	Specifications string  `json:"specifications"`
}

type SearchClient interface {
	IndexProduct(ctx context.Context, p *domain.Product) error
	SearchProducts(ctx context.Context, keyword string, minPrice, maxPrice float64, limit int) ([]uint, error)
	Close() error
}

type esClientImpl struct {
	client *elasticsearch.Client
}

func NewElasticsearchClient(addr string) SearchClient {
	if addr == "" {
		logger.Warn("⚠️ Elasticsearch address rỗng, sử dụng Noop Search Client")
		return &noopSearchClient{}
	}

	cfg := elasticsearch.Config{
		Addresses: []string{addr},
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		logger.Warn("⚠️ Không thể kết nối tới Elasticsearch", "addr", addr, "error", err.Error())
		return &noopSearchClient{}
	}

	// Ping thử Elasticsearch
	res, err := client.Info()
	if err != nil || res.IsError() {
		logger.Warn("⚠️ Elasticsearch chưa sẵn sàng (sẽ fallback sang PostgreSQL)", "error", err)
		return &noopSearchClient{}
	}
	defer res.Body.Close()

	logger.Info("✅ Đã kết nối thành công tới Elasticsearch 8!", "addr", addr)

	es := &esClientImpl{client: client}
	es.initIndex()

	return es
}

func (e *esClientImpl) initIndex() {
	// Kiểm tra xem index 'products' đã tồn tại chưa
	res, err := e.client.Indices.Exists([]string{ProductIndex})
	if err == nil && res.StatusCode == 200 {
		return
	}

	// Tạo index với mapping chuyên dụng cho tiếng Việt và thông số kỹ thuật
	mapping := `{
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0,
			"analysis": {
				"analyzer": {
					"vietnamese_analyzer": {
						"type": "standard"
					}
				}
			}
		},
		"mappings": {
			"properties": {
				"id": { "type": "keyword" },
				"name": { "type": "text", "analyzer": "vietnamese_analyzer", "boost": 3 },
				"slug": { "type": "keyword" },
				"description": { "type": "text", "analyzer": "vietnamese_analyzer" },
				"price": { "type": "double" },
				"category_id": { "type": "integer" },
				"brand_id": { "type": "integer" },
				"specifications": { "type": "text" }
			}
		}
	}`

	createRes, err := e.client.Indices.Create(ProductIndex, e.client.Indices.Create.WithBody(strings.NewReader(mapping)))
	if err != nil {
		logger.Warn("⚠️ Không thể tạo index Elasticsearch", "error", err.Error())
		return
	}
	defer createRes.Body.Close()

	logger.Info("✅ Đã khởi tạo Elasticsearch index 'products' thành công!")
}

func (e *esClientImpl) IndexProduct(ctx context.Context, p *domain.Product) error {
	doc := ProductDocument{
		ID:             p.ID,
		Name:           p.Name,
		Slug:           p.Slug,
		Description:    p.Description,
		Price:          p.Price,
		DiscountPrice:  p.DiscountPrice,
		CategoryID:     p.CategoryID,
		BrandID:        p.BrandID,
		Specifications: p.Specifications,
	}

	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	docID := strconv.Itoa(int(p.ID))
	res, err := e.client.Index(
		ProductIndex,
		bytes.NewReader(data),
		e.client.Index.WithDocumentID(docID),
		e.client.Index.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("lỗi khi index sản phẩm vào Elasticsearch: %s", res.Status())
	}

	return nil
}

func (e *esClientImpl) SearchProducts(ctx context.Context, keyword string, minPrice, maxPrice float64, limit int) ([]uint, error) {
	if limit <= 0 {
		limit = 12
	}

	// Xây dựng Elasticsearch DSL Query
	query := map[string]interface{}{
		"size": limit,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"multi_match": map[string]interface{}{
							"query":     keyword,
							"fields":    []string{"name^3", "description", "specifications"},
							"fuzziness": "AUTO",
						},
					},
				},
			},
		},
	}

	// Thêm bộ lọc giá nếu có
	if minPrice > 0 || maxPrice > 0 {
		rangeFilter := map[string]interface{}{}
		if minPrice > 0 {
			rangeFilter["gte"] = minPrice
		}
		if maxPrice > 0 {
			rangeFilter["lte"] = maxPrice
		}

		boolQuery := query["query"].(map[string]interface{})["bool"].(map[string]interface{})
		boolQuery["filter"] = []map[string]interface{}{
			{
				"range": map[string]interface{}{
					"price": rangeFilter,
				},
			},
		}
	}

	queryBody, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	res, err := e.client.Search(
		e.client.Search.WithIndex(ProductIndex),
		e.client.Search.WithBody(bytes.NewReader(queryBody)),
		e.client.Search.WithContext(ctx),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("lỗi tìm kiếm Elasticsearch: %s", res.Status())
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var searchRes struct {
		Hits struct {
			Hits []struct {
				ID string `json:"_id"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.Unmarshal(body, &searchRes); err != nil {
		return nil, err
	}

	var productIDs []uint
	for _, hit := range searchRes.Hits.Hits {
		id, err := strconv.ParseUint(hit.ID, 10, 32)
		if err == nil {
			productIDs = append(productIDs, uint(id))
		}
	}

	return productIDs, nil
}

func (e *esClientImpl) Close() error {
	return nil
}

// noopSearchClient cho fallback và unit tests
type noopSearchClient struct{}

func (n *noopSearchClient) IndexProduct(ctx context.Context, p *domain.Product) error {
	return nil
}

func (n *noopSearchClient) SearchProducts(ctx context.Context, keyword string, minPrice, maxPrice float64, limit int) ([]uint, error) {
	return nil, nil
}

func (n *noopSearchClient) Close() error {
	return nil
}

func NewNoopSearchClient() SearchClient {
	return &noopSearchClient{}
}
