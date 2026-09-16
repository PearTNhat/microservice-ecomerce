package service

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"ecomerce-service/services/order-service/internal/dto"
)

var (
	ErrQuoteExpired   = errors.New("quote token đã hết hiệu lực, vui lòng cập nhật lại giá")
	ErrQuoteInvalid   = errors.New("quote token không hợp lệ hoặc bị can thiệp")
	ErrSecretInsecure = errors.New("quote secret quá ngắn hoặc không an toàn (tối thiểu 16 ký tự)")
)

// PriceConflictError trả về lỗi xung đột giá HTTP 409 khi giá thay đổi hoặc hết suất sale
type PriceConflictError struct {
	ErrorCode string
	Message   string
	Response  dto.PriceConflictResponse
}

func (e *PriceConflictError) Error() string {
	if len(e.Response.AffectedItems) > 0 {
		return fmt.Sprintf("%s: %s (reason: %s)", e.ErrorCode, e.Message, e.Response.AffectedItems[0].Reason)
	}
	return fmt.Sprintf("%s: %s", e.ErrorCode, e.Message)
}

type QuoteItem struct {
	ProductID    uint    `json:"product_id"`
	Quantity     int     `json:"quantity"`
	QuotedPrice  float64 `json:"quoted_price"`
	IsFlashSale  bool    `json:"is_flash_sale"`
	CampaignID   *uint   `json:"campaign_id,omitempty"`
	PurchaseMode string  `json:"purchase_mode"` // "FLASH_SALE" hoặc "REGULAR"
}

type QuotePayload struct {
	UserID    string      `json:"user_id"`
	Items     []QuoteItem `json:"items"`
	Total     float64     `json:"total"`
	IssuedAt  int64       `json:"issued_at"`
	ExpiresAt int64       `json:"expires_at"` // Unix timestamp
}

// GenerateQuoteToken tạo token ký HMAC-SHA256 cho giỏ hàng bằng secret cấu hình (18.3)
func GenerateQuoteToken(secret []byte, userID string, items []QuoteItem, total float64, ttl time.Duration) (string, error) {
	if len(secret) < 16 {
		return "", ErrSecretInsecure
	}
	if userID == "" {
		return "", errors.New("userID bắt buộc phải có để sinh quote token")
	}
	now := time.Now()
	payload := QuotePayload{
		UserID:    userID,
		Items:     items,
		Total:     total,
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(ttl).Unix(),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	dataB64 := base64.RawURLEncoding.EncodeToString(data)

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(dataB64))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return fmt.Sprintf("%s.%s", dataB64, sig), nil
}

// VerifyQuoteToken xác thực tính hợp lệ, thời hạn và danh tính người dùng của token (18.3)
func VerifyQuoteToken(secret []byte, tokenStr string, expectedUserID string) (*QuotePayload, error) {
	if len(secret) < 16 {
		return nil, ErrSecretInsecure
	}
	if tokenStr == "" {
		return nil, ErrQuoteInvalid
	}
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 2 {
		return nil, ErrQuoteInvalid
	}
	dataB64, sig := parts[0], parts[1]

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(dataB64))
	expectedSig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(sig), []byte(expectedSig)) {
		return nil, ErrQuoteInvalid
	}

	data, err := base64.RawURLEncoding.DecodeString(dataB64)
	if err != nil {
		return nil, ErrQuoteInvalid
	}

	var payload QuotePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, ErrQuoteInvalid
	}

	if time.Now().Unix() > payload.ExpiresAt {
		return nil, ErrQuoteExpired
	}

	// 18.3: Bắt buộc kiểm tra user ID, từ chối nếu payload.UserID rỗng hoặc không khớp
	if expectedUserID == "" || payload.UserID == "" || payload.UserID != expectedUserID {
		return nil, errors.New("quote token không thuộc về người dùng này hoặc user_id không hợp lệ")
	}

	return &payload, nil
}

// ValidateBasketMatch kiểm tra danh sách items gửi lên checkout có khớp 1-1 với QuoteToken hay không
func (p *QuotePayload) ValidateBasketMatch(items []dto.CreateOrderItemRequest) error {
	if len(items) != len(p.Items) {
		return errors.New("danh sách sản phẩm không khớp với báo giá trong quote token")
	}
	itemMap := make(map[uint]int)
	for _, it := range items {
		itemMap[it.ProductID] += it.Quantity
	}
	for _, qi := range p.Items {
		qty, exists := itemMap[qi.ProductID]
		if !exists || qty != qi.Quantity {
			return fmt.Errorf("sản phẩm #%d có số lượng (%d) không khớp với quote token (%d)", qi.ProductID, qty, qi.Quantity)
		}
	}
	return nil
}
