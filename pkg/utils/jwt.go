package utils

import (
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// GenerateToken tạo ra JWT token có chứa ID người dùng (mặc định Role là CUSTOMER)
func GenerateToken(userID uint, secret string) (string, error) {
	return GenerateTokenWithRole(userID, "CUSTOMER", secret)
}

// GenerateTokenWithRole tạo ra JWT token có chứa ID và Role của người dùng
func GenerateTokenWithRole(userID uint, role string, secret string) (string, error) {
	if role == "" {
		role = "CUSTOMER"
	}

	// Khởi tạo các thông tin payload (claims)
	claims := jwt.MapClaims{
		"sub":  strconv.Itoa(int(userID)),
		"role": role,
		"exp":  time.Now().Add(time.Hour * 24 * 30).Unix(), // Hết hạn sau 30 ngày
		"iat":  time.Now().Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// Ký token bằng APP_SECRET
	return token.SignedString([]byte(secret))
}

// VerifyToken xác thực JWT token và trả về MapClaims nếu hợp lệ
func VerifyToken(tokenString string, secret string) (jwt.MapClaims, error) {
	// Parse token
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Đảm bảo thuật toán ký là HMAC (HS256)
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(secret), nil
	})

	if err != nil {
		return nil, err
	}

	// Trích xuất claims
	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		return claims, nil
	}

	return nil, jwt.ErrTokenInvalidClaims
}
