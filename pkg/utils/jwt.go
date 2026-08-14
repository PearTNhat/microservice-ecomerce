package utils

import (
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// GenerateToken tạo ra JWT token có chứa ID người dùng
func GenerateToken(userID uint, secret string) (string, error) {
	// Khởi tạo các thông tin payload (claims)
	claims := jwt.MapClaims{
		"sub": strconv.Itoa(int(userID)),
		"exp": time.Now().Add(time.Hour * 24 * 30).Unix(), // Hết hạn sau 30 ngày
		"iat": time.Now().Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// Ký token bằng APP_SECRET
	return token.SignedString([]byte(secret))
}
