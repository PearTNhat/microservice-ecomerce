package utils

import "golang.org/x/crypto/bcrypt"

// HashPassword mã hóa mật khẩu thô thành chuỗi băm (hash)
func HashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), 10)
	return string(bytes), err
}

// CheckPasswordHash so sánh mật khẩu thô với chuỗi băm
func CheckPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}
