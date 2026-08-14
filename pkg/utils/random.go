package utils

import (
	"crypto/rand"
	"math/big"
)

// GenerateRandomCode tạo ra một mã số nguyên ngẫu nhiên gồm 6 chữ số
func GenerateRandomCode() (int, error) {
	// Giới hạn max là 999999, min là 100000
	max := big.NewInt(900000)
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		return 0, err
	}
	return int(n.Int64()) + 100000, nil
}
