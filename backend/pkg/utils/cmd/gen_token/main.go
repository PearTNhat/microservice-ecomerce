package main

import (
	"fmt"
	"os"
	"strconv"

	"ecomerce-service/pkg/config"
	"ecomerce-service/pkg/utils"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: gen_token <userID> <role>")
		os.Exit(1)
	}
	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid userID: %v\n", err)
		os.Exit(1)
	}
	role := os.Args[2]
	cfg := config.LoadConfig()
	secret := cfg.AppSecret
	if secret == "" {
		secret = "my-super-secret-key-for-jwt"
	}

	token, err := utils.GenerateTokenWithRole(uint(id), role, secret)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error generating token: %v\n", err)
		os.Exit(1)
	}
	fmt.Print(token)
}
