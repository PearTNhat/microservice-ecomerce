package middlewares

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

func TestIdempotencyMiddleware(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Không thể khởi chạy miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	app := fiber.New()
	app.Post("/create-order", IdempotencyMiddleware(rdb), func(c *fiber.Ctx) error {
		return c.Status(http.StatusCreated).JSON(fiber.Map{
			"message": "Order created successfully",
		})
	})

	// 1. Request đầu tiên có Idempotency-Key hợp lệ -> 201 Created
	req1 := httptest.NewRequest("POST", "/create-order", nil)
	req1.Header.Set("X-Idempotency-Key", "test-key-12345")

	resp1, err := app.Test(req1)
	if err != nil {
		t.Fatalf("Lỗi gửi request 1: %v", err)
	}
	if resp1.StatusCode != http.StatusCreated {
		t.Errorf("Kỳ vọng status 201 nhưng nhận %d", resp1.StatusCode)
	}

	// 2. Request thứ 2 gửi lại cùng Idempotency-Key đó -> 409 Conflict
	req2 := httptest.NewRequest("POST", "/create-order", nil)
	req2.Header.Set("X-Idempotency-Key", "test-key-12345")

	resp2, err := app.Test(req2)
	if err != nil {
		t.Fatalf("Lỗi gửi request 2: %v", err)
	}
	if resp2.StatusCode != http.StatusConflict {
		t.Errorf("Kỳ vọng status 409 Conflict cho request lặp nhưng nhận %d", resp2.StatusCode)
	}

	// 3. Request với Idempotency-Key mới -> 201 Created
	req3 := httptest.NewRequest("POST", "/create-order", nil)
	req3.Header.Set("X-Idempotency-Key", "test-key-67890")

	resp3, err := app.Test(req3)
	if err != nil {
		t.Fatalf("Lỗi gửi request 3: %v", err)
	}
	if resp3.StatusCode != http.StatusCreated {
		t.Errorf("Kỳ vọng status 201 nhưng nhận %d", resp3.StatusCode)
	}
}
