package middlewares

import (
	"ecomerce-service/pkg/utils"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestRequestIDMiddleware(t *testing.T) {
	app := fiber.New()
	app.Use(RequestIDMiddleware())

	app.Get("/test-trace", func(c *fiber.Ctx) error {
		reqID := c.Locals("request_id").(string)
		if reqID == "" {
			t.Error("Kỳ vọng request_id tồn tại trong context nhưng bị rỗng")
		}
		return c.SendString("ok")
	})

	req := httptest.NewRequest("GET", "/test-trace", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Lỗi test request: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Kỳ vọng status 200 nhưng nhận %d", resp.StatusCode)
	}

	headerID := resp.Header.Get(HeaderXRequestID)
	if headerID == "" {
		t.Error("Kỳ vọng response header chứa X-Request-ID")
	}
}

func TestRBACMiddleware_AllowAndDeny(t *testing.T) {
	secret := "test-secret"
	app := fiber.New()

	protected := app.Group("/admin", RequireAuth(secret), RequireRole("ADMIN"))
	protected.Get("/secret-data", func(c *fiber.Ctx) error {
		return c.SendString("admin secret data")
	})

	// 1. Test với Token role CUSTOMER -> Kỳ vọng bị từ chối 403 Forbidden
	customerToken, _ := utils.GenerateTokenWithRole(1, "CUSTOMER", secret)
	reqCustomer := httptest.NewRequest("GET", "/admin/secret-data", nil)
	reqCustomer.Header.Set("Authorization", "Bearer "+customerToken)

	respCustomer, err := app.Test(reqCustomer)
	if err != nil {
		t.Fatalf("Lỗi test request customer: %v", err)
	}
	if respCustomer.StatusCode != http.StatusForbidden {
		t.Errorf("Kỳ vọng status 403 Forbidden cho Customer nhưng nhận %d", respCustomer.StatusCode)
	}

	// 2. Test với Token role ADMIN -> Kỳ vọng thành công 200 OK
	adminToken, _ := utils.GenerateTokenWithRole(2, "ADMIN", secret)
	reqAdmin := httptest.NewRequest("GET", "/admin/secret-data", nil)
	reqAdmin.Header.Set("Authorization", "Bearer "+adminToken)

	respAdmin, err := app.Test(reqAdmin)
	if err != nil {
		t.Fatalf("Lỗi test request admin: %v", err)
	}
	if respAdmin.StatusCode != http.StatusOK {
		t.Errorf("Kỳ vọng status 200 OK cho Admin nhưng nhận %d", respAdmin.StatusCode)
	}
}
