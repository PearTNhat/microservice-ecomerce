package handlers

import (
	"ecomerce-service/internal/api/rest"
	"ecomerce-service/internal/core/service"
	"ecomerce-service/internal/dto"
	"github.com/gofiber/fiber/v2"
	"net/http"
)

type UserHandler struct {
	svc *service.UserService
}

func SetupUserRoutes(rh *rest.RestHandler, svc *service.UserService) {
	app := rh.App
	handler := UserHandler{
		svc: svc,
	}

	// Public endpoints
	app.Post("/register", handler.Register)
	app.Post("/login", handler.Login)
	app.Post("/verify-email", handler.VerifyEmail)
}

func (h *UserHandler) Register(ctx *fiber.Ctx) error {
	user := dto.UserSignup{}

	err := ctx.BodyParser(&user)
	if err != nil {
		return ctx.Status(http.StatusBadRequest).JSON(&fiber.Map{
			"message": "please provide valid inputs",
		})
	}

	msg, err := h.svc.Signup(user)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(&fiber.Map{
			"message": err.Error(),
		})
	}

	return ctx.Status(http.StatusOK).JSON(&fiber.Map{
		"message": msg,
	})
}

func (h *UserHandler) VerifyEmail(ctx *fiber.Ctx) error {
	var req struct {
		Email string `json:"email"`
		Code  int    `json:"code"`
	}

	if err := ctx.BodyParser(&req); err != nil {
		return ctx.Status(http.StatusBadRequest).JSON(&fiber.Map{
			"message": "please provide valid inputs",
		})
	}

	token, err := h.svc.VerifyEmail(req.Email, req.Code)
	if err != nil {
		return ctx.Status(http.StatusBadRequest).JSON(&fiber.Map{
			"message": err.Error(),
		})
	}

	return ctx.Status(http.StatusOK).JSON(&fiber.Map{
		"token": token,
	})
}

func (h *UserHandler) Login(ctx *fiber.Ctx) error {
	loginReq := dto.UserLogin{}

	err := ctx.BodyParser(&loginReq)
	if err != nil {
		return ctx.Status(http.StatusBadRequest).JSON(&fiber.Map{
			"message": "please provide valid inputs",
		})
	}

	token, err := h.svc.Login(loginReq)
	if err != nil {
		return ctx.Status(http.StatusUnauthorized).JSON(&fiber.Map{
			"message": err.Error(),
		})
	}

	return ctx.Status(http.StatusOK).JSON(&fiber.Map{
		"token": token,
	})
}
