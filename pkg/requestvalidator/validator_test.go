package requestvalidator

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-playground/validator/v10"
)

type roleRequest struct {
	Code string `json:"code" validate:"required,max=64"`
	Name string `json:"name" validate:"required,max=64"`
}

func TestValidateReturnsOfficialFieldAndRule(t *testing.T) {
	err := New().Validate(
		httptest.NewRequest(http.MethodPost, "/role/create", nil),
		&roleRequest{Name: "Operator"},
	)

	validationErr := firstValidationError(t, err)
	if validationErr.Field() != "Code" || validationErr.Tag() != "required" {
		t.Fatalf("unexpected validation error: field=%s tag=%s", validationErr.Field(), validationErr.Tag())
	}
}

func TestValidateStringMaxUsesUnicodeCharacters(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/role/create", nil)
	if err := New().Validate(request, &roleRequest{
		Code: strings.Repeat("角", 64),
		Name: "Operator",
	}); err != nil {
		t.Fatalf("64 Unicode characters rejected: %v", err)
	}

	err := New().Validate(request, &roleRequest{
		Code: strings.Repeat("角", 65),
		Name: "Operator",
	})
	validationErr := firstValidationError(t, err)
	if validationErr.Field() != "Code" || validationErr.Tag() != "max" || validationErr.Param() != "64" {
		t.Fatalf(
			"unexpected validation error: field=%s tag=%s param=%s",
			validationErr.Field(),
			validationErr.Tag(),
			validationErr.Param(),
		)
	}
}

func TestValidateDoesNotApplyBusinessFormatRules(t *testing.T) {
	err := New().Validate(
		httptest.NewRequest(http.MethodPost, "/role/create", nil),
		&roleRequest{Code: "Invalid-Code", Name: "Operator"},
	)
	if err != nil {
		t.Fatalf("business format was unexpectedly validated: %v", err)
	}
}

func firstValidationError(t *testing.T, err error) validator.FieldError {
	t.Helper()
	var validationErrors validator.ValidationErrors
	if !errors.As(err, &validationErrors) || len(validationErrors) == 0 {
		t.Fatalf("expected validation errors, got %v", err)
	}
	return validationErrors[0]
}
