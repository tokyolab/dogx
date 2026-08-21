package httperror

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestHandleServiceUnavailable(t *testing.T) {
	cause := errors.New("dependency failed")
	status, body := Handle(context.Background(), ServiceUnavailable(cause))

	if status != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: %d", status)
	}

	response, ok := body.(Body)
	if !ok {
		t.Fatalf("unexpected body type: %T", body)
	}
	if response.Code != "SERVICE_UNAVAILABLE" {
		t.Fatalf("unexpected code: %s", response.Code)
	}
}

func TestHandleHidesUnknownError(t *testing.T) {
	status, body := Handle(context.Background(), errors.New("database secret detail"))

	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", status)
	}
	response := body.(Body)
	if response.Message != "internal server error" {
		t.Fatalf("unexpected public message: %s", response.Message)
	}
}
