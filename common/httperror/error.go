package httperror

import (
	"context"
	"errors"
	"net/http"

	"github.com/zeromicro/go-zero/core/logc"
)

type Error struct {
	status  int
	code    string
	message string
	cause   error
}

type Body struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func New(status int, code, message string, cause error) *Error {
	return &Error{
		status:  status,
		code:    code,
		message: message,
		cause:   cause,
	}
}

func ServiceUnavailable(cause error) *Error {
	return New(
		http.StatusServiceUnavailable,
		"SERVICE_UNAVAILABLE",
		"service dependencies are not ready",
		cause,
	)
}

func (e *Error) Error() string {
	return e.message
}

func (e *Error) Unwrap() error {
	return e.cause
}

func Handle(ctx context.Context, err error) (int, any) {
	var apiErr *Error
	if errors.As(err, &apiErr) {
		if apiErr.status >= http.StatusInternalServerError {
			logc.Errorf(ctx, "request failed: code=%s cause=%v", apiErr.code, apiErr.cause)
		}
		return apiErr.status, Body{Code: apiErr.code, Message: apiErr.message}
	}

	logc.Errorf(ctx, "unhandled request error: %v", err)
	return http.StatusInternalServerError, Body{
		Code:    "INTERNAL_ERROR",
		Message: "internal server error",
	}
}
