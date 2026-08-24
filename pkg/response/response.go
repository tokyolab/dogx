package response

import (
	"context"
	"errors"
	"net/http"

	"github.com/tokyolab/dogx/pkg/bizerror"

	"github.com/zeromicro/go-zero/core/logc"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const SuccessCode uint32 = 0

type Body struct {
	Code    uint32 `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

type Error struct {
	status  int
	message string
	cause   error
}

func NewError(statusCode int, message string, cause error) *Error {
	return &Error{
		status:  statusCode,
		message: message,
		cause:   cause,
	}
}

func ServiceUnavailable(cause error) *Error {
	return NewError(
		http.StatusServiceUnavailable,
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

func HandleSuccess(_ context.Context, data any) any {
	return Body{
		Code:    SuccessCode,
		Message: "success",
		Data:    data,
	}
}

func HandleUnauthorized(w http.ResponseWriter, r *http.Request, _ error) {
	httpx.WriteJsonCtx(r.Context(), w, http.StatusUnauthorized, Body{
		Code:    http.StatusUnauthorized,
		Message: "authentication required",
		Data:    nil,
	})
}

func HandleError(ctx context.Context, err error) (int, any) {
	if bizErr, ok := bizerror.From(err); ok {
		if !bizerror.IsCode(bizErr.Code()) {
			logc.Errorf(ctx, "invalid local business error code: %d", bizErr.Code())
			return technicalError(http.StatusInternalServerError, "internal server error")
		}
		return businessError(bizErr.Code(), bizErr.Error())
	}

	var httpErr *Error
	if errors.As(err, &httpErr) {
		if httpErr.status >= http.StatusInternalServerError {
			logc.Errorf(ctx, "request failed: status=%d cause=%v", httpErr.status, httpErr.cause)
		}
		return technicalError(httpErr.status, httpErr.message)
	}

	if grpcStatus, ok := status.FromError(err); ok {
		grpcCode := uint32(grpcStatus.Code())
		if bizerror.IsCode(grpcCode) {
			return businessError(grpcCode, grpcStatus.Message())
		}

		httpStatus := grpcCodeToHTTP(grpcStatus.Code())
		if httpStatus >= http.StatusInternalServerError {
			logc.Errorf(ctx, "RPC request failed: code=%d error=%v", grpcStatus.Code(), err)
		}
		return technicalError(httpStatus, grpcCodeMessage(grpcStatus.Code()))
	}

	// goctl-generated handlers pass request parsing and validation failures as
	// ordinary errors. API Logic must return bizerror, Error, or a gRPC status.
	return technicalError(http.StatusBadRequest, "invalid request")
}

func businessError(code uint32, message string) (int, any) {
	return http.StatusOK, Body{
		Code:    code,
		Message: message,
		Data:    nil,
	}
}

func technicalError(statusCode int, message string) (int, any) {
	return statusCode, Body{
		Code:    uint32(statusCode),
		Message: message,
		Data:    nil,
	}
}

func grpcCodeToHTTP(code codes.Code) int {
	switch code {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.NotFound:
		return http.StatusNotFound
	case codes.Canceled:
		return http.StatusRequestTimeout
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

func grpcCodeMessage(code codes.Code) string {
	switch code {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return "invalid request"
	case codes.Unauthenticated:
		return "authentication required"
	case codes.PermissionDenied:
		return "permission denied"
	case codes.NotFound:
		return "resource not found"
	case codes.Canceled:
		return "request canceled"
	case codes.AlreadyExists, codes.Aborted:
		return "request conflict"
	case codes.ResourceExhausted:
		return "too many requests"
	case codes.Unimplemented:
		return "not implemented"
	case codes.Unavailable:
		return "service unavailable"
	case codes.DeadlineExceeded:
		return "request timed out"
	default:
		return "internal server error"
	}
}
