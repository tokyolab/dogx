package response

import (
	"context"
	"errors"
	"net/http"

	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/i18n"
	"github.com/tokyolab/dogx/pkg/subcode"

	"github.com/go-playground/validator/v10"
	"github.com/zeromicro/go-zero/core/logc"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const SuccessCode uint32 = 0

type Body struct {
	Code    uint32 `json:"code"`
	Subcode string `json:"subcode"`
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

func HandleSuccess(ctx context.Context, data any) any {
	message, _ := i18n.Lookup(ctx, "common.success")
	return Body{
		Code:    SuccessCode,
		Subcode: "",
		Message: message,
		Data:    data,
	}
}

func HandleUnauthorized(w http.ResponseWriter, r *http.Request, _ error) {
	ctx := i18n.WithLocale(r.Context(), i18n.Resolve(r.Header.Get("Accept-Language")))
	httpx.WriteJsonCtx(ctx, w, http.StatusUnauthorized, Body{
		Code:    http.StatusUnauthorized,
		Subcode: subcode.AuthenticationRequired,
		Message: translate(ctx, subcode.AuthenticationRequired, subcode.InternalError),
		Data:    nil,
	})
}

func HandleError(ctx context.Context, err error) (int, any) {
	var validationErrors validator.ValidationErrors
	if errors.As(err, &validationErrors) {
		return technicalError(
			ctx,
			http.StatusBadRequest,
			subcode.InvalidRequest,
			validationErrors.Error(),
		)
	}

	if bizErr, ok := bizerror.From(err); ok {
		if !bizerror.IsCode(bizErr.Code()) || !bizerror.IsSubcode(bizErr.Subcode()) {
			logc.Errorf(ctx, "invalid local business error: code=%d subcode=%q", bizErr.Code(), bizErr.Subcode())
			return technicalError(ctx, http.StatusInternalServerError, subcode.InternalError, "")
		}
		return businessError(ctx, bizErr.Code(), bizErr.Subcode())
	}

	var httpErr *Error
	if errors.As(err, &httpErr) {
		if httpErr.status >= http.StatusInternalServerError {
			logc.Errorf(ctx, "request failed: status=%d cause=%v", httpErr.status, httpErr.cause)
		}
		return technicalError(ctx, httpErr.status, httpStatusSubcode(httpErr.status), "")
	}

	if grpcStatus, ok := status.FromError(err); ok {
		grpcCode := uint32(grpcStatus.Code())
		if bizerror.IsCode(grpcCode) {
			businessSubcode, found := bizerror.SubcodeFromStatus(grpcStatus)
			if !found {
				logc.Errorf(ctx, "RPC business error is missing a valid ErrorInfo reason: code=%d error=%v", grpcCode, err)
				businessSubcode = subcode.BusinessError
			}
			return businessError(ctx, grpcCode, businessSubcode)
		}

		httpStatus := grpcCodeToHTTP(grpcStatus.Code())
		if httpStatus >= http.StatusInternalServerError {
			logc.Errorf(ctx, "RPC request failed: code=%d error=%v", grpcStatus.Code(), err)
		}
		return technicalError(ctx, httpStatus, grpcCodeSubcode(grpcStatus.Code()), "")
	}

	// goctl-generated handlers pass request parsing and validation failures as
	// ordinary errors. API Logic must return bizerror, Error, or a gRPC status.
	logc.Infof(ctx, "invalid HTTP request: %v", err)
	return technicalError(ctx, http.StatusBadRequest, subcode.InvalidRequest, "")
}

func businessError(ctx context.Context, code uint32, businessSubcode string) (int, any) {
	return http.StatusOK, Body{
		Code:    code,
		Subcode: businessSubcode,
		Message: translate(ctx, businessSubcode, subcode.BusinessError),
		Data:    nil,
	}
}

func technicalError(
	ctx context.Context,
	statusCode int,
	technicalSubcode string,
	message string,
) (int, any) {
	if message == "" {
		message = translate(ctx, technicalSubcode, subcode.InternalError)
	}
	return statusCode, Body{
		Code:    uint32(statusCode),
		Subcode: technicalSubcode,
		Message: message,
		Data:    nil,
	}
}

func translate(ctx context.Context, key, fallback string) string {
	if message, ok := i18n.Lookup(ctx, key); ok {
		return message
	}
	logc.Errorf(ctx, "missing translation: key=%s locale=%s", key, i18n.Locale(ctx))
	if message, ok := i18n.Lookup(ctx, fallback); ok {
		return message
	}
	return "Internal server error"
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

func grpcCodeSubcode(code codes.Code) string {
	switch code {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return subcode.InvalidRequest
	case codes.Unauthenticated:
		return subcode.AuthenticationRequired
	case codes.PermissionDenied:
		return subcode.PermissionDenied
	case codes.NotFound:
		return subcode.ResourceNotFound
	case codes.Canceled:
		return subcode.RequestCanceled
	case codes.AlreadyExists, codes.Aborted:
		return subcode.RequestConflict
	case codes.ResourceExhausted:
		return subcode.TooManyRequests
	case codes.Unimplemented:
		return subcode.NotImplemented
	case codes.Unavailable:
		return subcode.ServiceUnavailable
	case codes.DeadlineExceeded:
		return subcode.RequestTimeout
	default:
		return subcode.InternalError
	}
}

func httpStatusSubcode(statusCode int) string {
	switch statusCode {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return subcode.InvalidRequest
	case http.StatusUnauthorized:
		return subcode.AuthenticationRequired
	case http.StatusForbidden:
		return subcode.PermissionDenied
	case http.StatusNotFound:
		return subcode.ResourceNotFound
	case http.StatusConflict:
		return subcode.RequestConflict
	case http.StatusTooManyRequests:
		return subcode.TooManyRequests
	case http.StatusNotImplemented:
		return subcode.NotImplemented
	case http.StatusServiceUnavailable:
		return subcode.ServiceUnavailable
	case http.StatusGatewayTimeout, http.StatusRequestTimeout:
		return subcode.RequestTimeout
	default:
		return subcode.InternalError
	}
}
