package response

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/i18n"
	"github.com/tokyolab/dogx/pkg/subcode"

	"github.com/go-playground/validator/v10"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHandleSuccess(t *testing.T) {
	data := map[string]string{"status": "ok"}
	body, ok := HandleSuccess(i18n.WithLocale(context.Background(), i18n.EnUS), data).(Body)

	if !ok {
		t.Fatal("unexpected success body type")
	}
	if body.Code != SuccessCode || body.Subcode != "" || body.Message != "Success" {
		t.Fatalf("unexpected success body: %+v", body)
	}
	if got := body.Data.(map[string]string)["status"]; got != "ok" {
		t.Fatalf("unexpected success data: %+v", body.Data)
	}
}

func TestHandleUnauthorizedUsesRequestedLanguage(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.Header.Set("Accept-Language", "en-US")
	recorder := httptest.NewRecorder()
	HandleUnauthorized(recorder, request, errors.New("private JWT detail"))

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
	var body Body
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode unauthorized response: %v", err)
	}
	if body.Code != http.StatusUnauthorized ||
		body.Subcode != subcode.AuthenticationRequired ||
		body.Message != "Authentication required" || body.Data != nil {
		t.Fatalf("unexpected unauthorized response: %+v", body)
	}
}

func TestHandleLocalBusinessError(t *testing.T) {
	httpStatus, value := HandleError(
		context.Background(),
		bizerror.New("system.auth.invalid_credentials", "internal readable diagnostic"),
	)
	body := value.(Body)

	if httpStatus != http.StatusOK || body.Code != bizerror.DefaultCode ||
		body.Subcode != "system.auth.invalid_credentials" {
		t.Fatalf("unexpected business response: status=%d body=%+v", httpStatus, body)
	}
	if body.Message == "internal readable diagnostic" {
		t.Fatal("internal diagnostic was exposed as the HTTP message")
	}
}

func TestHandleRejectsInvalidLocalBusinessError(t *testing.T) {
	tests := []*bizerror.Error{
		bizerror.NewCode(1, "system.auth.invalid", "invalid code"),
		bizerror.New("INVALID_SUBCODE", "invalid subcode"),
	}
	for _, err := range tests {
		httpStatus, value := HandleError(context.Background(), err)
		body := value.(Body)
		if httpStatus != http.StatusInternalServerError || body.Code != http.StatusInternalServerError ||
			body.Subcode != subcode.InternalError {
			t.Fatalf("unexpected invalid business response: status=%d body=%+v", httpStatus, body)
		}
	}
}

func TestHandleRPCBusinessError(t *testing.T) {
	st, err := status.New(codes.Code(100002), "token expired diagnostic").WithDetails(
		&errdetails.ErrorInfo{Reason: "system.auth.invalid_credentials"},
	)
	if err != nil {
		t.Fatalf("attach ErrorInfo: %v", err)
	}
	httpStatus, value := HandleError(context.Background(), st.Err())
	body := value.(Body)

	if httpStatus != http.StatusOK || body.Code != 100002 ||
		body.Subcode != "system.auth.invalid_credentials" {
		t.Fatalf("unexpected RPC business response: status=%d body=%+v", httpStatus, body)
	}
	if body.Message == "token expired diagnostic" {
		t.Fatal("RPC diagnostic was exposed as the HTTP message")
	}
}

func TestHandleRPCBusinessErrorWithoutDetailFallsBackSafely(t *testing.T) {
	httpStatus, value := HandleError(
		context.Background(),
		status.Error(codes.Code(100002), "private business detail"),
	)
	body := value.(Body)
	if httpStatus != http.StatusOK || body.Code != 100002 || body.Subcode != subcode.BusinessError {
		t.Fatalf("unexpected fallback response: status=%d body=%+v", httpStatus, body)
	}
}

func TestHandleStandardGRPCErrors(t *testing.T) {
	tests := []struct {
		name       string
		grpcCode   codes.Code
		httpStatus int
		subcode    string
	}{
		{name: "invalid argument", grpcCode: codes.InvalidArgument, httpStatus: http.StatusBadRequest, subcode: subcode.InvalidRequest},
		{name: "failed precondition", grpcCode: codes.FailedPrecondition, httpStatus: http.StatusBadRequest, subcode: subcode.InvalidRequest},
		{name: "out of range", grpcCode: codes.OutOfRange, httpStatus: http.StatusBadRequest, subcode: subcode.InvalidRequest},
		{name: "unauthenticated", grpcCode: codes.Unauthenticated, httpStatus: http.StatusUnauthorized, subcode: subcode.AuthenticationRequired},
		{name: "permission denied", grpcCode: codes.PermissionDenied, httpStatus: http.StatusForbidden, subcode: subcode.PermissionDenied},
		{name: "not found", grpcCode: codes.NotFound, httpStatus: http.StatusNotFound, subcode: subcode.ResourceNotFound},
		{name: "canceled", grpcCode: codes.Canceled, httpStatus: http.StatusRequestTimeout, subcode: subcode.RequestCanceled},
		{name: "already exists", grpcCode: codes.AlreadyExists, httpStatus: http.StatusConflict, subcode: subcode.RequestConflict},
		{name: "aborted", grpcCode: codes.Aborted, httpStatus: http.StatusConflict, subcode: subcode.RequestConflict},
		{name: "resource exhausted", grpcCode: codes.ResourceExhausted, httpStatus: http.StatusTooManyRequests, subcode: subcode.TooManyRequests},
		{name: "unimplemented", grpcCode: codes.Unimplemented, httpStatus: http.StatusNotImplemented, subcode: subcode.NotImplemented},
		{name: "unavailable", grpcCode: codes.Unavailable, httpStatus: http.StatusServiceUnavailable, subcode: subcode.ServiceUnavailable},
		{name: "deadline exceeded", grpcCode: codes.DeadlineExceeded, httpStatus: http.StatusGatewayTimeout, subcode: subcode.RequestTimeout},
		{name: "unknown", grpcCode: codes.Unknown, httpStatus: http.StatusInternalServerError, subcode: subcode.InternalError},
		{name: "internal", grpcCode: codes.Internal, httpStatus: http.StatusInternalServerError, subcode: subcode.InternalError},
		{name: "data loss", grpcCode: codes.DataLoss, httpStatus: http.StatusInternalServerError, subcode: subcode.InternalError},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := status.Error(test.grpcCode, "private framework detail")
			httpStatus, value := HandleError(context.Background(), err)
			body := value.(Body)

			if httpStatus != test.httpStatus || body.Code != uint32(test.httpStatus) || body.Subcode != test.subcode {
				t.Fatalf("unexpected standard gRPC response: status=%d body=%+v", httpStatus, body)
			}
			if body.Message == "private framework detail" || body.Data != nil {
				t.Fatalf("technical error exposed private data: %+v", body)
			}
		})
	}
}

func TestHandleServiceUnavailable(t *testing.T) {
	cause := errors.New("private dependency detail")
	err := ServiceUnavailable(cause)
	if err.Error() != "service dependencies are not ready" || !errors.Is(err, cause) {
		t.Fatalf("typed error lost its diagnostic or cause: %v", err)
	}

	httpStatus, value := HandleError(context.Background(), err)
	body := value.(Body)
	if httpStatus != http.StatusServiceUnavailable || body.Code != http.StatusServiceUnavailable ||
		body.Subcode != subcode.ServiceUnavailable {
		t.Fatalf("unexpected unavailable response: status=%d body=%+v", httpStatus, body)
	}
	if body.Message == err.Error() {
		t.Fatal("internal service diagnostic was exposed")
	}
}

func TestHandleTypedClientError(t *testing.T) {
	cause := errors.New("private client detail")
	httpStatus, value := HandleError(
		context.Background(),
		NewError(http.StatusUnprocessableEntity, "request cannot be processed", cause),
	)
	body := value.(Body)

	if httpStatus != http.StatusUnprocessableEntity || body.Code != http.StatusUnprocessableEntity ||
		body.Subcode != subcode.InvalidRequest || body.Data != nil {
		t.Fatalf("unexpected typed response: status=%d body=%+v", httpStatus, body)
	}
	if body.Message == "request cannot be processed" {
		t.Fatal("typed error diagnostic was exposed")
	}
}

func TestHandleValidationErrorKeepsOriginalDiagnostic(t *testing.T) {
	validate := validator.New(validator.WithRequiredStructEnabled())
	err := validate.Struct(struct {
		Code string `validate:"required"`
	}{})
	httpStatus, value := HandleError(context.Background(), err)
	body := value.(Body)
	if httpStatus != http.StatusBadRequest || body.Code != http.StatusBadRequest ||
		body.Subcode != subcode.InvalidRequest || body.Message != err.Error() {
		t.Fatalf("unexpected validation response: status=%d body=%+v", httpStatus, body)
	}
}

func TestHandleOrdinaryErrorAsInvalidRequest(t *testing.T) {
	httpStatus, value := HandleError(context.Background(), errors.New("json parse detail"))
	body := value.(Body)

	if httpStatus != http.StatusBadRequest || body.Subcode != subcode.InvalidRequest ||
		body.Message == "json parse detail" {
		t.Fatalf("unexpected ordinary response: status=%d body=%+v", httpStatus, body)
	}
}

func TestTranslateFallsBackWithoutExposingMissingKey(t *testing.T) {
	ctx := i18n.WithLocale(context.Background(), i18n.EnUS)
	message := translate(ctx, "system.missing.translation", subcode.BusinessError)
	if message != "Business operation failed" {
		t.Fatalf("unexpected fallback message: %q", message)
	}
}

func TestHTTPStatusSubcode(t *testing.T) {
	tests := []struct {
		statusCode int
		subcode    string
	}{
		{statusCode: http.StatusBadRequest, subcode: subcode.InvalidRequest},
		{statusCode: http.StatusUnprocessableEntity, subcode: subcode.InvalidRequest},
		{statusCode: http.StatusUnauthorized, subcode: subcode.AuthenticationRequired},
		{statusCode: http.StatusForbidden, subcode: subcode.PermissionDenied},
		{statusCode: http.StatusNotFound, subcode: subcode.ResourceNotFound},
		{statusCode: http.StatusConflict, subcode: subcode.RequestConflict},
		{statusCode: http.StatusTooManyRequests, subcode: subcode.TooManyRequests},
		{statusCode: http.StatusNotImplemented, subcode: subcode.NotImplemented},
		{statusCode: http.StatusServiceUnavailable, subcode: subcode.ServiceUnavailable},
		{statusCode: http.StatusGatewayTimeout, subcode: subcode.RequestTimeout},
		{statusCode: http.StatusRequestTimeout, subcode: subcode.RequestTimeout},
		{statusCode: http.StatusTeapot, subcode: subcode.InternalError},
	}

	for _, test := range tests {
		if got := httpStatusSubcode(test.statusCode); got != test.subcode {
			t.Errorf("status %d mapped to %q, want %q", test.statusCode, got, test.subcode)
		}
	}
}
