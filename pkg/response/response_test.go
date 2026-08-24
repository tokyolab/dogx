package response

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tokyolab/dogx/pkg/bizerror"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHandleSuccess(t *testing.T) {
	data := map[string]string{"status": "ok"}
	body, ok := HandleSuccess(context.Background(), data).(Body)

	if !ok {
		t.Fatal("unexpected success body type")
	}
	if body.Code != SuccessCode || body.Message != "success" {
		t.Fatalf("unexpected success body: %+v", body)
	}
	if got := body.Data.(map[string]string)["status"]; got != "ok" {
		t.Fatalf("unexpected success data: %+v", body.Data)
	}
}

func TestHandleUnauthorized(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	recorder := httptest.NewRecorder()
	HandleUnauthorized(recorder, request, errors.New("private JWT detail"))

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
	var body Body
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode unauthorized response: %v", err)
	}
	if body.Code != http.StatusUnauthorized || body.Message != "authentication required" || body.Data != nil {
		t.Fatalf("unexpected unauthorized response: %+v", body)
	}
}

func TestHandleLocalBusinessError(t *testing.T) {
	httpStatus, value := HandleError(context.Background(), bizerror.New("username already exists"))
	body := value.(Body)

	if httpStatus != http.StatusOK || body.Code != bizerror.DefaultCode {
		t.Fatalf("unexpected business response: status=%d body=%+v", httpStatus, body)
	}
}

func TestHandleRejectsInvalidLocalBusinessCode(t *testing.T) {
	httpStatus, value := HandleError(context.Background(), bizerror.NewCode(1, "invalid code"))
	body := value.(Body)

	if httpStatus != http.StatusInternalServerError || body.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected invalid-code response: status=%d body=%+v", httpStatus, body)
	}
}

func TestHandleRPCBusinessError(t *testing.T) {
	err := status.Error(codes.Code(100002), "token expired")
	httpStatus, value := HandleError(context.Background(), err)
	body := value.(Body)

	if httpStatus != http.StatusOK || body.Code != 100002 || body.Message != "token expired" {
		t.Fatalf("unexpected RPC business response: status=%d body=%+v", httpStatus, body)
	}
}

func TestHandleStandardGRPCErrors(t *testing.T) {
	tests := []struct {
		name       string
		grpcCode   codes.Code
		httpStatus int
		message    string
	}{
		{name: "invalid argument", grpcCode: codes.InvalidArgument, httpStatus: http.StatusBadRequest, message: "invalid request"},
		{name: "failed precondition", grpcCode: codes.FailedPrecondition, httpStatus: http.StatusBadRequest, message: "invalid request"},
		{name: "out of range", grpcCode: codes.OutOfRange, httpStatus: http.StatusBadRequest, message: "invalid request"},
		{name: "unauthenticated", grpcCode: codes.Unauthenticated, httpStatus: http.StatusUnauthorized, message: "authentication required"},
		{name: "permission denied", grpcCode: codes.PermissionDenied, httpStatus: http.StatusForbidden, message: "permission denied"},
		{name: "not found", grpcCode: codes.NotFound, httpStatus: http.StatusNotFound, message: "resource not found"},
		{name: "canceled", grpcCode: codes.Canceled, httpStatus: http.StatusRequestTimeout, message: "request canceled"},
		{name: "already exists", grpcCode: codes.AlreadyExists, httpStatus: http.StatusConflict, message: "request conflict"},
		{name: "aborted", grpcCode: codes.Aborted, httpStatus: http.StatusConflict, message: "request conflict"},
		{name: "resource exhausted", grpcCode: codes.ResourceExhausted, httpStatus: http.StatusTooManyRequests, message: "too many requests"},
		{name: "unimplemented", grpcCode: codes.Unimplemented, httpStatus: http.StatusNotImplemented, message: "not implemented"},
		{name: "unavailable", grpcCode: codes.Unavailable, httpStatus: http.StatusServiceUnavailable, message: "service unavailable"},
		{name: "deadline exceeded", grpcCode: codes.DeadlineExceeded, httpStatus: http.StatusGatewayTimeout, message: "request timed out"},
		{name: "unknown", grpcCode: codes.Unknown, httpStatus: http.StatusInternalServerError, message: "internal server error"},
		{name: "internal", grpcCode: codes.Internal, httpStatus: http.StatusInternalServerError, message: "internal server error"},
		{name: "data loss", grpcCode: codes.DataLoss, httpStatus: http.StatusInternalServerError, message: "internal server error"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := status.Error(test.grpcCode, "private framework detail")
			httpStatus, value := HandleError(context.Background(), err)
			body := value.(Body)

			if httpStatus != test.httpStatus || body.Code != uint32(test.httpStatus) {
				t.Fatalf("unexpected standard gRPC response: status=%d body=%+v", httpStatus, body)
			}
			if body.Message != test.message {
				t.Fatalf("message = %q, want %q", body.Message, test.message)
			}
			if body.Data != nil {
				t.Fatalf("technical error exposed data: %+v", body.Data)
			}
		})
	}
}

func TestHandleServiceUnavailable(t *testing.T) {
	cause := errors.New("private dependency detail")
	err := ServiceUnavailable(cause)
	if err.Error() != "service dependencies are not ready" {
		t.Fatalf("unexpected error string: %s", err.Error())
	}
	if !errors.Is(err, cause) {
		t.Fatal("typed error does not unwrap its cause")
	}

	httpStatus, value := HandleError(context.Background(), err)
	body := value.(Body)

	if httpStatus != http.StatusServiceUnavailable || body.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected unavailable response: status=%d body=%+v", httpStatus, body)
	}
	if body.Message != "service dependencies are not ready" {
		t.Fatalf("unexpected public message: %s", body.Message)
	}
}

func TestHandleTypedClientError(t *testing.T) {
	cause := errors.New("private client detail")
	httpStatus, value := HandleError(
		context.Background(),
		NewError(http.StatusUnprocessableEntity, "request cannot be processed", cause),
	)
	body := value.(Body)

	if httpStatus != http.StatusUnprocessableEntity || body.Code != http.StatusUnprocessableEntity {
		t.Fatalf("unexpected typed response: status=%d body=%+v", httpStatus, body)
	}
	if body.Message != "request cannot be processed" || body.Data != nil {
		t.Fatalf("unexpected typed response body: %+v", body)
	}
}

func TestHandleUnknownGRPCErrorHidesCause(t *testing.T) {
	err := status.Error(codes.Unknown, "database secret detail")
	httpStatus, value := HandleError(context.Background(), err)
	body := value.(Body)

	if httpStatus != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", httpStatus)
	}
	if body.Message != "internal server error" {
		t.Fatalf("unexpected public message: %s", body.Message)
	}
}

func TestHandleOrdinaryErrorAsInvalidRequest(t *testing.T) {
	httpStatus, value := HandleError(context.Background(), errors.New("json parse detail"))
	body := value.(Body)

	if httpStatus != http.StatusBadRequest || body.Message != "invalid request" {
		t.Fatalf("unexpected ordinary response: status=%d body=%+v", httpStatus, body)
	}
}
