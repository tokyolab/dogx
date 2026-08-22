package response

import (
	"context"
	"errors"
	"net/http"
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

func TestHandleStandardGRPCError(t *testing.T) {
	err := status.Error(codes.FailedPrecondition, "private framework detail")
	httpStatus, value := HandleError(context.Background(), err)
	body := value.(Body)

	if httpStatus != http.StatusBadRequest || body.Code != http.StatusBadRequest {
		t.Fatalf("unexpected standard gRPC response: status=%d body=%+v", httpStatus, body)
	}
	if body.Message != "invalid request" {
		t.Fatalf("unexpected public message: %s", body.Message)
	}
}

func TestHandleServiceUnavailable(t *testing.T) {
	cause := errors.New("private dependency detail")
	httpStatus, value := HandleError(context.Background(), ServiceUnavailable(cause))
	body := value.(Body)

	if httpStatus != http.StatusServiceUnavailable || body.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected unavailable response: status=%d body=%+v", httpStatus, body)
	}
	if body.Message != "service dependencies are not ready" {
		t.Fatalf("unexpected public message: %s", body.Message)
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
