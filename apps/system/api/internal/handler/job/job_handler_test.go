package job

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/response"
	"github.com/tokyolab/dogx/pkg/subcode"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setHTTPResponses(t *testing.T) {
	t.Helper()
	setHTTPValidation(t)
	httpx.SetErrorHandlerCtx(response.HandleError)
	httpx.SetOkHandler(response.HandleSuccess)
	t.Cleanup(func() { httpx.SetErrorHandlerCtx(nil); httpx.SetOkHandler(nil) })
}
func handlerRequest(body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/job", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	return request
}
func decodedResponse(t *testing.T, recorder *httptest.ResponseRecorder) response.Body {
	t.Helper()
	var body response.Body
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"code", "subcode", "message", "data"} {
		if _, ok := fields[field]; !ok {
			t.Fatalf("missing response field %s: %s", field, recorder.Body.String())
		}
	}
	return body
}

func TestJobHandlerErrorContractAndRedaction(t *testing.T) {
	setHTTPResponses(t)
	business, err := status.New(codes.Code(bizerror.DefaultCode), "internal detail secret.internal").WithDetails(&errdetails.ErrorInfo{Reason: "job.trigger.blocked"})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name        string
		err         error
		wantStatus  int
		wantCode    uint32
		wantSubcode string
	}{
		{"success", nil, http.StatusOK, 0, ""},
		{"business", business.Err(), http.StatusOK, bizerror.DefaultCode, "job.trigger.blocked"},
		{"invalid input", status.Error(codes.InvalidArgument, "secret.internal"), 400, 400, subcode.InvalidRequest},
		{"unavailable", status.Error(codes.Unavailable, "database secret.internal password"), 503, 503, subcode.ServiceUnavailable},
		{"timeout", status.Error(codes.DeadlineExceeded, "secret.internal"), 504, 504, subcode.RequestTimeout},
		{"cancel", status.Error(codes.Canceled, "secret.internal"), 408, 408, subcode.RequestCanceled},
		{"unknown", status.Error(codes.Unknown, "SQL password secret.internal"), 500, 500, subcode.InternalError},
	} {
		t.Run(test.name, func(t *testing.T) {
			stub := &handlerJobRPCStub{err: test.err}
			recorder := httptest.NewRecorder()
			CreateJobHandler(&svc.ServiceContext{JobRpc: stub})(recorder, handlerRequest(marshalBody(t, validCreateBody())))
			body := decodedResponse(t, recorder)
			if recorder.Code != test.wantStatus || body.Code != test.wantCode || body.Subcode != test.wantSubcode || stub.calls != 1 {
				t.Fatalf("status=%d body=%s calls=%d", recorder.Code, recorder.Body.String(), stub.calls)
			}
			if strings.Contains(recorder.Body.String(), "secret.internal") || strings.Contains(recorder.Body.String(), "password") {
				t.Fatalf("internal detail leaked: %s", recorder.Body.String())
			}
			if test.err != nil && body.Data != nil {
				t.Fatalf("failure data=%+v", body.Data)
			}
		})
	}
	for _, body := range []string{`{"name":"secret.internal","timeoutSeconds":`, `{"name":"x","executorGroupId":0,"processorName":"p","cronExpr":"0 * * * * *","timeoutSeconds":900}`} {
		stub := &handlerJobRPCStub{}
		recorder := httptest.NewRecorder()
		CreateJobHandler(&svc.ServiceContext{JobRpc: stub})(recorder, handlerRequest(body))
		decoded := decodedResponse(t, recorder)
		if recorder.Code != 400 || decoded.Subcode != subcode.InvalidRequest || decoded.Data != nil || stub.calls != 0 || strings.Contains(recorder.Body.String(), "secret.internal") {
			t.Fatalf("bad parse response: %s calls=%d", recorder.Body.String(), stub.calls)
		}
	}
}

func TestTriggerHandlerUsesAuthenticatedIdentityAndPreservesPresence(t *testing.T) {
	setHTTPResponses(t)
	stub := &handlerJobRPCStub{}
	recorder := httptest.NewRecorder()
	request := handlerRequest(`{"id":7,"requestId":"12051a0f-76b8-4d88-897f-15ec600de323","paramsOverridden":true,"params":"  ","operatorId":999,"triggeredBy":999}`)
	ctx := context.WithValue(request.Context(), "userId", int64(42))
	ctx = context.WithValue(ctx, "sessionId", "session")
	ctx = context.WithValue(ctx, "isSuperAdmin", false)
	TriggerJobHandler(&svc.ServiceContext{JobRpc: stub})(recorder, request.WithContext(ctx))
	if recorder.Code != 200 || stub.trigger == nil || stub.trigger.OperatorId != 42 || stub.trigger.Params == nil || *stub.trigger.Params != "  " || stub.trigger.ParamsOverridden == nil || !*stub.trigger.ParamsOverridden {
		t.Fatalf("response=%s request=%+v", recorder.Body.String(), stub.trigger)
	}
	decoded := decodedResponse(t, recorder)
	if decoded.Code != 0 || decoded.Subcode != "" {
		t.Fatalf("body=%+v", decoded)
	}
}
