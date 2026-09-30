package security

import (
	"context"
	"encoding/json"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/pkg/requestvalidator"
	"github.com/tokyolab/dogx/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http/httptest"
	"strings"
	"testing"
)

type securityRPCStub struct {
	systemclient.System
	calls int
	err   error
}

func (s *securityRPCStub) UpdateLoginSecurity(context.Context, *systemclient.LoginSecurityConfig, ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.calls++
	return &systemclient.EmptyResponse{}, s.err
}
func (s *securityRPCStub) GetLoginSecurity(context.Context, *systemclient.GetLoginSecurityRequest, ...grpc.CallOption) (*systemclient.LoginSecurityConfig, error) {
	s.calls++
	return &systemclient.LoginSecurityConfig{}, s.err
}

func TestSecurityHandlers(t *testing.T) {
	httpx.SetErrorHandlerCtx(response.HandleError)
	httpx.SetOkHandler(response.HandleSuccess)
	httpx.SetValidator(requestvalidator.New())
	t.Cleanup(func() { httpx.SetErrorHandlerCtx(nil); httpx.SetOkHandler(nil); httpx.SetValidator(nil) })
	valid := map[string]any{"rateLimitEnabled": false, "rateLimitWindowSeconds": 60, "rateLimitMaxRequests": 30, "failureLockEnabled": false, "failureWindowSeconds": 900, "failureThreshold": 5, "lockDurationSeconds": 900}
	for _, missing := range []string{"", "rateLimitEnabled", "failureLockEnabled", "failureThreshold"} {
		t.Run("missing "+missing, func(t *testing.T) {
			values := map[string]any{}
			for k, v := range valid {
				values[k] = v
			}
			delete(values, missing)
			body, _ := json.Marshal(values)
			stub := &securityRPCStub{}
			out := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/security/login/update", strings.NewReader(string(body)))
			req.Header.Set("Content-Type", "application/json")
			UpdateLoginSecurityHandler(&svc.ServiceContext{SystemRpc: stub})(out, req)
			want := 400
			if missing == "" {
				want = 200
			}
			if out.Code != want || (stub.calls == 1) != (missing == "") {
				t.Fatalf("%d %s calls=%d", out.Code, out.Body.String(), stub.calls)
			}
		})
	}
	for _, body := range []string{`{`, strings.Replace(`{"rateLimitEnabled":false,"rateLimitWindowSeconds":60,"rateLimitMaxRequests":30,"failureLockEnabled":false,"failureWindowSeconds":900,"failureThreshold":5,"lockDurationSeconds":900}`, `"failureThreshold":5`, `"failureThreshold":0`, 1)} {
		out := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		UpdateLoginSecurityHandler(&svc.ServiceContext{})(out, req)
		if out.Code != 400 {
			t.Fatal(out.Code)
		}
	}
	for _, dependencyErr := range []error{nil, status.Error(codes.Internal, "offline")} {
		stub := &securityRPCStub{err: dependencyErr}
		sc := &svc.ServiceContext{SystemRpc: stub}
		out := httptest.NewRecorder()
		GetLoginSecurityHandler(sc)(out, httptest.NewRequest("POST", "/", nil))
		want := 200
		if dependencyErr != nil {
			want = 500
		}
		if out.Code != want {
			t.Fatalf("get %d", out.Code)
		}
		body, _ := json.Marshal(valid)
		req := httptest.NewRequest("POST", "/", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		out = httptest.NewRecorder()
		UpdateLoginSecurityHandler(sc)(out, req)
		if out.Code != want {
			t.Fatalf("update %d", out.Code)
		}
	}
}
