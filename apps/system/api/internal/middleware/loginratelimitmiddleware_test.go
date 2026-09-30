package middleware

import (
	"context"
	"errors"
	"github.com/tokyolab/dogx/apps/system/api/internal/clientip"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/response"
	"github.com/zeromicro/go-zero/core/limit"
	"github.com/zeromicro/go-zero/rest/httpx"
	"net/http"
	"net/http/httptest"
	"testing"
)

type loginConfigStub struct {
	enabled bool
	err     error
}

func (s loginConfigStub) Current() (*system.LoginSecurityConfig, error) {
	return &system.LoginSecurityConfig{RateLimitEnabled: s.enabled, RateLimitWindowSeconds: 60, RateLimitMaxRequests: 2}, s.err
}

func TestLoginRateLimit(t *testing.T) {
	httpx.SetErrorHandlerCtx(response.HandleError)
	t.Cleanup(func() { httpx.SetErrorHandlerCtx(nil) })
	failure := errors.New("offline")
	resolver, err := clientip.New([]string{"10.0.0.0/8"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name           string
		enabled        bool
		cfgErr, error  error
		result, status int
		peer           string
		take           bool
	}{
		{"allowed", true, nil, nil, limit.Allowed, 204, "192.0.2.1:123", true},
		{"last allowed", true, nil, nil, limit.HitQuota, 204, "192.0.2.1:123", true},
		{"over quota", true, nil, nil, limit.OverQuota, 429, "192.0.2.1:123", true},
		{"redis fails closed", true, nil, failure, 0, 503, "192.0.2.1:123", true},
		{"invalid redis result", true, nil, nil, 999, 503, "192.0.2.1:123", true},
		{"disabled", false, nil, nil, 0, 204, "192.0.2.1:123", false},
		{"missing config", false, failure, nil, 0, 503, "192.0.2.1:123", false},
		{"bad peer", true, nil, nil, 0, 400, "bad", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			m := &LoginRateLimitMiddleware{config: loginConfigStub{tc.enabled, tc.cfgErr}, resolver: resolver, endpoint: "login", take: func(_ context.Context, key string, _ *system.LoginSecurityConfig) (int, error) {
				calls++
				if key != "login:192.0.2.1" {
					t.Fatalf("spoofed key %q", key)
				}
				return tc.result, tc.error
			}}
			reached := false
			handler := m.Handle(func(w http.ResponseWriter, r *http.Request) { reached = true; w.WriteHeader(204) })
			req := httptest.NewRequest("POST", "/auth/login", nil)
			req.RemoteAddr = tc.peer
			req.Header.Set("X-Forwarded-For", "1.1.1.1")
			out := httptest.NewRecorder()
			handler(out, req)
			if out.Code != tc.status || reached != (tc.status == 204) || (calls == 1) != tc.take {
				t.Fatalf("status=%d next=%v calls=%d", out.Code, reached, calls)
			}
		})
	}
}

func TestDefaultLoginRateLimitUsesForwardedIP(t *testing.T) {
	resolver, err := clientip.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	var key string
	m := &LoginRateLimitMiddleware{config: loginConfigStub{enabled: true}, resolver: resolver, endpoint: "login", take: func(_ context.Context, value string, _ *system.LoginSecurityConfig) (int, error) {
		key = value
		return limit.Allowed, nil
	}}
	req := httptest.NewRequest("POST", "/auth/login", nil)
	req.RemoteAddr = "127.0.0.1:123"
	req.Header.Set("X-Forwarded-For", "198.51.100.1, 10.0.0.2")
	out := httptest.NewRecorder()
	m.Handle(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })(out, req)
	if out.Code != http.StatusNoContent || key != "login:198.51.100.1" {
		t.Fatalf("status=%d key=%q", out.Code, key)
	}
}
