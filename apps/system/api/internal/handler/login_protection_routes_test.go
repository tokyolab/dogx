package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
)

func TestLoginRateLimitRunsBeforeParsingAndOnlyOnLogin(t *testing.T) {
	order := []string{}
	calls := 0
	server := newRouteTestServer(t, validRouteSessionReader(&order), &routeEnforcerStub{order: &order}, &routeSystemRPCStub{order: &order}, func(s *svc.ServiceContext) {
		s.LoginRateLimit = func(next http.HandlerFunc) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(http.StatusTooManyRequests) }
		}
	})
	for _, tc := range []struct {
		path   string
		status int
	}{{"/auth/login", 429}, {"/auth/refresh", 400}} {
		req := httptest.NewRequest("POST", tc.path, strings.NewReader(`{`))
		req.Header.Set("Content-Type", "application/json")
		out := httptest.NewRecorder()
		server.Serve(out, req)
		if out.Code != tc.status {
			t.Fatalf("%s: %d", tc.path, out.Code)
		}
	}
	if calls != 1 || len(order) != 0 {
		t.Fatalf("limiter calls=%d downstream=%v", calls, order)
	}
}
