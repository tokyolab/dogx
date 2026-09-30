//go:build integration

package middleware

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/tokyolab/dogx/pkg/response"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest/httpx"
)

func TestLoginMiddlewareSharesRedisQuota(t *testing.T) {
	host := os.Getenv("DOGX_TEST_REDIS_HOST")
	if host == "" {
		t.Fatal("DOGX_TEST_REDIS_HOST is required")
	}
	client, err := redis.NewRedis(redis.RedisConf{Host: host, Pass: os.Getenv("DOGX_TEST_REDIS_PASS"), Type: redis.NodeType})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := fmt.Sprintf("test-login-%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = client.Del("dogx:auth:rate:" + endpoint + ":192.0.2.1") })
	httpx.SetErrorHandlerCtx(response.HandleError)
	t.Cleanup(func() { httpx.SetErrorHandlerCtx(nil) })
	calls := 0
	next := func(w http.ResponseWriter, _ *http.Request) { calls++; w.WriteHeader(204) }
	first := NewLoginRateLimitMiddleware(loginConfigStub{enabled: true}, client, nil, endpoint).Handle(next)
	second := NewLoginRateLimitMiddleware(loginConfigStub{enabled: true}, client, nil, endpoint).Handle(next)
	for i, handler := range []http.HandlerFunc{first, second, first} {
		req := httptest.NewRequest("POST", "/auth/login", nil)
		req.RemoteAddr = "192.0.2.1:123"
		out := httptest.NewRecorder()
		handler(out, req)
		want := 204
		if i == 2 {
			want = 429
		}
		if out.Code != want {
			t.Fatalf("attempt %d: %d", i, out.Code)
		}
	}
	if calls != 2 {
		t.Fatal("over-quota request reached login")
	}
}
