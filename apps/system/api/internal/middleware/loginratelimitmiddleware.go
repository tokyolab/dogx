// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package middleware

import (
	"context"
	"github.com/tokyolab/dogx/apps/system/api/internal/clientip"
	"github.com/tokyolab/dogx/apps/system/internal/loginprotection"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/zeromicro/go-zero/core/limit"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http"
)

type LoginRateLimitMiddleware struct {
	config   loginprotection.ConfigProvider
	resolver *clientip.Resolver
	endpoint string
	take     func(context.Context, string, *system.LoginSecurityConfig) (int, error)
}

func NewLoginRateLimitMiddleware(config loginprotection.ConfigProvider, client *redis.Redis, resolver *clientip.Resolver, endpoint string) *LoginRateLimitMiddleware {
	return &LoginRateLimitMiddleware{config: config, resolver: resolver, endpoint: endpoint, take: func(ctx context.Context, key string, cfg *system.LoginSecurityConfig) (int, error) {
		return limit.NewPeriodLimit(int(cfg.RateLimitWindowSeconds), int(cfg.RateLimitMaxRequests), client, "dogx:auth:rate:").TakeCtx(ctx, key)
	}}
}

func (m *LoginRateLimitMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg, err := m.config.Current()
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, status.Error(codes.Unavailable, "login protection unavailable"))
			return
		}
		if !cfg.RateLimitEnabled {
			next(w, r)
			return
		}
		ip := m.resolver.Resolve(r)
		if ip == "" {
			httpx.ErrorCtx(r.Context(), w, status.Error(codes.InvalidArgument, "invalid client address"))
			return
		}
		result, err := m.take(r.Context(), m.endpoint+":"+ip, cfg)
		if err != nil {
			logx.WithContext(r.Context()).Errorf("login rate limit: %v", err)
			httpx.ErrorCtx(r.Context(), w, status.Error(codes.Unavailable, "login protection unavailable"))
			return
		}
		if result == limit.OverQuota {
			httpx.ErrorCtx(r.Context(), w, status.Error(codes.ResourceExhausted, "too many login requests"))
			return
		}
		if result != limit.Allowed && result != limit.HitQuota {
			httpx.ErrorCtx(r.Context(), w, status.Error(codes.Unavailable, "login protection unavailable"))
			return
		}
		next(w, r)
	}
}
