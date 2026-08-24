package svc

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/config"
	"github.com/tokyolab/dogx/apps/system/api/internal/middleware"
	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type RedisPinger interface {
	PingCtx(ctx context.Context) bool
}

type ServiceContext struct {
	Config      config.Config
	SystemRpc   systemclient.System
	Redis       RedisPinger
	Sessions    authn.SessionReader
	SessionAuth rest.Middleware
}

func NewServiceContext(c config.Config) (*ServiceContext, error) {
	systemRPC := systemclient.NewSystem(zrpc.MustNewClient(c.SystemRpc))
	redisClient, err := redis.NewRedis(c.RedisConf)
	if err != nil {
		return nil, err
	}
	sessions, err := authn.NewRedisSessionReader(redisClient, c.Auth.SessionKeyPrefix)
	if err != nil {
		return nil, err
	}

	return &ServiceContext{
		Config:      c,
		SystemRpc:   systemRPC,
		Redis:       redisClient,
		Sessions:    sessions,
		SessionAuth: middleware.NewSessionAuthMiddleware(sessions).Handle,
	}, nil
}
