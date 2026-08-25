package config

import (
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/database"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	App           AppConf
	Auth          AuthConf
	Authorization AuthorizationConf
	Postgres      database.PostgresConf
	RedisConf     redis.RedisConf
	SystemRpc     zrpc.RpcClientConf
}

type AppConf struct {
	Version          string        `json:",default=v0.1.0"`
	ReadinessTimeout time.Duration `json:",default=2s"`
}

type AuthConf struct {
	AccessSecret     string
	SessionKeyPrefix string `json:",default=dogx:auth:session"`
}

type AuthorizationConf struct {
	PolicyChannel  string        `json:",default=dogx:authorization:policy"`
	ReloadInterval time.Duration `json:",default=60s"`
}
