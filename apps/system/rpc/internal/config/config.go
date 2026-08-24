package config

import (
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/database"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	App            AppConf
	Authentication AuthenticationConf
	Postgres       database.PostgresConf
	RedisConf      redis.RedisConf
}

type AppConf struct {
	ReadinessTimeout time.Duration `json:",default=2s"`
}

type AuthenticationConf struct {
	AccessSecret          string
	AccessExpire          time.Duration `json:",default=15m"`
	RefreshExpire         time.Duration `json:",default=168h"`
	Issuer                string        `json:",default=dogx"`
	SessionKeyPrefix      string        `json:",default=dogx:auth:session"`
	UserSessionsKeyPrefix string        `json:",default=dogx:auth:user_sessions"`
}
