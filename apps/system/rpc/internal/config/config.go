package config

import (
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/database"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	App       AppConf
	Postgres  database.PostgresConf
	RedisConf redis.RedisConf
}

type AppConf struct {
	ReadinessTimeout time.Duration `json:",default=2s"`
}
