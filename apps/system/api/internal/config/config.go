package config

import (
	"time"

	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	App       AppConf
	SystemRpc zrpc.RpcClientConf
}

type AppConf struct {
	Version          string        `json:",default=v0.1.0"`
	ReadinessTimeout time.Duration `json:",default=2s"`
}
