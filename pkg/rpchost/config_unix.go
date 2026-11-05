//go:build linux || darwin || freebsd

package rpchost

import (
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/service"
)

func normalizeShutdown(config *service.ServiceConf, policy ShutdownConfig) {
	config.Shutdown = proc.ShutdownConf{
		WrapUpTime: policy.WrapUpTime,
		WaitTime:   policy.WaitTime,
	}
}
