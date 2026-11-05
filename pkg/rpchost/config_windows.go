//go:build windows

package rpchost

import "github.com/zeromicro/go-zero/core/service"

// normalizeShutdown cannot copy durations into go-zero's empty Windows type.
// The host uses ShutdownConfig directly for the same bounded shutdown behavior.
func normalizeShutdown(_ *service.ServiceConf, _ ShutdownConfig) {}
