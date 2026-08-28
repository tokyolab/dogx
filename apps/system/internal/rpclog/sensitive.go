package rpclog

import (
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/zrpc"
)

var sensitiveSystemMethods = []string{
	system.System_Login_FullMethodName,
	system.System_RefreshCredentials_FullMethodName,
	system.System_RevokeSession_FullMethodName,
	system.System_ChangePassword_FullMethodName,
}

// ProtectClientContent prevents go-zero's RPC client duration interceptor from
// logging requests or responses that contain credentials or session secrets.
func ProtectClientContent() {
	protectClientContent(zrpc.DontLogClientContentForMethod)
}

func protectClientContent(register func(string)) {
	for _, method := range sensitiveSystemMethods {
		register(method)
	}
}

// ProtectServerContent prevents go-zero's RPC server stat interceptor from
// logging requests that contain credentials or session secrets.
func ProtectServerContent(conf *zrpc.RpcServerConf) {
	if conf == nil {
		return
	}

	ignored := make(map[string]struct{}, len(conf.Middlewares.StatConf.IgnoreContentMethods)+len(sensitiveSystemMethods))
	for _, method := range conf.Middlewares.StatConf.IgnoreContentMethods {
		ignored[method] = struct{}{}
	}
	for _, method := range sensitiveSystemMethods {
		if _, exists := ignored[method]; exists {
			continue
		}
		conf.Middlewares.StatConf.IgnoreContentMethods = append(
			conf.Middlewares.StatConf.IgnoreContentMethods,
			method,
		)
		ignored[method] = struct{}{}
	}
}
