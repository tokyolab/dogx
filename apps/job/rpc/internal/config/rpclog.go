package config

import (
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/zeromicro/go-zero/zrpc"
)

// ProtectServerContent keeps RPC statistics while suppressing arbitrary Job
// parameters, which may contain secrets, from content logs.
func ProtectServerContent(conf *zrpc.RpcServerConf) {
	if conf == nil {
		return
	}
	ignored := make(map[string]bool, len(conf.Middlewares.StatConf.IgnoreContentMethods)+4)
	for _, method := range conf.Middlewares.StatConf.IgnoreContentMethods {
		ignored[method] = true
	}
	for _, method := range []string{
		job.Job_CreateJob_FullMethodName,
		job.Job_UpdateJob_FullMethodName,
		job.Job_GetJob_FullMethodName,
		job.Job_TriggerJob_FullMethodName,
	} {
		if !ignored[method] {
			conf.Middlewares.StatConf.IgnoreContentMethods = append(conf.Middlewares.StatConf.IgnoreContentMethods, method)
			ignored[method] = true
		}
	}
}
