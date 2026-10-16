package config

import (
	"reflect"
	"testing"

	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/zeromicro/go-zero/zrpc"
)

func TestProtectServerContentPreservesStatisticsAndConfiguredMethods(t *testing.T) {
	for _, stat := range []bool{true, false} {
		conf := zrpc.RpcServerConf{}
		conf.Middlewares.Stat = stat
		conf.Middlewares.StatConf.IgnoreContentMethods = []string{"/example.Service/Existing", job.Job_CreateJob_FullMethodName}
		ProtectServerContent(&conf)
		ProtectServerContent(&conf)
		want := []string{"/example.Service/Existing", job.Job_CreateJob_FullMethodName, job.Job_UpdateJob_FullMethodName, job.Job_GetJob_FullMethodName, job.Job_TriggerJob_FullMethodName}
		if conf.Middlewares.Stat != stat || !reflect.DeepEqual(conf.Middlewares.StatConf.IgnoreContentMethods, want) {
			t.Fatalf("stat=%t ignored=%v", conf.Middlewares.Stat, conf.Middlewares.StatConf.IgnoreContentMethods)
		}
	}
	ProtectServerContent(nil)
}
