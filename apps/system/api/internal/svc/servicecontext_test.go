package svc

import (
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/config"

	"github.com/zeromicro/go-zero/zrpc"
)

func TestNewServiceContextBuildsDirectRPCClient(t *testing.T) {
	rpcConfig := zrpc.NewDirectClientConf([]string{"127.0.0.1:65535"}, "", "")
	rpcConfig.NonBlock = true
	c := config.Config{SystemRpc: rpcConfig}
	c.Name = "system-api"

	ctx := NewServiceContext(c)
	if ctx.Config.Name != "system-api" {
		t.Fatalf("service name = %q, want system-api", ctx.Config.Name)
	}
	if ctx.SystemRpc == nil {
		t.Fatal("system RPC client was not initialized")
	}
}
