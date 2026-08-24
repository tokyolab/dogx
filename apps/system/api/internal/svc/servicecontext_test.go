package svc

import (
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/config"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

func TestNewServiceContextBuildsDirectRPCClient(t *testing.T) {
	rpcConfig := zrpc.NewDirectClientConf([]string{"127.0.0.1:65535"}, "", "")
	rpcConfig.NonBlock = true
	c := config.Config{SystemRpc: rpcConfig}
	c.Name = "system-api"
	c.Auth.SessionKeyPrefix = "dogx:test:auth:session"
	c.RedisConf = redis.RedisConf{Host: "127.0.0.1:6379", Type: redis.NodeType, NonBlock: true}

	ctx, err := NewServiceContext(c)
	if err != nil {
		t.Fatalf("create service context: %v", err)
	}
	if ctx.Config.Name != "system-api" {
		t.Fatalf("service name = %q, want system-api", ctx.Config.Name)
	}
	if ctx.SystemRpc == nil {
		t.Fatal("system RPC client was not initialized")
	}
	if ctx.Redis == nil || ctx.Sessions == nil || ctx.SessionAuth == nil {
		t.Fatal("API session authentication was not initialized")
	}
}
