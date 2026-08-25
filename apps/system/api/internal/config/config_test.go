package config

import (
	"path/filepath"
	"testing"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestExampleConfigLoads(t *testing.T) {
	t.Setenv("DOGX_ACCESS_SECRET", "0123456789abcdef0123456789abcdef")
	t.Setenv("DOGX_POSTGRES_PASSWORD", "postgres-test-password")
	t.Setenv("DOGX_REDIS_PASSWORD", "redis-test-password")

	var loaded Config
	path := filepath.Join("..", "..", "etc", "system-api.example.yaml")
	if err := conf.Load(path, &loaded, conf.UseEnv()); err != nil {
		t.Fatalf("load example config: %v", err)
	}

	if loaded.Name != "system-api" || loaded.Port != 8001 {
		t.Fatalf("unexpected REST config: name=%s port=%d", loaded.Name, loaded.Port)
	}
	if len(loaded.SystemRpc.Endpoints) != 1 || loaded.SystemRpc.Endpoints[0] != "127.0.0.1:9001" {
		t.Fatalf("unexpected system RPC endpoints: %v", loaded.SystemRpc.Endpoints)
	}
	if loaded.Auth.AccessSecret != "0123456789abcdef0123456789abcdef" {
		t.Fatal("access token secret was not expanded from the environment")
	}
	if loaded.Auth.SessionKeyPrefix != "dogx:auth:session" ||
		loaded.RedisConf.Host != "127.0.0.1:6379" || loaded.RedisConf.Pass != "redis-test-password" {
		t.Fatalf("unexpected API session Redis configuration: auth=%+v redis=%+v", loaded.Auth, loaded.RedisConf)
	}
	if loaded.Postgres.Password != "postgres-test-password" ||
		loaded.Authorization.PolicyChannel != "dogx:authorization:policy" ||
		loaded.Authorization.ReloadInterval.String() != "1m0s" {
		t.Fatalf("unexpected authorization configuration: postgres=%+v authorization=%+v", loaded.Postgres, loaded.Authorization)
	}
}
