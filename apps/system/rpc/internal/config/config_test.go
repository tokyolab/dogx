package config

import (
	"path/filepath"
	"testing"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestExampleConfigLoadsWithEnvironmentSecrets(t *testing.T) {
	t.Setenv("DOGX_POSTGRES_PASSWORD", "postgres-test-password")
	t.Setenv("DOGX_REDIS_PASSWORD", "redis-test-password")

	var loaded Config
	path := filepath.Join("..", "..", "etc", "system-rpc.example.yaml")
	if err := conf.Load(path, &loaded, conf.UseEnv()); err != nil {
		t.Fatalf("load example config: %v", err)
	}

	if loaded.Name != "system-rpc" || loaded.ListenOn != "0.0.0.0:9001" {
		t.Fatalf("unexpected RPC config: name=%s listenOn=%s", loaded.Name, loaded.ListenOn)
	}
	if loaded.Postgres.Password != "postgres-test-password" {
		t.Fatal("postgres password was not expanded from the environment")
	}
	if loaded.RedisConf.Pass != "redis-test-password" {
		t.Fatal("redis password was not expanded from the environment")
	}
}
