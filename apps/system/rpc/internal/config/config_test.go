package config

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestExampleConfigLoadsWithEnvironmentSecrets(t *testing.T) {
	t.Setenv("DOGX_ACCESS_SECRET", "0123456789abcdef0123456789abcdef")
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
	if loaded.Authentication.AccessSecret != "0123456789abcdef0123456789abcdef" {
		t.Fatal("access token secret was not expanded from the environment")
	}
	if loaded.Authentication.AccessExpire != 15*time.Minute ||
		loaded.Authentication.RefreshExpire != 7*24*time.Hour {
		t.Fatalf("unexpected authentication expiry: %+v", loaded.Authentication)
	}
	if loaded.Authentication.SessionKeyPrefix != "dogx:auth:session" ||
		loaded.Authentication.UserSessionsKeyPrefix != "dogx:auth:user_sessions" {
		t.Fatalf("unexpected session key prefixes: %+v", loaded.Authentication)
	}
	if loaded.Authorization.PolicyChannel != "dogx:authorization:policy" {
		t.Fatalf("unexpected authorization configuration: %+v", loaded.Authorization)
	}
}
