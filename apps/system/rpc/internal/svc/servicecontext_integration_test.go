//go:build integration

package svc

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	systemdb "github.com/tokyolab/dogx/apps/system/internal/database"
	"github.com/tokyolab/dogx/apps/system/internal/testutil"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/config"

	"github.com/jackc/pgx/v5"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

const testRedisHostEnv = "DOGX_TEST_REDIS_HOST"

func TestNewServiceContextWiresDependencies(t *testing.T) {
	c := testServiceConfig(t)
	ctx, err := NewServiceContext(c)
	if err != nil {
		t.Fatalf("create service context: %v", err)
	}
	t.Cleanup(func() {
		if err := ctx.Close(); err != nil {
			t.Errorf("close service context: %v", err)
		}
	})

	if ctx.DB == nil {
		t.Error("GORM database was not initialized")
	}
	if ctx.Redis == nil {
		t.Error("Redis client was not initialized")
	}
	if ctx.UserRepo == nil {
		t.Error("user repository was not initialized")
	}
	if ctx.Readiness == nil {
		t.Fatal("readiness checker was not initialized")
	}

	checkCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ctx.Readiness.Check(checkCtx); err != nil {
		t.Fatalf("wired dependencies are not ready: %v", err)
	}
}

func TestNewServiceContextRejectsInvalidRedisConfig(t *testing.T) {
	c := testServiceConfig(t)
	c.RedisConf = redis.RedisConf{}

	ctx, err := NewServiceContext(c)
	if err == nil {
		if ctx != nil {
			_ = ctx.Close()
		}
		t.Fatal("expected Redis initialization failure")
	}
	if !strings.Contains(err.Error(), "initialize redis") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testServiceConfig(t testing.TB) config.Config {
	t.Helper()

	parsed, err := pgx.ParseConfig(testutil.PostgresDSN(t))
	if err != nil {
		t.Fatalf("parse test PostgreSQL DSN: %v", err)
	}
	redisHost := strings.TrimSpace(os.Getenv(testRedisHostEnv))
	if redisHost == "" {
		t.Fatalf("%s is required for service-context integration tests", testRedisHostEnv)
	}

	return config.Config{
		Postgres: systemdb.PostgresConf{
			Host:            parsed.Host,
			Port:            int(parsed.Port),
			User:            parsed.User,
			Password:        parsed.Password,
			Database:        parsed.Database,
			SSLMode:         "disable",
			TimeZone:        "Asia/Shanghai",
			MaxIdleConns:    2,
			MaxOpenConns:    7,
			ConnMaxLifetime: 3 * time.Minute,
		},
		RedisConf: redis.RedisConf{
			Host:               redisHost,
			Type:               redis.NodeType,
			PingTimeout:        time.Second,
			DisableIdentity:    true,
			MaintNotifications: "disabled",
		},
	}
}
