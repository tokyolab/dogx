//go:build integration

package svc

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	gormadapter "github.com/casbin/gorm-adapter/v3"
	"github.com/tokyolab/dogx/apps/system/api/internal/config"
	"github.com/tokyolab/dogx/apps/system/internal/authorization"
	systemdb "github.com/tokyolab/dogx/apps/system/internal/database"
	"github.com/tokyolab/dogx/apps/system/internal/migration"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/testutil"

	"github.com/jackc/pgx/v5"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

func TestNewServiceContextWiresAuthorizationDependencies(t *testing.T) {
	setupDB, setupSQLDB := testutil.OpenPostgres(t)
	provider, err := migration.NewProvider(setupSQLDB)
	if err != nil {
		t.Fatalf("create migration provider: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	testRole := model.Role{
		Code:   "service_context_policy_probe",
		Name:   "Service Context Policy Probe",
		Status: model.RecordStatusEnabled,
	}
	if err := setupDB.WithContext(ctx).Create(&testRole).Error; err != nil {
		t.Fatalf("create authorization policy probe role: %v", err)
	}
	subject, err := authorization.RoleSubject(testRole.ID)
	if err != nil {
		t.Fatalf("build authorization policy probe subject: %v", err)
	}
	if err := setupDB.WithContext(ctx).Create(&gormadapter.CasbinRule{
		Ptype: "p",
		V0:    subject,
		V1:    "/role/api/update",
		V2:    "POST",
	}).Error; err != nil {
		t.Fatalf("create authorization policy probe: %v", err)
	}

	var schema string
	if err := setupDB.Raw("SELECT current_schema()").Scan(&schema).Error; err != nil {
		t.Fatalf("load integration schema: %v", err)
	}
	if !strings.HasPrefix(schema, "dogx_it_") {
		t.Fatalf("refuse to wire API service context to unexpected schema %q", schema)
	}
	// OpenPostgres creates new connections, so pass the isolated test schema
	// through libpq's standard connection option without changing production
	// configuration or sharing PostgreSQL's public schema.
	t.Setenv("PGOPTIONS", "-c search_path="+schema)

	c := apiServiceConfig(t)
	unique := fmt.Sprintf("dogx:test:api-svc:%d", time.Now().UnixNano())
	c.Auth.SessionKeyPrefix = unique + ":session"
	c.Authorization.PolicyChannel = unique + ":policy"

	serviceCtx, err := NewServiceContext(c)
	if err != nil {
		t.Fatalf("create API service context: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := serviceCtx.Close(); err != nil {
				t.Errorf("close API service context: %v", err)
			}
		}
	})

	if serviceCtx.SystemRpc == nil || serviceCtx.Redis == nil || serviceCtx.Sessions == nil {
		t.Fatal("API transport or session dependencies were not initialized")
	}
	if serviceCtx.SessionAuth == nil || serviceCtx.Authorization == nil {
		t.Fatal("API authentication or authorization middleware was not initialized")
	}
	if serviceCtx.AuthorizationEnforcer == nil || serviceCtx.AuthorizationReadiness == nil ||
		serviceCtx.policyWatcher == nil || serviceCtx.policyReloader == nil || serviceCtx.sqlDB == nil {
		t.Fatal("API authorization runtime was not completely initialized")
	}

	readyCtx, readyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer readyCancel()
	if err := serviceCtx.AuthorizationReadiness.Check(readyCtx); err != nil {
		t.Fatalf("API authorization dependencies are not ready: %v", err)
	}

	allowed, err := serviceCtx.AuthorizationEnforcer.Enforce(subject, "/role/api/update", "POST")
	if err != nil {
		t.Fatalf("enforce authorization policy probe: %v", err)
	}
	if !allowed {
		t.Fatal("ordinary role Casbin policy was not loaded into the API enforcer")
	}

	wiredSQLDB := serviceCtx.sqlDB
	if err := serviceCtx.Close(); err != nil {
		t.Fatalf("close API service context: %v", err)
	}
	closed = true
	if err := wiredSQLDB.PingContext(context.Background()); err == nil {
		t.Fatal("API service context left its PostgreSQL pool open")
	}
}

func TestNewServiceContextRejectsInvalidRedisConfiguration(t *testing.T) {
	c := apiServiceConfig(t)
	c.RedisConf = redis.RedisConf{}

	serviceCtx, err := NewServiceContext(c)
	if err == nil {
		if serviceCtx != nil {
			_ = serviceCtx.Close()
		}
		t.Fatal("expected invalid Redis configuration to be rejected")
	}
}

func apiServiceConfig(t testing.TB) config.Config {
	t.Helper()

	parsed, err := pgx.ParseConfig(testutil.PostgresDSN(t))
	if err != nil {
		t.Fatalf("parse test PostgreSQL DSN: %v", err)
	}
	redisHost := strings.TrimSpace(os.Getenv("DOGX_TEST_REDIS_HOST"))
	if redisHost == "" {
		t.Fatal("DOGX_TEST_REDIS_HOST is required for API service-context integration tests")
	}

	return config.Config{
		Auth: config.AuthConf{
			AccessSecret:     "0123456789abcdef0123456789abcdef",
			SessionKeyPrefix: "dogx:test:api:session",
		},
		Authorization: config.AuthorizationConf{
			PolicyChannel:  "dogx:test:api:authorization:policy",
			ReloadInterval: time.Hour,
		},
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
		SystemRpc: zrpc.RpcClientConf{
			Endpoints: []string{"127.0.0.1:1"},
			NonBlock:  true,
			Timeout:   200,
		},
	}
}
