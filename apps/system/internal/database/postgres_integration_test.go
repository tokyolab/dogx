//go:build integration

package database

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/testutil"

	"github.com/jackc/pgx/v5"
)

func TestOpenPostgresConfiguresAndConnectsPool(t *testing.T) {
	parsed, err := pgx.ParseConfig(testutil.PostgresDSN(t))
	if err != nil {
		t.Fatalf("parse test PostgreSQL DSN: %v", err)
	}

	conf := PostgresConf{
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
	}

	gormDB, sqlDB, err := OpenPostgres(conf)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	})
	if gormDB == nil {
		t.Fatal("GORM database was not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	if got := sqlDB.Stats().MaxOpenConnections; got != conf.MaxOpenConns {
		t.Fatalf("max open connections = %d, want %d", got, conf.MaxOpenConns)
	}
}

func TestOpenPostgresReportsConnectionFailure(t *testing.T) {
	_, _, err := OpenPostgres(PostgresConf{
		Host:            "127.0.0.1",
		Port:            1,
		User:            "dogx",
		Password:        "invalid",
		Database:        "dogx_test",
		SSLMode:         "disable",
		TimeZone:        "Asia/Shanghai",
		MaxOpenConns:    1,
		ConnMaxLifetime: time.Minute,
	})
	if err == nil {
		t.Fatal("expected connection failure")
	}
	if !strings.Contains(err.Error(), "connect postgres") {
		t.Fatalf("unexpected error: %v", err)
	}
}
