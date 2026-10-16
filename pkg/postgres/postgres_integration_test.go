//go:build integration

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	pgtest "github.com/tokyolab/dogx/pkg/testutil/postgres"
	"github.com/zeromicro/go-zero/core/logx/logtest"

	"github.com/jackc/pgx/v5"
)

func TestOpenConfiguresAndConnectsPool(t *testing.T) {
	parsed, err := pgx.ParseConfig(pgtest.PostgresDSN(t))
	if err != nil {
		t.Fatalf("parse test PostgreSQL DSN: %v", err)
	}

	conf := Config{
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

	gormDB, sqlDB, err := Open(conf)
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

func TestOpenReportsConnectionFailure(t *testing.T) {
	_, _, err := Open(Config{
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

func TestOpenRedactsSlowAndFailedQueryParameters(t *testing.T) {
	parsed, err := pgx.ParseConfig(pgtest.PostgresDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	db, sqlDB, err := Open(Config{
		Host: parsed.Host, Port: int(parsed.Port), User: parsed.User, Password: parsed.Password,
		Database: parsed.Database, SSLMode: "disable", TimeZone: "Asia/Shanghai",
		MaxIdleConns: 1, MaxOpenConns: 1, ConnMaxLifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close PostgreSQL logger test connection: %v", err)
		}
	})
	collector := logtest.NewCollector(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	secret := "business-parameter-must-never-appear-in-query-logs"
	// Deliberate database delay exercises GORM's actual slow-query callback.
	if err := db.WithContext(ctx).Exec("SELECT pg_sleep(0.22), ?::text", secret).Error; err != nil {
		t.Fatal(err)
	}
	slow := collector.String()
	if strings.Contains(slow, secret) || !strings.Contains(slow, "SLOW SQL") || !strings.Contains(slow, "$1") {
		t.Fatalf("slow-query diagnostic lost metadata or exposed params: %s", slow)
	}
	collector.Reset()
	if err := db.WithContext(ctx).Exec("SELECT ?::text, postgres_test_missing_column", secret).Error; err == nil {
		t.Fatal("expected missing-column query to fail")
	}
	failed := collector.String()
	if strings.Contains(failed, secret) || !strings.Contains(failed, "postgres_test_missing_column") || !strings.Contains(failed, "$1") {
		t.Fatalf("failed-query diagnostic lost metadata or exposed params: %s", failed)
	}
}
