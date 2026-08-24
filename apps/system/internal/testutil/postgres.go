//go:build integration

package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	postgresDSNEnv   = "DOGX_TEST_POSTGRES_DSN"
	testSchemaPrefix = "dogx_it_"
)

var schemaSequence atomic.Uint64

// OpenPostgres creates an isolated schema in a dedicated test database.
func OpenPostgres(t testing.TB) (*gorm.DB, *sql.DB) {
	t.Helper()

	dsn := strings.TrimSpace(os.Getenv(postgresDSNEnv))
	if dsn == "" {
		t.Fatalf("%s is required for PostgreSQL integration tests", postgresDSNEnv)
	}

	baseConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL integration test DSN: %v", err)
	}
	if !strings.HasSuffix(strings.ToLower(baseConfig.Database), "_test") {
		t.Fatalf("refuse to use PostgreSQL database %q: test database name must end with _test", baseConfig.Database)
	}

	adminDB := stdlib.OpenDB(*baseConfig)
	adminDB.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := adminDB.Close(); err != nil {
			t.Errorf("close PostgreSQL integration admin connection: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := adminDB.PingContext(ctx); err != nil {
		t.Fatalf("connect PostgreSQL integration test database: %v", err)
	}

	schema := fmt.Sprintf("%s%d_%d", testSchemaPrefix, os.Getpid(), schemaSequence.Add(1))
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatalf("create PostgreSQL integration test schema: %v", err)
	}
	t.Cleanup(func() {
		if !strings.HasPrefix(schema, testSchemaPrefix) {
			t.Errorf("refuse to drop unexpected PostgreSQL schema %q", schema)
			return
		}

		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := adminDB.ExecContext(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop PostgreSQL integration test schema %q: %v", schema, err)
		}
	})

	testConfig := baseConfig.Copy()
	if testConfig.RuntimeParams == nil {
		testConfig.RuntimeParams = make(map[string]string)
	}
	testConfig.RuntimeParams["search_path"] = schema
	testDB := stdlib.OpenDB(*testConfig)
	testDB.SetMaxIdleConns(2)
	testDB.SetMaxOpenConns(5)
	t.Cleanup(func() {
		if err := testDB.Close(); err != nil {
			t.Errorf("close PostgreSQL integration test connection: %v", err)
		}
	})

	if err := testDB.PingContext(ctx); err != nil {
		t.Fatalf("connect isolated PostgreSQL integration test schema: %v", err)
	}

	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: testDB}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("initialize GORM integration test connection: %v", err)
	}

	return gormDB, testDB
}
