package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx/logtest"
	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestConfigDSNEscapesCredentialsAndPreservesTimeZone(t *testing.T) {
	databaseConf := Config{
		Host:     "2001:db8::1",
		Port:     5432,
		User:     "dogx user",
		Password: `p@ss:\'word`,
		Database: "dogx_dev",
		SSLMode:  "disable",
		TimeZone: "Asia/Shanghai",
	}

	want := `host='2001:db8::1' port=5432 user='dogx user' password='p@ss:\\\'word' dbname='dogx_dev' sslmode=disable TimeZone=Asia/Shanghai`
	if got := databaseConf.DSN(); got != want {
		t.Fatalf("unexpected DSN:\n got: %s\nwant: %s", got, want)
	}
}

func TestConfigLoadsDeploymentDefaults(t *testing.T) {
	var value struct{ Postgres Config }
	if err := conf.LoadFromYamlBytes([]byte("Postgres:\n  Host: localhost\n  User: dogx\n  Password: test-password\n  Database: dogx_test\n"), &value); err != nil {
		t.Fatalf("load PostgreSQL config: %v", err)
	}
	want := Config{
		Host: "localhost", Port: 5432, User: "dogx", Password: "test-password", Database: "dogx_test",
		SSLMode: "disable", TimeZone: "Asia/Shanghai", MaxIdleConns: 10, MaxOpenConns: 50, ConnMaxLifetime: time.Hour,
	}
	if value.Postgres != want {
		t.Fatalf("loaded PostgreSQL config = %+v, want %+v", value.Postgres, want)
	}
}

func TestConfigLoadsExplicitConnectionSettings(t *testing.T) {
	var value struct{ Postgres Config }
	if err := conf.LoadFromYamlBytes([]byte(`Postgres:
  Host: db.internal
  Port: 6432
  User: app
  Password: test-password
  Database: app_test
  SSLMode: verify-full
  TimeZone: UTC
  MaxIdleConns: 0
  MaxOpenConns: 7
  ConnMaxLifetime: 3m
`), &value); err != nil {
		t.Fatalf("load PostgreSQL config: %v", err)
	}
	want := Config{
		Host: "db.internal", Port: 6432, User: "app", Password: "test-password", Database: "app_test",
		SSLMode: "verify-full", TimeZone: "UTC", MaxIdleConns: 0, MaxOpenConns: 7, ConnMaxLifetime: 3 * time.Minute,
	}
	if value.Postgres != want {
		t.Fatalf("loaded PostgreSQL config = %+v, want %+v", value.Postgres, want)
	}
}

func TestLoggerKeepsQueryPlaceholders(t *testing.T) {
	filter, ok := newLogger().(interface {
		ParamsFilter(context.Context, string, ...any) (string, []any)
	})
	if !ok {
		t.Fatal("database logger does not implement GORM parameter filtering")
	}
	query := "SELECT $1::text"
	sql, params := filter.ParamsFilter(context.Background(), query, "business-parameter-secret")
	if sql != query || len(params) != 0 {
		t.Fatalf("logger would interpolate parameter values: sql=%q count=%d", sql, len(params))
	}
}

func TestOpenRejectsInvalidConnectionSettings(t *testing.T) {
	logtest.Discard(t)
	db, sqlDB, err := Open(Config{SSLMode: "invalid"})
	if db != nil || sqlDB != nil || err == nil || !strings.Contains(err.Error(), "connect postgres") {
		t.Fatalf("invalid connection settings returned db=%v pool=%v error=%v", db, sqlDB, err)
	}
}

func TestOpenConfiguresPoolLimits(t *testing.T) {
	conn := &testConnection{}
	pool := sql.OpenDB(testConnector{conn})
	t.Cleanup(func() { _ = pool.Close() })
	db, sqlDB, err := open(Config{MaxIdleConns: 0, MaxOpenConns: 7, ConnMaxLifetime: 3 * time.Minute}, func(_ gorm.Dialector, options ...gorm.Option) (*gorm.DB, error) {
		return gorm.Open(gormpostgres.New(gormpostgres.Config{Conn: pool}), options...)
	})
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	if db == nil || sqlDB != pool {
		t.Fatal("successful initialization did not return the configured database and pool")
	}
	if got := pool.Stats().MaxOpenConnections; got != 7 {
		t.Fatalf("max open connections = %d, want 7", got)
	}
	if conn.closed != 1 || pool.Stats().Idle != 0 {
		t.Fatalf("zero idle limit did not close the idle connection: closes=%d stats=%+v", conn.closed, pool.Stats())
	}
}

func TestOpenClosesPoolWhenAutomaticPingFails(t *testing.T) {
	for _, closeFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "closed", true: "close failure preserves original"}[closeFails], func(t *testing.T) {
			logtest.Discard(t)
			pingErr := errors.New("test PostgreSQL ping failed")
			closeErr := errors.New("test PostgreSQL close failed")
			conn := &testConnection{pingErr: pingErr}
			if closeFails {
				conn.closeErr = closeErr
			}
			pool := sql.OpenDB(testConnector{conn})
			t.Cleanup(func() { _ = pool.Close() })
			// Exercise GORM's real initialization and Ping with a controlled
			// database/sql connection; no external server or port is contacted.
			db, sqlDB, err := open(Config{}, func(_ gorm.Dialector, options ...gorm.Option) (*gorm.DB, error) {
				return gorm.Open(gormpostgres.New(gormpostgres.Config{Conn: pool}), options...)
			})
			if db != nil || sqlDB != nil || !errors.Is(err, pingErr) {
				t.Fatalf("failed open returned db=%v pool=%v error=%v", db, sqlDB, err)
			}
			if errors.Is(err, closeErr) != closeFails {
				t.Fatalf("close error lost or invented: %v", err)
			}
			if conn.closed != 1 {
				t.Fatalf("failed initialization closed connection %d times, want 1", conn.closed)
			}
			if err := pool.Ping(); err == nil || errors.Is(err, pingErr) {
				t.Fatalf("failed initialization left a reusable pool: %v", err)
			}
		})
	}
}

type testConnection struct {
	pingErr, closeErr error
	closed            int
}

func (c *testConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c *testConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}
func (c *testConnection) Ping(context.Context) error { return c.pingErr }
func (c *testConnection) Close() error               { c.closed++; return c.closeErr }

type testConnector struct{ connection *testConnection }

func (c testConnector) Connect(context.Context) (driver.Conn, error) { return c.connection, nil }
func (c testConnector) Driver() driver.Driver                        { return testDriver{c.connection} }

type testDriver struct{ connection *testConnection }

func (d testDriver) Open(string) (driver.Conn, error) { return d.connection, nil }
