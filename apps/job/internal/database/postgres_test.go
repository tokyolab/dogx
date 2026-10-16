package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/zeromicro/go-zero/core/logx/logtest"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestPostgresConfDSNEscapesCredentialsAndPreservesTimeZone(t *testing.T) {
	databaseConf := PostgresConf{
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

func TestPostgresLoggerKeepsQueryPlaceholders(t *testing.T) {
	filter, ok := newPostgresLogger().(interface {
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

func TestOpenPostgresClosesPoolWhenAutomaticPingFails(t *testing.T) {
	for _, closeFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "closed", true: "close failure preserves original"}[closeFails], func(t *testing.T) {
			logtest.Discard(t)
			pingErr := errors.New("test PostgreSQL ping failed")
			closeErr := errors.New("test PostgreSQL close failed")
			conn := &failedPingConnection{pingErr: pingErr}
			if closeFails {
				conn.closeErr = closeErr
			}
			pool := sql.OpenDB(failedPingConnector{conn})
			t.Cleanup(func() { _ = pool.Close() })
			// Exercise GORM's real initialization and Ping with a controlled
			// database/sql connection; no external server or port is contacted.
			db, sqlDB, err := openPostgres(PostgresConf{}, func(_ gorm.Dialector, options ...gorm.Option) (*gorm.DB, error) {
				return gorm.Open(postgres.New(postgres.Config{Conn: pool}), options...)
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

type failedPingConnection struct {
	pingErr, closeErr error
	closed            int
}

func (c *failedPingConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c *failedPingConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}
func (c *failedPingConnection) Ping(context.Context) error { return c.pingErr }
func (c *failedPingConnection) Close() error               { c.closed++; return c.closeErr }

type failedPingConnector struct{ connection *failedPingConnection }

func (c failedPingConnector) Connect(context.Context) (driver.Conn, error) { return c.connection, nil }
func (c failedPingConnector) Driver() driver.Driver                        { return failedPingDriver{c.connection} }

type failedPingDriver struct{ connection *failedPingConnection }

func (d failedPingDriver) Open(string) (driver.Conn, error) { return d.connection, nil }
