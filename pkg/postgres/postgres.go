package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Config struct {
	Host            string
	Port            int `json:",default=5432"`
	User            string
	Password        string
	Database        string
	SSLMode         string        `json:",default=disable,options=disable|require|verify-ca|verify-full"`
	TimeZone        string        `json:",default=Asia/Shanghai"`
	MaxIdleConns    int           `json:",default=10,range=[0:1000]"`
	MaxOpenConns    int           `json:",default=50,range=[1:10000]"`
	ConnMaxLifetime time.Duration `json:",default=1h"`
}

func (c Config) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s TimeZone=%s",
		quoteDSNValue(c.Host),
		c.Port,
		quoteDSNValue(c.User),
		quoteDSNValue(c.Password),
		quoteDSNValue(c.Database),
		c.SSLMode,
		c.TimeZone,
	)
}

func Open(c Config) (*gorm.DB, *sql.DB, error) {
	return open(c, gorm.Open)
}

func open(c Config, openGORM func(gorm.Dialector, ...gorm.Option) (*gorm.DB, error)) (*gorm.DB, *sql.DB, error) {
	db, err := openGORM(gormpostgres.Open(c.DSN()), &gorm.Config{Logger: newLogger()})
	if err != nil {
		connectErr := fmt.Errorf("connect postgres: %w", err)
		// GORM can return a database handle together with an automatic Ping
		// failure. Its pool is already live and remains our responsibility.
		if db != nil {
			pool, poolErr := db.DB()
			if poolErr != nil {
				connectErr = errors.Join(connectErr, fmt.Errorf("get failed postgres connection pool: %w", poolErr))
			} else if closeErr := pool.Close(); closeErr != nil {
				connectErr = errors.Join(connectErr, fmt.Errorf("close failed postgres connection pool: %w", closeErr))
			}
		}
		return nil, nil, connectErr
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, fmt.Errorf("get postgres connection pool: %w", err)
	}

	sqlDB.SetMaxIdleConns(c.MaxIdleConns)
	sqlDB.SetMaxOpenConns(c.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(c.ConnMaxLifetime)

	return db, sqlDB, nil
}

func newLogger() logger.Interface {
	return logger.New(logWriter{}, logger.Config{
		SlowThreshold:        200 * time.Millisecond,
		LogLevel:             logger.Warn,
		ParameterizedQueries: true,
	})
}

type logWriter struct{}

func (logWriter) Printf(format string, args ...any) {
	// GORM filters ordinary successful queries at Warn. Keep its remaining
	// diagnostics in the configured logx error stream without expanding params.
	logx.Errorf(format, args...)
}

func quoteDSNValue(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return `'` + escaped + `'`
}
