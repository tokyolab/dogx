package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type PostgresConf struct {
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

func (c PostgresConf) DSN() string {
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

func OpenPostgres(c PostgresConf) (*gorm.DB, *sql.DB, error) {
	db, err := gorm.Open(postgres.Open(c.DSN()), &gorm.Config{})
	if err != nil {
		return nil, nil, fmt.Errorf("connect postgres: %w", err)
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

func quoteDSNValue(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return `'` + escaped + `'`
}
