package repository

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestLoginLogListUsesExactUsernameForCountAndPage(t *testing.T) {
	var statements bytes.Buffer
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN: "host=localhost user=test dbname=test sslmode=disable",
	}), &gorm.Config{
		DryRun: true, DisableAutomaticPing: true,
		Logger: logger.New(log.New(&statements, "", 0), logger.Config{LogLevel: logger.Info}),
	})
	if err != nil {
		t.Fatal(err)
	}
	// GORM normally clears SQL after execution, but retains it in DryRun mode.
	// Reset before each query so the page SQL is rebuilt after the count SQL.
	if err := db.Callback().Query().Before("gorm:query").Register("test:reset_dry_run_sql", func(tx *gorm.DB) {
		tx.Statement.SQL.Reset()
		tx.Statement.Vars = nil
	}); err != nil {
		t.Fatal(err)
	}
	r := &loginLogRepository{db: db}
	if _, _, err := r.List(context.Background(), LoginLogListQuery{Username: " AlIce ", Limit: 20}); err != nil {
		t.Fatal(err)
	}
	sql := statements.String()
	if strings.Count(sql, "LOWER(username) = LOWER('AlIce')") != 2 {
		t.Fatalf("count and page must use the same trimmed exact username predicate: %s", sql)
	}
	if strings.Contains(sql, "LIKE") || !strings.Contains(sql, "ORDER BY id DESC") {
		t.Fatalf("unexpected login log query: %s", sql)
	}
}

func TestLoginLogRepositoryRejectsNilLog(t *testing.T) {
	repository := &loginLogRepository{}
	if err := repository.Create(context.Background(), nil); err == nil {
		t.Fatal("expected nil login log to be rejected")
	}
}

func TestNewLoginLogRepositoryRejectsNilDatabase(t *testing.T) {
	if _, err := NewLoginLogRepository(nil); err == nil {
		t.Fatal("expected nil login log repository database to be rejected")
	}
}

func TestLoginLogListRejectsInvalidQuery(t *testing.T) {
	r := &loginLogRepository{}
	for _, q := range []LoginLogListQuery{{}, {Limit: -1}, {Limit: 20, Offset: -1}} {
		if _, _, err := r.List(context.Background(), q); err == nil {
			t.Fatal("invalid pagination accepted")
		}
	}
	if _, _, err := r.List(nil, LoginLogListQuery{Limit: 20}); err == nil {
		t.Fatal("nil context accepted")
	}
}
