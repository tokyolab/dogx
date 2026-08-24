package svc

import (
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestServiceContextCloseWithoutDatabase(t *testing.T) {
	if err := (&ServiceContext{}).Close(); err != nil {
		t.Fatalf("close empty service context: %v", err)
	}
}

func TestServiceContextCloseDatabaseHandle(t *testing.T) {
	db, err := sql.Open("pgx", "host=localhost user=dogx dbname=dogx_test sslmode=disable")
	if err != nil {
		t.Fatalf("open database handle: %v", err)
	}

	ctx := &ServiceContext{sqlDB: db}
	if err := ctx.Close(); err != nil {
		t.Fatalf("close service context: %v", err)
	}
	if err := db.Ping(); err == nil {
		t.Fatal("closed database handle still accepts operations")
	}
}
