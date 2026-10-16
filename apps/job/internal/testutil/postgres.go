//go:build integration

package testutil

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/migration"
	pgtest "github.com/tokyolab/dogx/pkg/testutil/postgres"
	"gorm.io/gorm"
)

// OpenMigratedPostgres applies only this domain's embedded migrations to an
// isolated test schema. It never reads a development configuration file.
func OpenMigratedPostgres(t testing.TB) (*gorm.DB, *sql.DB) {
	t.Helper()
	db, sqlDB := pgtest.OpenPostgres(t)
	provider, err := migration.NewProvider(sqlDB)
	if err != nil {
		t.Fatalf("create Job migration provider: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("apply Job migrations: %v", err)
	}
	return db, sqlDB
}
