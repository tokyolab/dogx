package migration

import (
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestNewProviderLoadsEmbeddedMigrations(t *testing.T) {
	db, err := sql.Open("pgx", "host=localhost user=test dbname=test sslmode=disable")
	if err != nil {
		t.Fatalf("open test database handle: %v", err)
	}
	defer db.Close()

	provider, err := NewProvider(db)
	if err != nil {
		t.Fatalf("create migration provider: %v", err)
	}

	sources := provider.ListSources()
	if len(sources) != 1 {
		t.Fatalf("unexpected migration count: got %d, want 1", len(sources))
	}
	if sources[0].Version != 1 || sources[0].Path != "00001_init_system.sql" {
		t.Fatalf("unexpected migration source: version=%d path=%s", sources[0].Version, sources[0].Path)
	}
}
