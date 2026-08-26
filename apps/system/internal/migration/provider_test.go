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
	if len(sources) != 5 {
		t.Fatalf("unexpected migration count: got %d, want 5", len(sources))
	}
	if sources[0].Version != 1 || sources[0].Path != "00001_init_system.sql" {
		t.Fatalf("unexpected migration source: version=%d path=%s", sources[0].Version, sources[0].Path)
	}
	if sources[1].Version != 20260824100845 || sources[1].Path != "20260824100845_add_login_log.sql" {
		t.Fatalf("unexpected migration source: version=%d path=%s", sources[1].Version, sources[1].Path)
	}
	if sources[2].Version != 20260825151501 || sources[2].Path != "20260825151501_add_api_authorization.sql" {
		t.Fatalf("unexpected migration source: version=%d path=%s", sources[2].Version, sources[2].Path)
	}
	if sources[3].Version != 20260825183427 || sources[3].Path != "20260825183427_seed_initial_authorization.sql" {
		t.Fatalf("unexpected migration source: version=%d path=%s", sources[3].Version, sources[3].Path)
	}
	if sources[4].Version != 20260826104035 || sources[4].Path != "20260826104035_seed_rbac_query_apis.sql" {
		t.Fatalf("unexpected migration source: version=%d path=%s", sources[4].Version, sources[4].Path)
	}
}
