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
	if len(sources) != 10 {
		t.Fatalf("unexpected migration count: got %d, want 10", len(sources))
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
	if sources[5].Version != 20260826112413 || sources[5].Path != "20260826112413_add_role_management.sql" {
		t.Fatalf("unexpected migration source: version=%d path=%s", sources[5].Version, sources[5].Path)
	}
	if sources[6].Version != 20260827131521 || sources[6].Path != "20260827131521_drop_foreign_keys.sql" {
		t.Fatalf("unexpected migration source: version=%d path=%s", sources[6].Version, sources[6].Path)
	}
	if sources[7].Version != 20260827152932 || sources[7].Path != "20260827152932_optimize_system_indexes.sql" {
		t.Fatalf("unexpected migration source: version=%d path=%s", sources[7].Version, sources[7].Path)
	}
	if sources[8].Version != 20260828182507 || sources[8].Path != "20260828182507_remove_super_admin_policies.sql" {
		t.Fatalf("unexpected migration source: version=%d path=%s", sources[8].Version, sources[8].Path)
	}
	if sources[9].Version != 20260831100816 || sources[9].Path != "20260831100816_protect_super_admin_role_code.sql" {
		t.Fatalf("unexpected migration source: version=%d path=%s", sources[9].Version, sources[9].Path)
	}
}
