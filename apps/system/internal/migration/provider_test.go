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
	if len(sources) != 16 {
		t.Fatalf("unexpected migration count: got %d, want 16", len(sources))
	}
	if sources[15].Version != 20260920120000 || sources[15].Path != "20260920120000_add_department_management.sql" {
		t.Fatalf("unexpected department migration: %+v", sources[15])
	}
	if sources[14].Version != 20260918160000 || sources[14].Path != "20260918160000_add_role_menu_apis.sql" {
		t.Fatalf("unexpected role menu migration: %+v", sources[14])
	}
	if sources[13].Version != 20260916160107 || sources[13].Path != "20260916160107_seed_system_menus.sql" {
		t.Fatalf("unexpected system menu seed migration: %+v", sources[13])
	}
	if sources[12].Version != 20260916160000 || sources[12].Path != "20260916160000_add_menu_management.sql" {
		t.Fatalf("unexpected menu management migration: %+v", sources[12])
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
	if sources[10].Version != 20260907103000 || sources[10].Path != "20260907103000_add_user_management.sql" {
		t.Fatalf("unexpected user management migration: %+v", sources[10])
	}
	if sources[11].Version != 20260916071004 || sources[11].Path != "20260916071004_constrain_username_format.sql" {
		t.Fatalf("unexpected username format migration: %+v", sources[11])
	}
}
