//go:build integration

package migration

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/system/internal/testutil"
)

func assertSystemMenuSeed(t testing.TB, ctx context.Context, db *sql.DB) {
	t.Helper()
	type menuSeed struct {
		name, parent, path, component string
		kind, sort                    int
	}
	want := map[string]menuSeed{
		"SystemManagement": {"系统管理", "", "/system", "", 1, 10},
		"MenuManagement":   {"菜单管理", "SystemManagement", "/system/menu", "system/menu/index", 2, 10},
		"UserManagement":   {"用户管理", "SystemManagement", "/system/user", "system/user/index", 2, 20},
		"RoleManagement":   {"角色管理", "SystemManagement", "/system/role", "system/role/index", 2, 30},
	}
	rows, err := db.QueryContext(ctx, `
		SELECT m.route_name, m.name, COALESCE(p.route_name, ''), m.path, m.component,
		       m.menu_type, m.sort, m.app_code, m.visible, m.status, m.permission, m.keep_alive, m.external
		FROM sys_menu m LEFT JOIN sys_menu p ON p.id = m.parent_id
		WHERE m.deleted_at IS NULL
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close system menu seed rows: %v", err)
		}
	}()
	for rows.Next() {
		var route, appCode, permission string
		var visible, keepAlive, external bool
		var status int
		var got menuSeed
		if err := rows.Scan(&route, &got.name, &got.parent, &got.path, &got.component,
			&got.kind, &got.sort, &appCode, &visible, &status, &permission, &keepAlive, &external); err != nil {
			t.Fatal(err)
		}
		expected, exists := want[route]
		if !exists || got != expected || appCode != "admin_web" || !visible || status != 1 || permission != "" || keepAlive || external {
			t.Fatalf("unexpected seed %s: %+v app=%s visible=%t status=%d permission=%q cache=%t external=%t", route, got, appCode, visible, status, permission, keepAlive, external)
		}
		delete(want, route)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(want) != 0 {
		t.Fatalf("missing system menus: %+v", want)
	}
}

func TestSystemMenuSeedDoesNotOverwriteConflictingMenus(t *testing.T) {
	_, db := testutil.OpenPostgres(t)
	provider := newTestProvider(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := provider.UpTo(ctx, 20260916160000); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO sys_menu (app_code, menu_type, name, route_name, path)
		VALUES ('admin_web', 2, 'Existing', 'RoleManagement', '/custom-role')
	`); err != nil {
		t.Fatal(err)
	}
	_, err := provider.UpTo(ctx, 20260916160107)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("expected a uniqueness conflict: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sys_menu").Scan(&count); err != nil || count != 1 {
		t.Fatalf("failed seed inserted partial menu data: count=%d err=%v", count, err)
	}
	var path string
	if err := db.QueryRowContext(ctx, "SELECT path FROM sys_menu WHERE route_name = 'RoleManagement'").Scan(&path); err != nil || path != "/custom-role" {
		t.Fatalf("existing menu changed: path=%q err=%v", path, err)
	}
	if version, err := provider.GetDBVersion(ctx); err != nil || version != 20260916160000 {
		t.Fatalf("failed seed advanced migration version: %d %v", version, err)
	}
}

func TestSystemMenuSeedRollbackProtectsChildrenAndCleansGrants(t *testing.T) {
	_, db := testutil.OpenPostgres(t)
	provider := newTestProvider(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Stop at the migration under test so Down still targets the menu seed
	// when later migrations are added.
	if _, err := provider.UpTo(ctx, 20260916160107); err != nil {
		t.Fatal(err)
	}
	var rootID, childID int64
	if err := db.QueryRowContext(ctx, "SELECT id FROM sys_menu WHERE route_name = 'SystemManagement'").Scan(&rootID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO sys_menu (app_code, parent_id, menu_type, name, route_name, path)
		VALUES ('admin_web', $1, 2, 'Custom', 'Custom', '/custom') RETURNING id
	`, rootID).Scan(&childID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO sys_role_menu (role_id, menu_id) VALUES (999, $1)", rootID); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err == nil {
		t.Fatal("seed rollback would orphan the custom child")
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sys_role_menu WHERE menu_id = $1", rootID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("failed rollback removed grants: %d %v", count, err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM sys_menu WHERE id = $1", childID); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sys_role_menu WHERE menu_id = $1", rootID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("seed rollback left grants: %d %v", count, err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sys_menu").Scan(&count); err != nil || count != 0 {
		t.Fatalf("seed rollback left menus: %d %v", count, err)
	}
}
