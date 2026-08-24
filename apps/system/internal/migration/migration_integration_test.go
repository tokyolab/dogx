//go:build integration

package migration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/testutil"
)

func TestMigrationsApplyToEmptyPostgreSQL(t *testing.T) {
	_, sqlDB := testutil.OpenPostgres(t)
	provider := newTestProvider(t, sqlDB)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := provider.Up(ctx)
	if err != nil {
		t.Fatalf("apply migrations to empty PostgreSQL database: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("unexpected applied migration count: got %d, want 1", len(results))
	}
	if results[0].Source.Version != 1 || results[0].Source.Path != "00001_init_system.sql" {
		t.Fatalf(
			"unexpected applied migration: version=%d path=%s",
			results[0].Source.Version,
			results[0].Source.Path,
		)
	}

	secondResults, err := provider.Up(ctx)
	if err != nil {
		t.Fatalf("apply migrations for the second time: %v", err)
	}
	if len(secondResults) != 0 {
		t.Fatalf("second migration run applied %d migrations, want 0", len(secondResults))
	}

	version, err := provider.GetDBVersion(ctx)
	if err != nil {
		t.Fatalf("read Goose database version: %v", err)
	}
	if version != 1 {
		t.Fatalf("unexpected Goose database version: got %d, want 1", version)
	}

	expectedTables := map[string]string{
		"sys_user":      "系统用户表",
		"sys_role":      "系统角色表",
		"sys_menu":      "系统菜单与权限标识表",
		"sys_user_role": "用户角色关联表",
		"sys_role_menu": "角色菜单关联表",
	}
	for table, comment := range expectedTables {
		assertTableComment(t, ctx, sqlDB, table, comment)
		assertAllColumnsHaveComments(t, ctx, sqlDB, table)
	}
	assertTableExists(t, ctx, sqlDB, "goose_db_version")

	for _, index := range []string{
		"uk_sys_user_username_active",
		"uk_sys_user_email_active",
		"uk_sys_user_phone_active",
		"idx_sys_user_deleted_at",
		"uk_sys_role_code_active",
		"idx_sys_role_deleted_at",
		"idx_sys_menu_parent_id",
		"idx_sys_menu_deleted_at",
		"idx_sys_menu_permission",
		"idx_sys_user_role_role_id",
		"idx_sys_role_menu_menu_id",
	} {
		assertIndexExists(t, ctx, sqlDB, index)
	}

	for table, constraints := range map[string][]string{
		"sys_menu":      {"ck_sys_menu_type", "ck_sys_menu_status", "fk_sys_menu_parent"},
		"sys_user":      {"ck_sys_user_status"},
		"sys_role":      {"ck_sys_role_status"},
		"sys_user_role": {"fk_sys_user_role_user", "fk_sys_user_role_role"},
		"sys_role_menu": {"fk_sys_role_menu_role", "fk_sys_role_menu_menu"},
	} {
		for _, constraint := range constraints {
			assertConstraintExists(t, ctx, sqlDB, table, constraint)
		}
	}
}

func TestMigratedSchemaSupportsCurrentGORMModels(t *testing.T) {
	gormDB, sqlDB := testutil.OpenPostgres(t)
	provider := newTestProvider(t, sqlDB)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	email := "integration@example.com"
	phone := "13800000000"
	user := model.User{
		Username:     "integration-user",
		PasswordHash: "hashed-password",
		Nickname:     "Integration User",
		Email:        &email,
		Phone:        &phone,
		Status:       model.RecordStatusEnabled,
		Remark:       "migration integration test",
	}
	role := model.Role{
		Code:        "integration-role",
		Name:        "Integration Role",
		Description: "migration integration test",
		Status:      model.RecordStatusEnabled,
	}
	menu := model.Menu{
		Type:       model.MenuTypePage,
		Name:       "Integration Menu",
		RouteName:  "IntegrationMenu",
		Path:       "/integration",
		Component:  "system/integration/index",
		Permission: "system:integration:list",
		Visible:    true,
		Status:     model.RecordStatusEnabled,
		Remark:     "migration integration test",
	}

	for _, entity := range []struct {
		name  string
		value any
	}{
		{name: "user", value: &user},
		{name: "role", value: &role},
		{name: "menu", value: &menu},
	} {
		if err := gormDB.WithContext(ctx).Create(entity.value).Error; err != nil {
			t.Fatalf("create %s through current GORM model: %v", entity.name, err)
		}
	}
	if user.ID == 0 || role.ID == 0 || menu.ID == 0 {
		t.Fatalf("identity values were not populated: user=%d role=%d menu=%d", user.ID, role.ID, menu.ID)
	}

	userRole := model.UserRole{UserID: user.ID, RoleID: role.ID}
	roleMenu := model.RoleMenu{RoleID: role.ID, MenuID: menu.ID}
	if err := gormDB.WithContext(ctx).Create(&userRole).Error; err != nil {
		t.Fatalf("create user-role relation through current GORM model: %v", err)
	}
	if err := gormDB.WithContext(ctx).Create(&roleMenu).Error; err != nil {
		t.Fatalf("create role-menu relation through current GORM model: %v", err)
	}

	var relationCount int64
	if err := gormDB.WithContext(ctx).Model(&model.UserRole{}).Count(&relationCount).Error; err != nil {
		t.Fatalf("query user-role relation through current GORM model: %v", err)
	}
	if relationCount != 1 {
		t.Fatalf("unexpected user-role relation count: got %d, want 1", relationCount)
	}
	if err := gormDB.WithContext(ctx).Model(&model.RoleMenu{}).Count(&relationCount).Error; err != nil {
		t.Fatalf("query role-menu relation through current GORM model: %v", err)
	}
	if relationCount != 1 {
		t.Fatalf("unexpected role-menu relation count: got %d, want 1", relationCount)
	}
}

func newTestProvider(t testing.TB, db *sql.DB) *goose.Provider {
	t.Helper()

	provider, err := NewProvider(db)
	if err != nil {
		t.Fatalf("create migration provider: %v", err)
	}
	return provider
}

func assertTableExists(t testing.TB, ctx context.Context, db *sql.DB, table string) {
	t.Helper()

	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists); err != nil {
		t.Fatalf("check table %s: %v", table, err)
	}
	if !exists {
		t.Errorf("table %s does not exist", table)
	}
}

func assertTableComment(t testing.TB, ctx context.Context, db *sql.DB, table, want string) {
	t.Helper()
	assertTableExists(t, ctx, db, table)

	var comment sql.NullString
	if err := db.QueryRowContext(
		ctx,
		"SELECT obj_description(to_regclass($1), 'pg_class')",
		table,
	).Scan(&comment); err != nil {
		t.Fatalf("read table comment for %s: %v", table, err)
	}
	if !comment.Valid || comment.String != want {
		t.Errorf("unexpected table comment for %s: got %q, want %q", table, comment.String, want)
	}
}

func assertAllColumnsHaveComments(t testing.TB, ctx context.Context, db *sql.DB, table string) {
	t.Helper()

	var uncommented int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pg_attribute
		WHERE attrelid = to_regclass($1)
		  AND attnum > 0
		  AND NOT attisdropped
		  AND col_description(attrelid, attnum) IS NULL
	`, table).Scan(&uncommented); err != nil {
		t.Fatalf("check column comments for %s: %v", table, err)
	}
	if uncommented != 0 {
		t.Errorf("table %s has %d columns without comments", table, uncommented)
	}
}

func assertIndexExists(t testing.TB, ctx context.Context, db *sql.DB, index string) {
	t.Helper()

	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_indexes
			WHERE schemaname = current_schema()
			  AND indexname = $1
		)
	`, index).Scan(&exists); err != nil {
		t.Fatalf("check index %s: %v", index, err)
	}
	if !exists {
		t.Errorf("index %s does not exist", index)
	}
}

func assertConstraintExists(t testing.TB, ctx context.Context, db *sql.DB, table, constraint string) {
	t.Helper()

	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_constraint
			WHERE conrelid = to_regclass($1)
			  AND conname = $2
		)
	`, table, constraint).Scan(&exists); err != nil {
		t.Fatalf("check constraint %s on %s: %v", constraint, table, err)
	}
	if !exists {
		t.Errorf("constraint %s does not exist on table %s", constraint, table)
	}
}
