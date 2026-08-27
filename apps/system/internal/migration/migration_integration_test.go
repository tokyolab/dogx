//go:build integration

package migration

import (
	"context"
	"database/sql"
	"strings"
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
	if len(results) != 8 {
		t.Fatalf("unexpected applied migration count: got %d, want 8", len(results))
	}
	if results[0].Source.Version != 1 || results[0].Source.Path != "00001_init_system.sql" {
		t.Fatalf(
			"unexpected applied migration: version=%d path=%s",
			results[0].Source.Version,
			results[0].Source.Path,
		)
	}
	if results[1].Source.Version != 20260824100845 ||
		results[1].Source.Path != "20260824100845_add_login_log.sql" {
		t.Fatalf(
			"unexpected applied migration: version=%d path=%s",
			results[1].Source.Version,
			results[1].Source.Path,
		)
	}
	if results[2].Source.Version != 20260825151501 ||
		results[2].Source.Path != "20260825151501_add_api_authorization.sql" {
		t.Fatalf(
			"unexpected applied migration: version=%d path=%s",
			results[2].Source.Version,
			results[2].Source.Path,
		)
	}
	if results[3].Source.Version != 20260825183427 ||
		results[3].Source.Path != "20260825183427_seed_initial_authorization.sql" {
		t.Fatalf(
			"unexpected applied migration: version=%d path=%s",
			results[3].Source.Version,
			results[3].Source.Path,
		)
	}
	if results[4].Source.Version != 20260826104035 ||
		results[4].Source.Path != "20260826104035_seed_rbac_query_apis.sql" {
		t.Fatalf(
			"unexpected applied migration: version=%d path=%s",
			results[4].Source.Version,
			results[4].Source.Path,
		)
	}
	if results[5].Source.Version != 20260826112413 ||
		results[5].Source.Path != "20260826112413_add_role_management.sql" {
		t.Fatalf(
			"unexpected applied migration: version=%d path=%s",
			results[5].Source.Version,
			results[5].Source.Path,
		)
	}
	if results[6].Source.Version != 20260827131521 ||
		results[6].Source.Path != "20260827131521_drop_foreign_keys.sql" {
		t.Fatalf(
			"unexpected applied migration: version=%d path=%s",
			results[6].Source.Version,
			results[6].Source.Path,
		)
	}
	if results[7].Source.Version != 20260827152932 ||
		results[7].Source.Path != "20260827152932_optimize_system_indexes.sql" {
		t.Fatalf(
			"unexpected applied migration: version=%d path=%s",
			results[7].Source.Version,
			results[7].Source.Path,
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
	if version != 20260827152932 {
		t.Fatalf("unexpected Goose database version: got %d, want 20260827152932", version)
	}

	expectedTables := map[string]string{
		"sys_user":      "系统用户表",
		"sys_role":      "系统角色表",
		"sys_menu":      "系统多应用目录、页面与页面元素表",
		"sys_api":       "系统接口授权资源目录表",
		"casbin_rule":   "Casbin官方GORM Adapter策略持久化表",
		"sys_user_role": "用户角色关联表",
		"sys_role_menu": "角色菜单关联表",
		"sys_login_log": "用户登录审计日志表",
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
		"uk_sys_role_code_active",
		"idx_sys_menu_parent_id",
		"uk_sys_menu_app_route_name_active",
		"uk_sys_menu_app_path_active",
		"idx_sys_menu_app_parent_sort",
		"idx_sys_user_role_role_id",
		"idx_sys_role_menu_menu_id",
		"uk_sys_api_method_path_active",
		"idx_sys_api_service_group",
		"idx_casbin_rule",
		"idx_casbin_rule_ptype_v0",
		"idx_sys_login_log_user_created_at",
		"idx_sys_login_log_created_at",
		"idx_sys_login_log_username_created_at",
	} {
		assertIndexExists(t, ctx, sqlDB, index)
	}
	for _, index := range []string{
		"idx_sys_user_deleted_at",
		"idx_sys_role_deleted_at",
		"idx_sys_menu_deleted_at",
		"idx_sys_menu_permission",
		"idx_sys_api_deleted_at",
	} {
		assertIndexNotExists(t, ctx, sqlDB, index)
	}
	assertIndexDefinitionContains(
		t,
		ctx,
		sqlDB,
		"uk_sys_role_code_active",
		"(code)",
		"where (deleted_at is null)",
	)
	assertIndexDefinitionContains(
		t,
		ctx,
		sqlDB,
		"idx_sys_user_role_role_id",
		"(role_id, user_id)",
	)

	for table, constraints := range map[string][]string{
		"sys_menu": {"ck_sys_menu_app_code_not_blank", "ck_sys_menu_type", "ck_sys_menu_element_permission", "ck_sys_menu_element_route", "ck_sys_menu_status"},
		"sys_api":  {"ck_sys_api_method_upper", "ck_sys_api_path", "ck_sys_api_status"},
		"sys_user": {"ck_sys_user_status"},
		"sys_role": {"ck_sys_role_status", "ck_sys_role_code_format", "ck_sys_role_name_not_blank", "ck_sys_role_sort"},
	} {
		for _, constraint := range constraints {
			assertConstraintExists(t, ctx, sqlDB, table, constraint)
		}
	}
	assertConstraintNotExists(t, ctx, sqlDB, "sys_menu", "uk_sys_menu_id_app_code")
	assertNoForeignKeys(t, ctx, sqlDB)

	var seedCount int
	if err := sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM casbin_rule
		WHERE ptype = 'p'
		  AND v1 = '/role/api/update'
		  AND v2 = 'POST'
	`).Scan(&seedCount); err != nil {
		t.Fatalf("query initial authorization policy: %v", err)
	}
	if seedCount != 1 {
		t.Fatalf("unexpected initial authorization policy count: got %d, want 1", seedCount)
	}
	if err := sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM casbin_rule
		WHERE ptype = 'p'
		  AND (v1, v2) IN (
		      ('/role/list', 'POST'),
		      ('/role/get', 'POST'),
		      ('/role/api/get', 'POST'),
		      ('/role/api/update', 'POST'),
		      ('/api/list', 'POST')
		  )
	`).Scan(&seedCount); err != nil {
		t.Fatalf("query RBAC management authorization policies: %v", err)
	}
	if seedCount != 5 {
		t.Fatalf("unexpected RBAC query policy count: got %d, want 5", seedCount)
	}
	if err := sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM casbin_rule
		WHERE ptype = 'p'
		  AND (v1, v2) IN (
		      ('/role/create', 'POST'),
		      ('/role/update', 'POST'),
		      ('/role/status/update', 'POST'),
		      ('/role/delete', 'POST')
		  )
	`).Scan(&seedCount); err != nil {
		t.Fatalf("query role management authorization policies: %v", err)
	}
	if seedCount != 4 {
		t.Fatalf("unexpected role management policy count: got %d, want 4", seedCount)
	}
	var systemRole bool
	if err := sqlDB.QueryRowContext(ctx, `
		SELECT is_system
		FROM sys_role
		WHERE code = 'super_admin' AND deleted_at IS NULL
	`).Scan(&systemRole); err != nil {
		t.Fatalf("query built-in role marker: %v", err)
	}
	if !systemRole {
		t.Fatal("super_admin role was not marked as a system role")
	}

	downResult, err := provider.Down(ctx)
	if err != nil {
		t.Fatalf("roll back latest migration: %v", err)
	}
	if downResult.Source.Version != 20260827152932 ||
		downResult.Source.Path != "20260827152932_optimize_system_indexes.sql" {
		t.Fatalf(
			"unexpected rolled-back migration: version=%d path=%s",
			downResult.Source.Version,
			downResult.Source.Path,
		)
	}
	for _, index := range []string{
		"idx_sys_user_deleted_at",
		"idx_sys_role_deleted_at",
		"idx_sys_menu_deleted_at",
		"idx_sys_menu_permission",
		"idx_sys_api_deleted_at",
	} {
		assertIndexExists(t, ctx, sqlDB, index)
	}
	assertIndexDefinitionContains(
		t,
		ctx,
		sqlDB,
		"uk_sys_role_code_active",
		"lower((code)::text)",
	)
	assertIndexDefinitionContains(
		t,
		ctx,
		sqlDB,
		"idx_sys_user_role_role_id",
		"(role_id)",
	)

	downResult, err = provider.Down(ctx)
	if err != nil {
		t.Fatalf("roll back foreign key migration: %v", err)
	}
	if downResult.Source.Version != 20260827131521 ||
		downResult.Source.Path != "20260827131521_drop_foreign_keys.sql" {
		t.Fatalf(
			"unexpected rolled-back migration: version=%d path=%s",
			downResult.Source.Version,
			downResult.Source.Path,
		)
	}

	downResult, err = provider.Down(ctx)
	if err != nil {
		t.Fatalf("roll back role management migration: %v", err)
	}
	if downResult.Source.Version != 20260826112413 ||
		downResult.Source.Path != "20260826112413_add_role_management.sql" {
		t.Fatalf(
			"unexpected rolled-back migration: version=%d path=%s",
			downResult.Source.Version,
			downResult.Source.Path,
		)
	}
	assertColumnNotExists(t, ctx, sqlDB, "sys_role", "is_system")
	if err := sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM sys_api
		WHERE (path, method) IN (
		    ('/role/create', 'POST'),
		    ('/role/update', 'POST'),
		    ('/role/status/update', 'POST'),
		    ('/role/delete', 'POST')
		)
	`).Scan(&seedCount); err != nil {
		t.Fatalf("query rolled-back role management API resources: %v", err)
	}
	if seedCount != 0 {
		t.Fatalf("role management API resources remained after rollback: %d", seedCount)
	}

	downResult, err = provider.Down(ctx)
	if err != nil {
		t.Fatalf("roll back RBAC query seed migration: %v", err)
	}
	if downResult.Source.Version != 20260826104035 ||
		downResult.Source.Path != "20260826104035_seed_rbac_query_apis.sql" {
		t.Fatalf(
			"unexpected rolled-back migration: version=%d path=%s",
			downResult.Source.Version,
			downResult.Source.Path,
		)
	}
	assertTableExists(t, ctx, sqlDB, "sys_api")
	assertTableExists(t, ctx, sqlDB, "casbin_rule")
	if err := sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM sys_api
		WHERE (path, method) IN (
		    ('/role/list', 'POST'),
		    ('/role/get', 'POST'),
		    ('/role/api/get', 'POST'),
		    ('/api/list', 'POST')
		)
	`).Scan(&seedCount); err != nil {
		t.Fatalf("query rolled-back RBAC query API resources: %v", err)
	}
	if seedCount != 0 {
		t.Fatalf("RBAC query API resources remained after rollback: %d", seedCount)
	}
	if err := sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM sys_api
		WHERE path = '/role/api/update'
		  AND method = 'POST'
	`).Scan(&seedCount); err != nil {
		t.Fatalf("query retained role API update resource: %v", err)
	}
	if seedCount != 1 {
		t.Fatalf("older role API update resource was removed by latest rollback: %d", seedCount)
	}

	downResult, err = provider.Down(ctx)
	if err != nil {
		t.Fatalf("roll back initial authorization seed migration: %v", err)
	}
	if downResult.Source.Version != 20260825183427 ||
		downResult.Source.Path != "20260825183427_seed_initial_authorization.sql" {
		t.Fatalf(
			"unexpected rolled-back migration: version=%d path=%s",
			downResult.Source.Version,
			downResult.Source.Path,
		)
	}

	downResult, err = provider.Down(ctx)
	if err != nil {
		t.Fatalf("roll back API authorization schema migration: %v", err)
	}
	if downResult.Source.Version != 20260825151501 ||
		downResult.Source.Path != "20260825151501_add_api_authorization.sql" {
		t.Fatalf(
			"unexpected rolled-back migration: version=%d path=%s",
			downResult.Source.Version,
			downResult.Source.Path,
		)
	}
	assertTableNotExists(t, ctx, sqlDB, "sys_api")
	assertTableNotExists(t, ctx, sqlDB, "casbin_rule")
	assertColumnExists(t, ctx, sqlDB, "sys_menu", "permission")
	assertColumnNotExists(t, ctx, sqlDB, "sys_menu", "app_code")

	reapplyResults, err := provider.Up(ctx)
	if err != nil {
		t.Fatalf("reapply latest migration after rollback: %v", err)
	}
	if len(reapplyResults) != 6 ||
		reapplyResults[0].Source.Version != 20260825151501 ||
		reapplyResults[1].Source.Version != 20260825183427 ||
		reapplyResults[2].Source.Version != 20260826104035 ||
		reapplyResults[3].Source.Version != 20260826112413 ||
		reapplyResults[4].Source.Version != 20260827131521 ||
		reapplyResults[5].Source.Version != 20260827152932 {
		t.Fatalf("unexpected reapplied migrations: %+v", reapplyResults)
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
		Code:        "integration_role",
		Name:        "Integration Role",
		Description: "migration integration test",
		Status:      model.RecordStatusEnabled,
	}
	menu := model.Menu{
		AppCode:    model.MenuAppAdminWeb,
		Type:       model.MenuTypePage,
		Name:       "Integration Menu",
		RouteName:  "IntegrationMenu",
		Path:       "/integration",
		Component:  "system/integration/index",
		Permission: "",
		Visible:    true,
		Status:     model.RecordStatusEnabled,
		Remark:     "migration integration test",
	}
	apiResource := model.API{
		ServiceName: "system-api",
		Group:       "integration",
		Name:        "Integration API",
		Path:        "/integration/get",
		Method:      "POST",
		Status:      model.RecordStatusEnabled,
		Remark:      "migration integration test",
	}

	for _, entity := range []struct {
		name  string
		value any
	}{
		{name: "user", value: &user},
		{name: "role", value: &role},
		{name: "menu", value: &menu},
		{name: "api", value: &apiResource},
	} {
		if err := gormDB.WithContext(ctx).Create(entity.value).Error; err != nil {
			t.Fatalf("create %s through current GORM model: %v", entity.name, err)
		}
	}
	if user.ID == 0 || role.ID == 0 || menu.ID == 0 || apiResource.ID == 0 {
		t.Fatalf(
			"identity values were not populated: user=%d role=%d menu=%d api=%d",
			user.ID,
			role.ID,
			menu.ID,
			apiResource.ID,
		)
	}

	element := model.Menu{
		AppCode:    model.MenuAppAdminWeb,
		ParentID:   &menu.ID,
		Type:       model.MenuTypeElement,
		Name:       "Integration Create Button",
		Permission: "system:integration:create",
		Status:     model.RecordStatusEnabled,
		Remark:     "migration integration test",
	}
	if err := gormDB.WithContext(ctx).Create(&element).Error; err != nil {
		t.Fatalf("create page element through current GORM model: %v", err)
	}
	if element.ID == 0 {
		t.Fatal("page element identity value was not populated")
	}

	elementWithoutPermission := model.Menu{
		AppCode:  model.MenuAppAdminWeb,
		ParentID: &menu.ID,
		Type:     model.MenuTypeElement,
		Name:     "Invalid Element Without Permission",
		Status:   model.RecordStatusEnabled,
	}
	if err := gormDB.WithContext(ctx).Create(&elementWithoutPermission).Error; err == nil {
		t.Fatal("create page element without permission succeeded, want check-constraint violation")
	}

	lowercaseMethodAPI := model.API{
		ServiceName: "system-api",
		Group:       "integration",
		Name:        "Invalid Lowercase Method API",
		Path:        "/integration/lowercase-method",
		Method:      "get",
		Status:      model.RecordStatusEnabled,
	}
	if err := gormDB.WithContext(ctx).Create(&lowercaseMethodAPI).Error; err == nil {
		t.Fatal("create API resource with lowercase method succeeded, want check-constraint violation")
	}

	invalidRoleCode := model.Role{
		Code:   "invalid-role-code",
		Name:   "Invalid Role Code",
		Status: model.RecordStatusEnabled,
	}
	if err := gormDB.WithContext(ctx).Create(&invalidRoleCode).Error; err == nil {
		t.Fatal("create role with invalid code succeeded, want check-constraint violation")
	}
	blankRoleName := model.Role{
		Code:   "blank_role_name",
		Name:   "   ",
		Status: model.RecordStatusEnabled,
	}
	if err := gormDB.WithContext(ctx).Create(&blankRoleName).Error; err == nil {
		t.Fatal("create role with blank name succeeded, want check-constraint violation")
	}
	negativeRoleSort := model.Role{
		Code:   "negative_role_sort",
		Name:   "Negative Role Sort",
		Sort:   -1,
		Status: model.RecordStatusEnabled,
	}
	if err := gormDB.WithContext(ctx).Create(&negativeRoleSort).Error; err == nil {
		t.Fatal("create role with negative sort succeeded, want check-constraint violation")
	}

	const insertCasbinPolicy = "INSERT INTO casbin_rule (ptype, v0, v1, v2) VALUES ($1, $2, $3, $4)"
	if _, err := sqlDB.ExecContext(
		ctx,
		insertCasbinPolicy,
		"p",
		"r:1",
		apiResource.Path,
		apiResource.Method,
	); err != nil {
		t.Fatalf("insert Casbin policy through adapter-compatible schema: %v", err)
	}
	if _, err := sqlDB.ExecContext(
		ctx,
		insertCasbinPolicy,
		"p",
		"r:1",
		apiResource.Path,
		apiResource.Method,
	); err == nil {
		t.Fatal("insert duplicate Casbin policy succeeded, want unique-index violation")
	}

	userRole := model.UserRole{UserID: user.ID, RoleID: role.ID}
	roleMenus := []model.RoleMenu{
		{RoleID: role.ID, MenuID: menu.ID},
		{RoleID: role.ID, MenuID: element.ID},
	}
	loginLog := model.LoginLog{
		UserID:    &user.ID,
		Username:  user.Username,
		Success:   true,
		IPAddress: "192.0.2.1",
		UserAgent: "DogX Migration Test",
	}
	if err := gormDB.WithContext(ctx).Create(&userRole).Error; err != nil {
		t.Fatalf("create user-role relation through current GORM model: %v", err)
	}
	if err := gormDB.WithContext(ctx).Create(&roleMenus).Error; err != nil {
		t.Fatalf("create role-menu relations through current GORM model: %v", err)
	}
	if err := gormDB.WithContext(ctx).Create(&loginLog).Error; err != nil {
		t.Fatalf("create login log through current GORM model: %v", err)
	}
	if loginLog.ID == 0 || loginLog.CreatedAt.IsZero() {
		t.Fatalf("login log timestamps or identity were not populated: %+v", loginLog)
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
	if relationCount != 2 {
		t.Fatalf("unexpected role-menu relation count: got %d, want 2", relationCount)
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

func assertTableNotExists(t testing.TB, ctx context.Context, db *sql.DB, table string) {
	t.Helper()

	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists); err != nil {
		t.Fatalf("check table %s: %v", table, err)
	}
	if exists {
		t.Errorf("table %s exists, want it to be absent", table)
	}
}

func assertColumnExists(t testing.TB, ctx context.Context, db *sql.DB, table, column string) {
	t.Helper()
	assertColumnPresence(t, ctx, db, table, column, true)
}

func assertColumnNotExists(t testing.TB, ctx context.Context, db *sql.DB, table, column string) {
	t.Helper()
	assertColumnPresence(t, ctx, db, table, column, false)
}

func assertColumnPresence(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	table string,
	column string,
	want bool,
) {
	t.Helper()

	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = $1
			  AND column_name = $2
		)
	`, table, column).Scan(&exists); err != nil {
		t.Fatalf("check column %s.%s: %v", table, column, err)
	}
	if exists != want {
		t.Errorf("column %s.%s existence: got %t, want %t", table, column, exists, want)
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

func assertIndexNotExists(t testing.TB, ctx context.Context, db *sql.DB, index string) {
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
		t.Fatalf("check absent index %s: %v", index, err)
	}
	if exists {
		t.Errorf("index %s exists, want it to be absent", index)
	}
}

func assertIndexDefinitionContains(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	index string,
	wantFragments ...string,
) {
	t.Helper()

	var definition string
	if err := db.QueryRowContext(
		ctx,
		"SELECT pg_get_indexdef(to_regclass($1))",
		index,
	).Scan(&definition); err != nil {
		t.Fatalf("read index definition %s: %v", index, err)
	}
	definition = strings.ToLower(definition)
	for _, fragment := range wantFragments {
		if !strings.Contains(definition, strings.ToLower(fragment)) {
			t.Errorf("index %s definition %q does not contain %q", index, definition, fragment)
		}
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

func assertConstraintNotExists(t testing.TB, ctx context.Context, db *sql.DB, table, constraint string) {
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
		t.Fatalf("check absent constraint %s on %s: %v", constraint, table, err)
	}
	if exists {
		t.Errorf("constraint %s unexpectedly exists on table %s", constraint, table)
	}
}

func assertNoForeignKeys(t testing.TB, ctx context.Context, db *sql.DB) {
	t.Helper()

	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pg_constraint
		WHERE contype = 'f'
		  AND connamespace = current_schema()::regnamespace
	`).Scan(&count); err != nil {
		t.Fatalf("count foreign key constraints: %v", err)
	}
	if count != 0 {
		t.Errorf("current schema has %d foreign key constraints, want 0", count)
	}
}
