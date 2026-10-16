//go:build integration

package migration_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/tokyolab/dogx/apps/job/internal/migration"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
)

func TestJobMigrationStructureAndRoundTrip(t *testing.T) {
	_, sqlDB := testutil.OpenPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	provider, err := migration.NewProvider(sqlDB)
	if err != nil {
		t.Fatal(err)
	}
	results, err := provider.Up(ctx)
	if err != nil || len(results) != 1 {
		t.Fatalf("Up results=%v error=%v", results, err)
	}
	if results, err := provider.Up(ctx); err != nil || len(results) != 0 {
		t.Fatalf("second Up results=%v error=%v", results, err)
	}

	// Every declared field is checked against the deployed schema, including
	// nullability and capacities shared by current values and historical snapshots.
	columns := map[string]map[string]string{
		"job_executor_group": {"id": "bigint!", "name": "character varying(128)!", "created_at": "timestamp with time zone!", "updated_at": "timestamp with time zone!", "deleted_at": "timestamp with time zone"},
		"job_definition":     {"id": "bigint!", "name": "character varying(128)!", "remark": "character varying(500)!", "executor_group_id": "bigint!", "processor_name": "character varying(128)!", "execution_mode": "smallint!", "post_enabled": "boolean!", "params": "text!", "cron_expr": "character varying(128)!", "status": "smallint!", "misfire_policy": "smallint!", "block_policy": "smallint!", "max_retries": "integer!", "retry_interval_seconds": "integer!", "timeout_seconds": "integer!", "next_trigger_at": "timestamp with time zone", "created_at": "timestamp with time zone!", "updated_at": "timestamp with time zone!", "deleted_at": "timestamp with time zone"},
		"job_instance":       {"id": "bigint!", "job_id": "bigint!", "job_name": "character varying(128)!", "executor_group_id": "bigint!", "trigger_type": "smallint!", "scheduled_at": "timestamp with time zone", "request_id": "uuid", "params_overridden": "boolean!", "triggered_by": "bigint", "execution_config": "jsonb!", "status": "smallint!", "started_at": "timestamp with time zone", "deadline_at": "timestamp with time zone", "finished_at": "timestamp with time zone", "end_reason": "character varying(64)", "result_summary": "text", "created_at": "timestamp with time zone!", "updated_at": "timestamp with time zone!"},
	}
	for table, expected := range columns {
		var comment string
		if err := sqlDB.QueryRowContext(ctx, "SELECT obj_description($1::regclass)", table).Scan(&comment); err != nil || comment == "" {
			t.Fatalf("table %s comment=%q error=%v", table, comment, err)
		}
		rows, err := sqlDB.QueryContext(ctx, `SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull, COALESCE(col_description(a.attrelid, a.attnum), '') FROM pg_attribute a WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped`, table)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			var name, typ, columnComment string
			var notNull bool
			if err := rows.Scan(&name, &typ, &notNull, &columnComment); err != nil {
				t.Fatal(err)
			}
			if notNull {
				typ += "!"
			}
			if want, ok := expected[name]; !ok || want != typ || columnComment == "" {
				t.Errorf("%s.%s type=%q comment=%q, want %q", table, name, typ, columnComment, want)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if count != len(expected) {
			t.Errorf("%s column count=%d want=%d", table, count, len(expected))
		}
	}
	indexes := map[string]string{
		"uq_job_instance_auto_identity":   "UNIQUE INDEX|job_id, scheduled_at|scheduled_at IS NOT NULL",
		"uq_job_instance_manual_identity": "UNIQUE INDEX|job_id, request_id|request_id IS NOT NULL",
		"idx_job_definition_due":          "next_trigger_at, id|deleted_at IS NULL|status = 1|next_trigger_at IS NOT NULL",
		"idx_job_instance_active_job":     "job_id|status = ANY",
		"idx_job_instance_waiting":        "id|status = 1",
		"idx_job_definition_group_active": "executor_group_id, id DESC|deleted_at IS NULL",
	}
	rows, err := sqlDB.QueryContext(ctx, `SELECT c.relname, pg_get_indexdef(i.indexrelid) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid IN ('job_executor_group'::regclass, 'job_definition'::regclass, 'job_instance'::regclass) AND NOT i.indisprimary`)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			t.Fatal(err)
		}
		parts, ok := indexes[name]
		if !ok {
			t.Errorf("unexpected index %s", name)
		}
		for _, part := range strings.Split(parts, "|") {
			if !strings.Contains(definition, part) {
				t.Errorf("index %s missing %q: %s", name, part, definition)
			}
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if count != 6 {
		t.Fatalf("non-primary indexes=%d want=6", count)
	}
	var businessTables int
	if err := sqlDB.QueryRowContext(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname = current_schema() AND tablename <> 'job_goose_db_version'").Scan(&businessTables); err != nil || businessTables != 3 {
		t.Fatalf("business tables=%d want=3 error=%v", businessTables, err)
	}
	var foreignKeys, checks int
	if err := sqlDB.QueryRowContext(ctx, `SELECT count(*) FILTER (WHERE contype = 'f'), count(*) FILTER (WHERE contype = 'c') FROM pg_constraint WHERE conrelid IN ('job_executor_group'::regclass, 'job_definition'::regclass, 'job_instance'::regclass)`).Scan(&foreignKeys, &checks); err != nil {
		t.Fatal(err)
	}
	if foreignKeys != 0 || checks != 1 {
		t.Fatalf("foreign keys=%d checks=%d", foreignKeys, checks)
	}
	var groups int
	if err := sqlDB.QueryRowContext(ctx, "SELECT count(*) FROM job_executor_group").Scan(&groups); err != nil || groups != 0 {
		t.Fatalf("implicit group seeds=%d err=%v", groups, err)
	}
	if _, err := provider.Down(ctx); err != nil {
		t.Fatal(err)
	}
	for table := range columns {
		var missing bool
		if err := sqlDB.QueryRowContext(ctx, "SELECT to_regclass($1) IS NULL", table).Scan(&missing); err != nil || !missing {
			t.Fatalf("Down left %s: missing=%v error=%v", table, missing, err)
		}
	}
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("Up after Down: %v", err)
	}
}

func TestJobAndSystemMigrationsUseIndependentLedgers(t *testing.T) {
	_, sqlDB := testutil.OpenPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	// Load the actual System migration sources without importing its private
	// data layer; both providers operate sequentially on this isolated schema.
	systemFiles := os.DirFS(filepath.Join("..", "..", "..", "system", "internal", "migration", "migrations"))
	system, err := goose.NewProvider(goose.DialectPostgres, sqlDB, systemFiles)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := system.Up(ctx); err != nil {
		t.Fatal(err)
	}
	systemVersion, err := system.GetDBVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job, err := migration.NewProvider(sqlDB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := job.Up(ctx); err != nil {
		t.Fatal(err)
	}
	jobVersion, err := job.GetDBVersion(ctx)
	if err != nil || jobVersion != 20261016090000 {
		t.Fatalf("Job version=%d err=%v", jobVersion, err)
	}
	if _, err := job.Down(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := system.GetDBVersion(ctx); err != nil || got != systemVersion {
		t.Fatalf("Job rollback changed System ledger: got=%d want=%d err=%v", got, systemVersion, err)
	}
	if _, err := job.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := system.Down(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := job.GetDBVersion(ctx); err != nil || got != jobVersion {
		t.Fatalf("System rollback changed Job ledger: got=%d want=%d err=%v", got, jobVersion, err)
	}
	var present bool
	if err := sqlDB.QueryRowContext(ctx, "SELECT to_regclass('job_instance') IS NOT NULL AND to_regclass('sys_user') IS NOT NULL").Scan(&present); err != nil || !present {
		t.Fatalf("independent domain tables missing: %v", err)
	}
}
