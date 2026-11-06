//go:build integration

package repository

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestTaskManagementPaginationPreservesNullableFactsWithoutData(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, ordinary := createResultTask(t, db, job)
	previous := "previous attempt failed"
	if err := db.Model(&model.JobTask{}).Where("id = ?", ordinary.ID).Update("result_summary", previous).Error; err != nil {
		t.Fatal(err)
	}
	waiting := model.JobTask{InstanceID: instance.ID, TaskType: model.TaskTypeShard, Status: model.TaskWaiting}
	if err := db.Create(&waiting).Error; err != nil {
		t.Fatal(err)
	}
	identity, address, summary, reason := uuid.New(), "127.0.0.1:9090", "", model.EndReasonSuccess
	started := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	finished := started.Add(time.Second)
	completed := model.JobTask{InstanceID: instance.ID, TaskType: model.TaskTypeShard, Status: model.TaskSucceeded,
		AttemptNo: 3, UsedAttempts: 2, AttemptRegisteredAt: &started, ExecutorInstanceID: &identity, ExecutorAddress: &address,
		ResultData: model.TaskResultData(`"` + strings.Repeat("payload", 4096) + `"`), ResultSummary: &summary,
		EndReason: &reason, StartedAt: &started, FinishedAt: &finished, CreatedAt: started, UpdatedAt: finished}
	if err := db.Create(&completed).Error; err != nil {
		t.Fatal(err)
	}
	other, _ := createResultTask(t, db, job)
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	items, total, err := repo.ListByInstance(context.Background(), instance.ID, 1, 2)
	if err != nil || total != 3 || len(items) != 2 || items[0].ID != ordinary.ID || items[1].ID != waiting.ID {
		t.Fatalf("first stable page: items=%+v total=%d err=%v", items, total, err)
	}
	if counter.count != 2 || strings.Contains(strings.Join(counter.statements, "\n"), "result_data") ||
		!strings.Contains(counter.statements[1], "ORDER BY id ASC") || strings.Contains(counter.statements[1], "SELECT *") {
		t.Fatalf("Task list added content reads or per-row queries: %v", counter.statements)
	}
	if items[0].Status != model.TaskRunning || items[0].ResultSummary == nil || *items[0].ResultSummary != previous {
		t.Fatal("the running Task's previous-attempt summary was hidden or treated as completion")
	}
	missing := items[1]
	if missing.AttemptRegisteredAt != nil || missing.NextDispatchAt != nil || missing.ExecutorInstanceID != nil ||
		missing.ExecutorAddress != nil || missing.ResultSummary != nil || missing.EndReason != nil || missing.StartedAt != nil || missing.FinishedAt != nil {
		t.Fatalf("unstarted Task acquired invented execution facts: %+v", missing)
	}
	items, total, err = repo.ListByInstance(context.Background(), instance.ID, 2, 2)
	saved := readRepositoryTask(t, db, completed.ID)
	expected := TaskSummary{ID: saved.ID, InstanceID: saved.InstanceID, TaskType: saved.TaskType, Status: saved.Status,
		AttemptNo: saved.AttemptNo, UsedAttempts: saved.UsedAttempts, AttemptRegisteredAt: saved.AttemptRegisteredAt,
		ExecutorInstanceID: saved.ExecutorInstanceID, ExecutorAddress: saved.ExecutorAddress,
		ResultSummary: saved.ResultSummary, EndReason: saved.EndReason, StartedAt: saved.StartedAt,
		FinishedAt: saved.FinishedAt, CreatedAt: saved.CreatedAt, UpdatedAt: saved.UpdatedAt}
	if err != nil || total != 3 || len(items) != 1 || !reflect.DeepEqual(items[0], expected) {
		t.Fatalf("second page lost nullable or execution facts: items=%+v total=%d err=%v", items, total, err)
	}
	items, total, err = repo.ListByInstance(context.Background(), instance.ID, 3, 2)
	if err != nil || total != 3 || items == nil || len(items) != 0 {
		t.Fatalf("empty page: items=%+v total=%d err=%v", items, total, err)
	}
	if err := db.Where("instance_id = ?", other.ID).Delete(&model.JobTask{}).Error; err != nil {
		t.Fatal(err)
	}
	items, total, err = repo.ListByInstance(context.Background(), other.ID, 1, 200)
	if err != nil || total != 0 || items == nil || len(items) != 0 {
		t.Fatalf("existing parent without Tasks: items=%+v total=%d err=%v", items, total, err)
	}
}

func TestExecutionQueriesUseCompletionAndPaginationIndexes(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	// A history-heavy dataset makes a missing index visible without forcing
	// planner switches or asserting machine-dependent query durations.
	if err := db.Exec(`INSERT INTO job_instance
		(job_id, job_name, executor_group_id, trigger_type, scheduled_at, params_overridden, execution_config, status)
		SELECT n, 'query history', 1, 1, CURRENT_TIMESTAMP, FALSE,
		jsonb_build_object('schema_version', 1, 'execution_mode', 1), 3
		FROM generate_series(1, 20000) n`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO job_task (instance_id, task_type, status, attempt_no, used_attempts, result_data)
		SELECT id, 1, 4, 1, 1, to_jsonb(repeat('x', 1024)) FROM job_instance`).Error; err != nil {
		t.Fatal(err)
	}
	// Hundreds of terminal shards require the all-status management index;
	// neither the singleton nor unfinished-only index can serve that list.
	if err := db.Exec(`INSERT INTO job_task (instance_id, task_type, status, attempt_no, used_attempts)
		SELECT 10000, 3, 4, 1, 1 FROM generate_series(1, 600)`).Error; err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"job_instance", "job_task"} {
		if err := db.Exec("ANALYZE " + table).Error; err != nil {
			t.Fatal(err)
		}
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	capture := &executionQueryCapture{queryCounter: counter}
	instances, err := NewJobInstanceRepository(db.Session(&gorm.Session{Logger: capture}))
	if err != nil {
		t.Fatal(err)
	}
	completion, err := instances.FindCompletion(context.Background(), 10000)
	if err != nil || completion == nil || completion.TaskID == nil || counter.count != 1 || len(capture.queries) != 1 {
		t.Fatalf("completion lookup: completion=%+v count=%d err=%v", completion, counter.count, err)
	}
	plan := explainExecutionQuery(t, db, capture.queries[0])
	if !strings.Contains(plan, "job_instance_pkey") || !strings.Contains(plan, "uq_job_task_singleton") || strings.Contains(plan, "Seq Scan") {
		t.Fatalf("completion did not use parent and singleton indexes:\n%s", plan)
	}
	capture.reset()
	tasks, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: capture}))
	if err != nil {
		t.Fatal(err)
	}
	items, total, err := tasks.ListByInstance(context.Background(), 10000, 2, 200)
	if err != nil || total != 601 || len(items) != 200 || counter.count != 2 || len(capture.queries) != 2 {
		t.Fatalf("indexed Task page: total=%d items=%d count=%d err=%v", total, len(items), counter.count, err)
	}
	for index, query := range capture.queries {
		plan := explainExecutionQuery(t, db, query)
		if !strings.Contains(plan, "idx_job_task_instance") || strings.Contains(plan, "Seq Scan") || (index == 1 && strings.Contains(plan, "Sort  (")) {
			t.Fatalf("Task count/page did not use its management index:\n%s", plan)
		}
	}
}

type executionQuery struct {
	sql  string
	vars []any
}

// executionQueryCapture retains repository SQL and bindings before GORM formats
// its log message. It is used serially and leaves the logging inputs intact.
type executionQueryCapture struct {
	*queryCounter
	queries []executionQuery
}

func (c *executionQueryCapture) ParamsFilter(_ context.Context, statement string, vars ...any) (string, []any) {
	c.queries = append(c.queries, executionQuery{sql: statement, vars: append([]any(nil), vars...)})
	return statement, vars
}

func (c *executionQueryCapture) reset() {
	c.count, c.statements, c.queries = 0, nil, nil
}

func explainExecutionQuery(t *testing.T, db *gorm.DB, query executionQuery) string {
	t.Helper()
	// GORM's interpolated log SQL omits timestamp offsets. Execute the original
	// placeholders and arguments through this session's connection pool so that
	// time bounds and any transaction-local state match the repository call.
	rows, err := db.Statement.ConnPool.QueryContext(context.Background(), "EXPLAIN (ANALYZE, BUFFERS) "+query.sql, query.vars...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
	}()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(lines, "\n")
	t.Logf("execution query plan:\n%s", plan)
	return plan
}
