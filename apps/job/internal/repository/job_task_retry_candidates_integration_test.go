//go:build integration

package repository

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestRetryCandidatesUseFixedTimeAndOriginalAttemptCursor(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	scanNow := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	due := scanNow.Add(-time.Minute)
	_, first := createRetryRepositoryTask(t, db, job, due, scanNow)
	_, second := createRetryRepositoryTask(t, db, job, due, scanNow)
	_, boundary := createRetryRepositoryTask(t, db, job, scanNow, scanNow)
	_, later := createRetryRepositoryTask(t, db, job, scanNow.Add(time.Second), scanNow)
	if err := db.Model(&model.JobTask{}).Where("id = ?", first.ID).Update("attempt_no", 7).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobTask{}).Where("id = ?", second.ID).Update("task_type", model.TaskTypeShard).Error; err != nil {
		t.Fatal(err)
	}
	// Discovery depends on the retained Instance, not the current Job or the
	// readability of its frozen business configuration.
	if err := db.Delete(job).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobInstance{}).Where("job_id = ?", job.ID).
		Update("execution_config", gorm.Expr("'null'::jsonb")).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	page, err := repo.ListRetryCandidates(context.Background(), scanNow, nil, 1)
	if err != nil || len(page) != 1 || page[0].TaskID != first.ID || page[0].AttemptNo != 7 || !page[0].NextDispatchAt.Equal(due) {
		t.Fatalf("first retry page: %+v err=%v", page, err)
	}
	if counter.count != 1 || strings.Contains(counter.statements[0], "(t.next_dispatch_at, t.id) >") {
		t.Fatalf("initial page added a cursor or extra reads: %v", counter.statements)
	}
	for _, forbidden := range []string{"task_type", "execution_config", "result_data", "result_summary", "executor_address", "job_definition", "FOR UPDATE", "OFFSET"} {
		if strings.Contains(counter.statements[0], forbidden) {
			t.Fatalf("retry discovery added %s: %s", forbidden, counter.statements[0])
		}
	}
	cursor := &RetryCursor{NextDispatchAt: page[0].NextDispatchAt, TaskID: page[0].TaskID}
	// The original position stays valid after a handed-off row leaves retry state.
	if err := db.Model(&model.JobTask{}).Where("id = ?", first.ID).Update("status", model.TaskSucceeded).Error; err != nil {
		t.Fatal(err)
	}
	page, err = repo.ListRetryCandidates(context.Background(), scanNow, cursor, 128)
	if err != nil || len(page) != 2 || page[0].TaskID != second.ID || page[1].TaskID != boundary.ID {
		t.Fatalf("continued retry page: %+v err=%v", page, err)
	}
	cursor = &RetryCursor{NextDispatchAt: page[1].NextDispatchAt, TaskID: page[1].TaskID}
	page, err = repo.ListRetryCandidates(context.Background(), scanNow, cursor, 128)
	if err != nil || page == nil || len(page) != 0 {
		t.Fatalf("fixed upper time admitted later work: %+v err=%v", page, err)
	}
	page, err = repo.ListRetryCandidates(context.Background(), scanNow.Add(time.Second), nil, 128)
	if err != nil || len(page) != 3 || page[2].TaskID != later.ID {
		t.Fatalf("new traversal did not discover newly due work: %+v err=%v", page, err)
	}
}

func TestRetryCandidatesFilterParentStateDeadlineAndTaskState(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	_, valid := createRetryRepositoryTask(t, db, job, now, now)
	for _, state := range []model.InstanceStatus{model.InstanceWaiting, model.InstanceStopping, model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled} {
		instance, _ := createRetryRepositoryTask(t, db, job, now, now)
		if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("status", state).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, deadline := range []any{nil, now, now.Add(-time.Second)} {
		instance, _ := createRetryRepositoryTask(t, db, job, now, now)
		if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("deadline_at", deadline).Error; err != nil {
			t.Fatal(err)
		}
	}
	missing, _ := createRetryRepositoryTask(t, db, job, now, now)
	if err := db.Delete(&model.JobInstance{}, missing.ID).Error; err != nil {
		t.Fatal(err)
	}
	for _, state := range []model.TaskStatus{model.TaskWaiting, model.TaskRunning, model.TaskSucceeded, model.TaskFailed, model.TaskCanceled} {
		_, task := createRetryRepositoryTask(t, db, job, now, now)
		if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Update("status", state).Error; err != nil {
			t.Fatal(err)
		}
	}
	_, noTime := createRetryRepositoryTask(t, db, job, now, now)
	if err := db.Model(&model.JobTask{}).Where("id = ?", noTime.ID).Update("next_dispatch_at", nil).Error; err != nil {
		t.Fatal(err)
	}
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	before := readRepositoryTask(t, db, valid.ID)
	page, err := repo.ListRetryCandidates(context.Background(), now, nil, 128)
	if err != nil || len(page) != 1 || page[0].TaskID != valid.ID || !reflect.DeepEqual(before, readRepositoryTask(t, db, valid.ID)) {
		t.Fatalf("discovery changed work or included ineligible facts: %+v err=%v", page, err)
	}
	if err := db.Exec("ALTER TABLE job_instance RENAME TO unavailable_retry_parent").Error; err != nil {
		t.Fatal(err)
	}
	page, err = repo.ListRetryCandidates(context.Background(), now, nil, 128)
	var driverErr *pgconn.PgError
	if page != nil || !errors.As(err, &driverErr) || driverErr.Code != "42P01" {
		t.Fatalf("query failure became an empty traversal: %+v err=%v", page, err)
	}
}

func TestRetryCandidateQueryUsesDueIndexAndParentPrimaryKey(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	if err := db.Exec(`INSERT INTO job_instance
		(job_id, job_name, executor_group_id, trigger_type, scheduled_at, params_overridden, execution_config, status, started_at, deadline_at)
		SELECT n, 'retry history', 1, 1, CURRENT_TIMESTAMP, FALSE, '{}'::jsonb,
		CASE WHEN n % 10 = 0 THEN 6 ELSE 2 END, ?::timestamptz - INTERVAL '1 hour', ?::timestamptz + INTERVAL '1 hour'
		FROM generate_series(1, 30000) n`, now, now).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO job_task (instance_id, task_type, status, attempt_no, used_attempts, next_dispatch_at)
		SELECT id, 1, CASE WHEN id % 20 = 1 OR id % 20 = 0 THEN 3 ELSE 4 END, 1, 1,
		?::timestamptz - id * INTERVAL '1 second' FROM job_instance`, now).Error; err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"job_instance", "job_task"} {
		if err := db.Exec("ANALYZE " + table).Error; err != nil {
			t.Fatal(err)
		}
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	page, err := repo.ListRetryCandidates(context.Background(), now, nil, 128)
	if err != nil || len(page) != 128 {
		t.Fatalf("indexed retry first page: len=%d err=%v", len(page), err)
	}
	last := page[len(page)-1]
	page, err = repo.ListRetryCandidates(context.Background(), now, &RetryCursor{NextDispatchAt: last.NextDispatchAt, TaskID: last.TaskID}, 128)
	if err != nil || len(page) != 128 || counter.count != 2 {
		t.Fatalf("indexed retry next page: len=%d count=%d err=%v", len(page), counter.count, err)
	}
	for _, statement := range counter.statements {
		plan := explainExecutionQuery(t, db, statement)
		if !strings.Contains(plan, "idx_job_task_retry_due") || !strings.Contains(plan, "job_instance_pkey") ||
			strings.Contains(plan, "Seq Scan") || strings.Contains(plan, "Sort  (") {
			t.Fatalf("retry candidates lost indexed ordering or parent lookup:\n%s", plan)
		}
	}
}

func createRetryRepositoryTask(t *testing.T, db *gorm.DB, job *model.JobDefinition, due, now time.Time) (model.JobInstance, model.JobTask) {
	t.Helper()
	instance, task := createResultTask(t, db, job)
	deadline := now.Add(time.Hour)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("deadline_at", deadline).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).
		Updates(map[string]any{"status": model.TaskRetryWaiting, "next_dispatch_at": due}).Error; err != nil {
		t.Fatal(err)
	}
	instance.DeadlineAt = &deadline
	return instance, readRepositoryTask(t, db, task.ID)
}
