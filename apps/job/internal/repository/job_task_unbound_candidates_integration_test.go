//go:build integration

package repository

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestUnboundCandidatesKeepFixedCutoffAndOriginalCursor(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	cutoff, registered := now.Add(-30*time.Second), now.Add(-31*time.Second)
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	// An earlier sequence allocation may commit after a later position has
	// already been processed. Only a new traversal must rediscover that row.
	_, lateCommit := createUnboundCandidateTask(t, tx, job, now, registered)
	_, first := createUnboundCandidateTask(t, db, job, now, registered)
	_, second := createUnboundCandidateTask(t, db, job, now, registered)
	_, boundary := createUnboundCandidateTask(t, db, job, now, cutoff)
	_, later := createUnboundCandidateTask(t, db, job, now, cutoff.Add(time.Microsecond))
	if err := db.Model(&model.JobTask{}).Where("id = ?", first.ID).Updates(map[string]any{
		"attempt_no": 7, "result_data": model.TaskData(`{"old":true}`), "result_summary": "previous attempt",
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		id   int64
		kind model.TaskType
	}{{second.ID, model.TaskTypeShard}, {boundary.ID, model.TaskTypePrepare}, {later.ID, model.TaskTypePost}} {
		if err := db.Model(&model.JobTask{}).Where("id = ?", entry.id).Update("task_type", entry.kind).Error; err != nil {
			t.Fatal(err)
		}
	}
	// Discovery does not depend on a live Job or parse the frozen snapshot.
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
	page, err := repo.ListUnboundCandidates(context.Background(), now, cutoff, nil, 1)
	if err != nil || len(page) != 1 || page[0].TaskID != first.ID || page[0].InstanceID != first.InstanceID ||
		page[0].AttemptNo != 7 || !page[0].AttemptRegisteredAt.Equal(registered) {
		t.Fatalf("initial unbound page: %+v err=%v", page, err)
	}
	if counter.count != 1 || strings.Contains(counter.statements[0], "(t.attempt_registered_at, t.id) >") {
		t.Fatalf("first page added a cursor or extra reads: %v", counter.statements)
	}
	for _, forbidden := range []string{"task_type", "execution_config", "result_data", "result_summary", "used_attempts", "job_definition", "FOR UPDATE", "OFFSET"} {
		if strings.Contains(counter.statements[0], forbidden) {
			t.Fatalf("candidate discovery included %s: %s", forbidden, counter.statements[0])
		}
	}
	cursor := &UnboundCursor{AttemptRegisteredAt: page[0].AttemptRegisteredAt, TaskID: page[0].TaskID}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobTask{}).Where("id = ?", first.ID).Update("status", model.TaskSucceeded).Error; err != nil {
		t.Fatal(err)
	}
	page, err = repo.ListUnboundCandidates(context.Background(), now, cutoff, cursor, 128)
	if err != nil || len(page) != 2 || page[0].TaskID != second.ID || page[1].TaskID != boundary.ID ||
		!page[1].AttemptRegisteredAt.Equal(cutoff) {
		t.Fatalf("original cursor skipped a tie or changed the cutoff: %+v err=%v", page, err)
	}
	cursor = &UnboundCursor{AttemptRegisteredAt: page[1].AttemptRegisteredAt, TaskID: page[1].TaskID}
	page, err = repo.ListUnboundCandidates(context.Background(), now, cutoff, cursor, 128)
	if err != nil || page == nil || len(page) != 0 {
		t.Fatalf("fixed traversal admitted a later or earlier position: %+v err=%v", page, err)
	}
	page, err = repo.ListUnboundCandidates(context.Background(), now.Add(time.Microsecond), cutoff.Add(time.Microsecond), nil, 128)
	if err != nil || len(page) != 4 || page[0].TaskID != lateCommit.ID || page[1].TaskID != second.ID ||
		page[2].TaskID != boundary.ID || page[3].TaskID != later.ID {
		t.Fatalf("new traversal lost late commits or newly eligible work: %+v err=%v", page, err)
	}
}

func TestUnboundCandidatesFilterBindingStateAndParentTimes(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-30 * time.Second)
	parent, valid := createUnboundCandidateTask(t, db, job, now, cutoff)
	for _, state := range []model.InstanceStatus{model.InstanceWaiting, model.InstanceStopping, model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled} {
		instance, _ := createUnboundCandidateTask(t, db, job, now, cutoff)
		if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("status", state).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, fields := range []map[string]any{
		{"started_at": nil},
		{"deadline_at": nil},
		{"deadline_at": now},
		{"deadline_at": now.Add(-time.Microsecond)},
		{"started_at": now.Add(time.Hour), "deadline_at": now.Add(time.Hour)},
		{"started_at": now.Add(2 * time.Hour), "deadline_at": now.Add(time.Hour)},
	} {
		instance, _ := createUnboundCandidateTask(t, db, job, now, cutoff)
		if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Updates(fields).Error; err != nil {
			t.Fatal(err)
		}
	}
	orphan, _ := createUnboundCandidateTask(t, db, job, now, cutoff)
	if err := db.Delete(&model.JobInstance{}, orphan.ID).Error; err != nil {
		t.Fatal(err)
	}
	for _, state := range []model.TaskStatus{model.TaskWaiting, model.TaskRetryWaiting, model.TaskSucceeded, model.TaskFailed, model.TaskCanceled} {
		_, task := createUnboundCandidateTask(t, db, job, now, cutoff)
		if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Update("status", state).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, fields := range []map[string]any{
		{"attempt_registered_at": nil},
		{"attempt_registered_at": cutoff.Add(time.Microsecond)},
		{"executor_instance_id": uuid.New()},
		{"executor_address": "127.0.0.1:9001"},
		{"executor_address": ""},
		{"executor_instance_id": uuid.New(), "executor_address": "127.0.0.1:9001"},
	} {
		_, task := createUnboundCandidateTask(t, db, job, now, cutoff)
		if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(fields).Error; err != nil {
			t.Fatal(err)
		}
	}
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	before, parentBefore := readRepositoryTask(t, db, valid.ID), readCompletionInstance(t, db, parent.ID)
	page, err := repo.ListUnboundCandidates(context.Background(), now, cutoff, nil, 128)
	if err != nil || len(page) != 1 || page[0].TaskID != valid.ID {
		t.Fatalf("discovery included ineligible facts: %+v err=%v", page, err)
	}
	if !reflect.DeepEqual(before, readRepositoryTask(t, db, valid.ID)) || !reflect.DeepEqual(parentBefore, readCompletionInstance(t, db, parent.ID)) {
		t.Fatal("candidate discovery changed Task or Instance facts")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if page, err = repo.ListUnboundCandidates(ctx, now, cutoff, nil, 128); page != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation became an empty traversal: %+v err=%v", page, err)
	}
	if err := db.Exec("ALTER TABLE job_instance RENAME TO unavailable_unbound_parent").Error; err != nil {
		t.Fatal(err)
	}
	page, err = repo.ListUnboundCandidates(context.Background(), now, cutoff, nil, 128)
	var driverErr *pgconn.PgError
	if page != nil || !errors.As(err, &driverErr) || driverErr.Code != "42P01" {
		t.Fatalf("failed parent read became an empty traversal: %+v err=%v", page, err)
	}
}

func TestUnboundCandidateQueryUsesObservationIndexAndParentPrimaryKey(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-30 * time.Second)
	if err := db.Exec(`INSERT INTO job_instance
		(job_id, job_name, executor_group_id, trigger_type, scheduled_at, params_overridden, execution_config, status, started_at, deadline_at)
		SELECT n, 'unbound history', 1, 1, CURRENT_TIMESTAMP, FALSE, '{}'::jsonb,
		CASE WHEN n % 10 = 0 THEN 6 ELSE 2 END, ?::timestamptz - INTERVAL '1 day', ?::timestamptz + INTERVAL '1 hour'
		FROM generate_series(1, 30000) n`, now, now).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO job_task
		(instance_id, task_type, status, attempt_no, used_attempts, attempt_registered_at, executor_address)
		SELECT id, 1, CASE WHEN id % 5 = 0 OR id % 5 = 1 THEN 2 ELSE 4 END, 1, 1,
		?::timestamptz - id * INTERVAL '1 second',
		CASE WHEN id % 20 = 0 OR id % 20 = 1 THEN NULL ELSE '127.0.0.1:9001' END FROM job_instance`, cutoff).Error; err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"job_instance", "job_task"} {
		if err := db.Exec("ANALYZE " + table).Error; err != nil {
			t.Fatal(err)
		}
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	capture := &executionQueryCapture{queryCounter: counter}
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: capture}))
	if err != nil {
		t.Fatal(err)
	}
	// The deployment page size may exceed the default of 128.
	page, err := repo.ListUnboundCandidates(context.Background(), now, cutoff, nil, 129)
	if err != nil || len(page) != 129 {
		t.Fatalf("indexed first page: len=%d err=%v", len(page), err)
	}
	last := page[len(page)-1]
	page, err = repo.ListUnboundCandidates(context.Background(), now, cutoff,
		&UnboundCursor{AttemptRegisteredAt: last.AttemptRegisteredAt, TaskID: last.TaskID}, 129)
	if err != nil || len(page) != 129 || counter.count != 2 || len(capture.queries) != 2 {
		t.Fatalf("indexed continued page: len=%d count=%d err=%v", len(page), counter.count, err)
	}
	for _, query := range capture.queries {
		plan := explainExecutionQuery(t, db, query)
		if !strings.Contains(plan, "idx_job_task_unbound_observation") || !strings.Contains(plan, "job_instance_pkey") ||
			strings.Contains(plan, "Seq Scan") || strings.Contains(plan, "Sort  (") {
			t.Fatalf("unbound candidates lost indexed ordering or parent lookup:\n%s", plan)
		}
	}
}

func createUnboundCandidateTask(t *testing.T, db *gorm.DB, job *model.JobDefinition, now, registered time.Time) (model.JobInstance, model.JobTask) {
	t.Helper()
	instance, task := createUnknownRepositoryTask(t, db, job, now)
	if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Update("attempt_registered_at", registered).Error; err != nil {
		t.Fatal(err)
	}
	task.AttemptRegisteredAt = &registered
	return instance, task
}
