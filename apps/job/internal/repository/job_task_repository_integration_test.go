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
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

func taskRepositoryFixture(t *testing.T) (*gorm.DB, int64, int64, time.Time) {
	t.Helper()
	db, job := repositoryFixture(t)
	ctx := context.Background()
	instances, err := NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	instanceID, created, err := instances.Insert(ctx, repositoryInstance(job))
	if err != nil || !created {
		t.Fatalf("create parent: created = %v, error = %v", created, err)
	}
	// Mirror the parent lock that coordinates first start with competing starts
	// and stops. The fixture's outer transaction stays open; it makes no
	// commit-success claim.
	var parent model.JobInstance
	if err := db.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", instanceID).Take(&parent).Error; err != nil {
		t.Fatal(err)
	}
	started := time.Date(2000, 1, 2, 3, 4, 5, 123456000, time.UTC)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", instanceID).Updates(map[string]any{
		"status": model.InstanceRunning, "started_at": started, "deadline_at": started.Add(15 * time.Minute),
	}).Error; err != nil {
		t.Fatal(err)
	}
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	taskID, err := repo.InsertInitial(ctx, instanceID, model.TaskTypeOrdinary, started)
	if err != nil || taskID <= 0 {
		t.Fatalf("create ordinary task: ID = %d, error = %v", taskID, err)
	}
	return db, instanceID, taskID, started
}

func readRepositoryTask(t *testing.T, db *gorm.DB, id int64) model.JobTask {
	t.Helper()
	var task model.JobTask
	if err := db.Where("id = ?", id).Take(&task).Error; err != nil {
		t.Fatal(err)
	}
	return task
}

func taskExecutorBinding(taskID, instanceID int64) ExecutorBinding {
	return ExecutorBinding{TaskID: taskID, InstanceID: instanceID, AttemptNo: 1,
		ExecutorInstanceID: uuid.MustParse("11111111-1111-4111-8111-111111111111"), ExecutorAddress: "127.0.0.1:9090"}
}

func TestTaskRepositoryInsertInitialUsesOneStartTime(t *testing.T) {
	db, instanceID, taskID, started := taskRepositoryFixture(t)
	task := readRepositoryTask(t, db, taskID)
	if task.InstanceID != instanceID || task.TaskType != model.TaskTypeOrdinary || task.Status != model.TaskRunning || task.AttemptNo != 1 || task.UsedAttempts != 1 {
		t.Fatalf("unexpected first attempt: %+v", task)
	}
	if task.StartedAt == nil || task.AttemptRegisteredAt == nil || !task.StartedAt.Equal(started) ||
		!task.AttemptRegisteredAt.Equal(started) || !task.CreatedAt.Equal(started) || !task.UpdatedAt.Equal(started) {
		t.Fatalf("first attempt used different timestamps: %+v", task)
	}
	if task.NextDispatchAt != nil || task.ExecutorInstanceID != nil || task.ExecutorAddress != nil ||
		task.ResultData != nil || task.ResultSummary != nil || task.EndReason != nil || task.FinishedAt != nil {
		t.Fatalf("first attempt contains execution outcomes: %+v", task)
	}

	// A savepoint isolates the deliberate constraint failure without disguising
	// it as successful creation or aborting the surrounding fixture transaction.
	err := db.Transaction(func(tx *gorm.DB) error {
		repo, err := NewJobTaskRepository(tx)
		if err != nil {
			return err
		}
		id, err := repo.InsertInitial(context.Background(), instanceID, model.TaskTypeOrdinary, started)
		if id != 0 || err == nil {
			t.Fatalf("singleton conflict reported creation: ID = %d, error = %v", id, err)
		}
		return err
	})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "uq_job_task_singleton" {
		t.Fatalf("singleton cause was not preserved: %v", err)
	}
	if got := readRepositoryTask(t, db, taskID); !reflect.DeepEqual(task, got) {
		t.Fatalf("duplicate create changed the existing task: %+v", got)
	}
}

func TestTaskRepositoryBindExecutorWritesOnceWithoutChangingExecution(t *testing.T) {
	db, instanceID, taskID, _ := taskRepositoryFixture(t)
	before := readRepositoryTask(t, db, taskID)
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	binding := taskExecutorBinding(taskID, instanceID)
	if applied, err := repo.BindExecutor(context.Background(), binding); err != nil || !applied {
		t.Fatalf("first binding: applied = %v, error = %v", applied, err)
	}
	if counter.count != 1 || !strings.HasPrefix(counter.statements[0], "UPDATE ") {
		t.Fatalf("binding performed queries beyond its update: %v", counter.statements)
	}
	after := readRepositoryTask(t, db, taskID)
	if after.ExecutorInstanceID == nil || *after.ExecutorInstanceID != binding.ExecutorInstanceID || after.ExecutorAddress == nil || *after.ExecutorAddress != binding.ExecutorAddress || !after.UpdatedAt.After(before.UpdatedAt) {
		t.Fatalf("binding did not persist both fields and audit time: %+v", after)
	}
	expected := before
	expected.ExecutorInstanceID, expected.ExecutorAddress, expected.UpdatedAt = after.ExecutorInstanceID, after.ExecutorAddress, after.UpdatedAt
	if !reflect.DeepEqual(expected, after) {
		t.Fatalf("binding changed execution facts: before = %+v, after = %+v", before, after)
	}
	for _, conflicting := range []bool{false, true} {
		if conflicting {
			binding.ExecutorInstanceID = uuid.MustParse("22222222-2222-4222-8222-222222222222")
			binding.ExecutorAddress = "[::1]:9091"
		}
		counter.count, counter.statements = 0, nil
		if applied, err := repo.BindExecutor(context.Background(), binding); err != nil || applied {
			t.Fatalf("repeat binding: applied = %v, error = %v", applied, err)
		}
		if counter.count != 1 {
			t.Fatalf("zero-row binding queried or retried: %v", counter.statements)
		}
		if got := readRepositoryTask(t, db, taskID); !reflect.DeepEqual(after, got) {
			t.Fatalf("repeat binding overwrote the original executor: %+v", got)
		}
	}
}

func TestTaskRepositoryBindExecutorRejectsStaleOrPartiallyBoundAttempts(t *testing.T) {
	db, instanceID, taskID, _ := taskRepositoryFixture(t)
	rollback := errors.New("rollback binding scenario")
	for _, tc := range []struct {
		name   string
		fields map[string]any
		change func(*ExecutorBinding)
	}{
		{name: "missing task", change: func(b *ExecutorBinding) { b.TaskID += 1000 }},
		{name: "wrong instance", change: func(b *ExecutorBinding) { b.InstanceID += 1000 }},
		{name: "old attempt", fields: map[string]any{"attempt_no": 2}},
		{name: "waiting", fields: map[string]any{"status": model.TaskWaiting}},
		{name: "retry waiting", fields: map[string]any{"status": model.TaskRetryWaiting}},
		{name: "result succeeded", fields: map[string]any{"status": model.TaskSucceeded, "result_data": model.TaskData(`{"completed":true}`), "end_reason": "success"}},
		{name: "result failed", fields: map[string]any{"status": model.TaskFailed, "end_reason": "execution_failed"}},
		{name: "canceled", fields: map[string]any{"status": model.TaskCanceled, "end_reason": "manual_stop"}},
		{name: "identity only", fields: map[string]any{"executor_instance_id": uuid.MustParse("22222222-2222-4222-8222-222222222222")}},
		{name: "address only", fields: map[string]any{"executor_address": "127.0.0.1:9091"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := db.Transaction(func(tx *gorm.DB) error {
				if len(tc.fields) > 0 {
					if err := tx.Model(&model.JobTask{}).Where("id = ?", taskID).Updates(tc.fields).Error; err != nil {
						return err
					}
				}
				before := readRepositoryTask(t, tx, taskID)
				counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
				repo, err := NewJobTaskRepository(tx.Session(&gorm.Session{Logger: counter}))
				if err != nil {
					return err
				}
				binding := taskExecutorBinding(taskID, instanceID)
				if tc.change != nil {
					tc.change(&binding)
				}
				if applied, err := repo.BindExecutor(context.Background(), binding); err != nil || applied {
					t.Fatalf("stale binding applied = %v, error = %v", applied, err)
				}
				if counter.count != 1 {
					t.Fatalf("zero-row binding performed extra work: %v", counter.statements)
				}
				if after := readRepositoryTask(t, tx, taskID); !reflect.DeepEqual(before, after) {
					t.Fatalf("stale binding changed execution facts: %+v", after)
				}
				return rollback
			})
			if !errors.Is(err, rollback) {
				t.Fatal(err)
			}
		})
	}
}

func TestTaskRepositoryBindExecutorAllowsStoppingParent(t *testing.T) {
	db, instanceID, taskID, _ := taskRepositoryFixture(t)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", instanceID).Updates(map[string]any{
		"status": model.InstanceStopping, "end_reason": "manual_stop",
	}).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	binding := taskExecutorBinding(taskID, instanceID)
	binding.ExecutorAddress = "[::1]:9090"
	if applied, err := repo.BindExecutor(context.Background(), binding); err != nil || !applied {
		t.Fatalf("stopping parent blocked binding: applied = %v, error = %v", applied, err)
	}
	if counter.count != 1 {
		t.Fatalf("binding inspected the parent: %v", counter.statements)
	}
	var parent model.JobInstance
	if err := db.Where("id = ?", instanceID).Take(&parent).Error; err != nil {
		t.Fatal(err)
	}
	if parent.Status != model.InstanceStopping || parent.EndReason == nil || *parent.EndReason != "manual_stop" {
		t.Fatalf("binding changed the stopping decision: %+v", parent)
	}
}

func TestTaskRepositoryDatabaseErrorsDoNotReportSuccess(t *testing.T) {
	db, instanceID, taskID, started := taskRepositoryFixture(t)
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if id, err := repo.InsertInitial(ctx, instanceID, model.TaskTypeOrdinary, started); id != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled insert: ID = %d, error = %v", id, err)
	}
	if applied, err := repo.BindExecutor(ctx, taskExecutorBinding(taskID, instanceID)); applied || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled binding: applied = %v, error = %v", applied, err)
	}
	for _, operation := range []string{"insert", "bind"} {
		t.Run(operation, func(t *testing.T) {
			// The temporary table removal is confined to this fixture's savepoint.
			// Both repository operations must retain the actual PostgreSQL failure.
			err := db.Transaction(func(tx *gorm.DB) error {
				if err := tx.Exec("DROP TABLE job_task").Error; err != nil {
					return err
				}
				repo, err := NewJobTaskRepository(tx)
				if err != nil {
					return err
				}
				if operation == "insert" {
					id, err := repo.InsertInitial(context.Background(), instanceID, model.TaskTypeOrdinary, started)
					if id != 0 || err == nil {
						t.Fatalf("failed write reported insertion: ID = %d, error = %v", id, err)
					}
					return err
				}
				applied, err := repo.BindExecutor(context.Background(), taskExecutorBinding(taskID, instanceID))
				if applied || err == nil {
					t.Fatalf("failed write reported binding: applied = %v, error = %v", applied, err)
				}
				return err
			})
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
				t.Fatalf("database cause was lost: %v", err)
			}
		})
	}
	if task := readRepositoryTask(t, db, taskID); task.ExecutorInstanceID != nil || task.ExecutorAddress != nil {
		t.Fatalf("failed writes changed the binding: %+v", task)
	}
}
