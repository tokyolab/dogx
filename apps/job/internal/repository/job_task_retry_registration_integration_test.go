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
	"gorm.io/gorm/logger"
)

func TestOrdinaryAttemptRegistrationKeepsFirstStartAndSummary(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	instance, task := createRetryRepositoryTask(t, db, job, now.Add(-time.Second), now)
	identity, address := uuid.New(), "127.0.0.1:9001"
	if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(map[string]any{
		"attempt_no": 7, "used_attempts": 3, "executor_instance_id": identity, "executor_address": address,
		"result_data": model.TaskData(`{"old":true}`), "result_summary": "last failure",
		"finished_at": now.Add(-time.Minute), "end_reason": model.EndReasonExecutionFailed,
	}).Error; err != nil {
		t.Fatal(err)
	}
	before, parentBefore := readRepositoryTask(t, db, task.ID), readCompletionInstance(t, db, instance.ID)
	tx, registeredAt := retryRepositoryTransaction(t, db, instance.ID)
	defer tx.Rollback()
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobTaskRepository(tx.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	registered, err := repo.RegisterAttempt(context.Background(), AttemptRegistration{TaskType: model.TaskTypeOrdinary, ExpectedStatus: model.TaskRetryWaiting,
		TaskID: task.ID, InstanceID: instance.ID, ExpectedAttemptNo: 7, MaxAttempts: 4, RegisteredAt: registeredAt,
	})
	if err != nil || registered == nil || registered.TaskID != task.ID || registered.InstanceID != instance.ID ||
		registered.AttemptNo != 8 || registered.UsedAttempts != 4 || !registered.RegisteredAt.Equal(registeredAt) {
		t.Fatalf("new retry version: %+v err=%v", registered, err)
	}
	if counter.count != 1 || !strings.HasPrefix(counter.statements[0], "UPDATE ") ||
		!strings.Contains(counter.statements[0], "RETURNING") || strings.Contains(counter.statements[0], "result_summary") || strings.Contains(counter.statements[0], "started_at") {
		t.Fatalf("retry registration added reads or changed retained fields: %v", counter.statements)
	}
	if !reflect.DeepEqual(before, readRepositoryTask(t, db, task.ID)) {
		t.Fatal("RETURNING was exposed as an already committed retry")
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	after := readRepositoryTask(t, db, task.ID)
	if after.Status != model.TaskRunning || after.AttemptNo != 8 || after.UsedAttempts != 4 || after.AttemptRegisteredAt == nil ||
		!after.AttemptRegisteredAt.Equal(registeredAt) || after.NextDispatchAt != nil || after.ExecutorInstanceID != nil || after.ExecutorAddress != nil ||
		after.ResultData != nil || after.FinishedAt != nil || after.EndReason != nil || !after.UpdatedAt.Equal(registeredAt) {
		t.Fatalf("incorrect retry reset: %+v", after)
	}
	expected := before
	expected.Status, expected.AttemptNo, expected.UsedAttempts = after.Status, after.AttemptNo, after.UsedAttempts
	expected.AttemptRegisteredAt, expected.NextDispatchAt, expected.UpdatedAt = after.AttemptRegisteredAt, nil, after.UpdatedAt
	expected.ExecutorInstanceID, expected.ExecutorAddress, expected.ResultData, expected.FinishedAt, expected.EndReason = nil, nil, nil, nil, nil
	if !reflect.DeepEqual(expected, after) || !reflect.DeepEqual(parentBefore, readCompletionInstance(t, db, instance.ID)) {
		t.Fatal("retry registration changed first-start, summary or parent execution facts")
	}
}

func TestOrdinaryAttemptRegistrationZeroRowsPreserveOriginalFacts(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	for _, scenario := range []struct {
		name   string
		fields map[string]any
		change func(*AttemptRegistration)
	}{
		{name: "missing task", change: func(w *AttemptRegistration) { w.TaskID += 100000 }},
		{name: "wrong parent", change: func(w *AttemptRegistration) { w.InstanceID += 100000 }},
		{name: "old attempt", change: func(w *AttemptRegistration) { w.ExpectedAttemptNo = 2 }},
		{name: "changed stage", fields: map[string]any{"task_type": model.TaskTypeShard}},
		{name: "running", fields: map[string]any{"status": model.TaskRunning}},
		{name: "succeeded", fields: map[string]any{"status": model.TaskSucceeded}},
		{name: "canceled", fields: map[string]any{"status": model.TaskCanceled}},
		{name: "not due", fields: map[string]any{"next_dispatch_at": now.Add(time.Hour)}},
		{name: "missing due time", fields: map[string]any{"next_dispatch_at": nil}},
		{name: "budget exhausted", fields: map[string]any{"used_attempts": 4}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			instance, task := createRetryRepositoryTask(t, db, job, now.Add(-time.Second), now)
			if len(scenario.fields) != 0 {
				if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(scenario.fields).Error; err != nil {
					t.Fatal(err)
				}
			}
			before := readRepositoryTask(t, db, task.ID)
			tx, registeredAt := retryRepositoryTransaction(t, db, instance.ID)
			defer tx.Rollback()
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			repo, err := NewJobTaskRepository(tx.Session(&gorm.Session{Logger: counter}))
			if err != nil {
				t.Fatal(err)
			}
			write := AttemptRegistration{TaskType: model.TaskTypeOrdinary, ExpectedStatus: model.TaskRetryWaiting, TaskID: task.ID, InstanceID: instance.ID, ExpectedAttemptNo: 1, MaxAttempts: 4, RegisteredAt: registeredAt}
			if scenario.change != nil {
				scenario.change(&write)
			}
			if registered, err := repo.RegisterAttempt(context.Background(), write); registered != nil || err != nil || counter.count != 1 {
				t.Fatalf("ineligible retry: %+v err=%v queries=%d", registered, err, counter.count)
			}
			if err := tx.Commit().Error; err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, readRepositoryTask(t, db, task.ID)) {
				t.Fatal("zero-row retry changed stored facts")
			}
		})
	}
}

func TestOrdinaryDispatchCandidateDoesNotSkipAnotherClosedAttempt(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	instance, task := createRetryRepositoryTask(t, db, job, now.Add(-time.Second), now)
	register := func(expected int64) *RegisteredTask {
		t.Helper()
		tx, registeredAt := retryRepositoryTransaction(t, db, instance.ID)
		defer tx.Rollback()
		repo, err := NewJobTaskRepository(tx)
		if err != nil {
			t.Fatal(err)
		}
		registered, err := repo.RegisterAttempt(context.Background(), AttemptRegistration{TaskType: model.TaskTypeOrdinary, ExpectedStatus: model.TaskRetryWaiting,
			TaskID: task.ID, InstanceID: instance.ID, ExpectedAttemptNo: expected, MaxAttempts: 4, RegisteredAt: registeredAt,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit().Error; err != nil {
			t.Fatal(err)
		}
		return registered
	}
	if registered := register(1); registered == nil || registered.AttemptNo != 2 {
		t.Fatalf("second attempt was not registered: %+v", registered)
	}
	if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).
		Updates(map[string]any{"status": model.TaskRetryWaiting, "next_dispatch_at": now, "result_summary": "second failure"}).Error; err != nil {
		t.Fatal(err)
	}
	before := readRepositoryTask(t, db, task.ID)
	if registered := register(1); registered != nil || !reflect.DeepEqual(before, readRepositoryTask(t, db, task.ID)) {
		t.Fatal("old candidate registered again after a newer attempt returned to retry state")
	}
	if registered := register(2); registered == nil || registered.AttemptNo != 3 || registered.UsedAttempts != 3 {
		t.Fatalf("fresh candidate could not register: %+v", registered)
	}
}

func TestOrdinaryRetryReusesRefundedBudgetWithoutReusingAttempt(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	instance, task := createRetryRepositoryTask(t, db, job, now, now)
	if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).
		Updates(map[string]any{"status": model.TaskRunning, "next_dispatch_at": nil}).Error; err != nil {
		t.Fatal(err)
	}
	independent, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := independent.SaveDispatchBusy(context.Background(), DispatchBusyWrite{
		TaskID: task.ID, InstanceID: instance.ID, AttemptNo: 1, Delay: time.Microsecond,
	}); err != nil || !applied {
		t.Fatalf("refund rejected attempt: applied=%t err=%v", applied, err)
	}
	refunded := readRepositoryTask(t, db, task.ID)
	if refunded.Status != model.TaskRetryWaiting || refunded.AttemptNo != 1 || refunded.UsedAttempts != 0 {
		t.Fatalf("BUSY did not refund only the opportunity: %+v", refunded)
	}
	tx, registeredAt := retryRepositoryTransaction(t, db, instance.ID)
	defer tx.Rollback()
	repo, err := NewJobTaskRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	registered, err := repo.RegisterAttempt(context.Background(), AttemptRegistration{TaskType: model.TaskTypeOrdinary, ExpectedStatus: model.TaskRetryWaiting,
		TaskID: task.ID, InstanceID: instance.ID, ExpectedAttemptNo: 1, MaxAttempts: 1, RegisteredAt: registeredAt,
	})
	if err != nil || registered == nil || registered.AttemptNo != 2 || registered.UsedAttempts != 1 {
		t.Fatalf("refunded first opportunity could not be reused: %+v err=%v", registered, err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
}

func TestOrdinaryRetryRollbackAndIntegerOverflow(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	for _, scenario := range []struct {
		name     string
		attempt  int64
		used     int32
		max      int64
		overflow bool
	}{
		{"explicit rollback", 1, 1, 4, false},
		{"wide budget", 1, 1<<31 - 2, 1 << 31, false},
		{"attempt overflow", 1<<63 - 1, 1, 4, true},
		{"used count overflow", 1, 1<<31 - 1, 1 << 31, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			instance, task := createRetryRepositoryTask(t, db, job, now, now)
			if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).
				Updates(map[string]any{"attempt_no": scenario.attempt, "used_attempts": scenario.used}).Error; err != nil {
				t.Fatal(err)
			}
			before := readRepositoryTask(t, db, task.ID)
			tx, registeredAt := retryRepositoryTransaction(t, db, instance.ID)
			defer tx.Rollback()
			repo, err := NewJobTaskRepository(tx)
			if err != nil {
				t.Fatal(err)
			}
			registered, err := repo.RegisterAttempt(context.Background(), AttemptRegistration{TaskType: model.TaskTypeOrdinary, ExpectedStatus: model.TaskRetryWaiting,
				TaskID: task.ID, InstanceID: instance.ID, ExpectedAttemptNo: scenario.attempt, MaxAttempts: scenario.max, RegisteredAt: registeredAt,
			})
			if scenario.overflow {
				var driverErr *pgconn.PgError
				if registered != nil || !errors.As(err, &driverErr) || driverErr.Code != "22003" {
					t.Fatalf("overflow did not reject registration: %+v err=%v", registered, err)
				}
			} else if err != nil || registered == nil || registered.AttemptNo != scenario.attempt+1 || registered.UsedAttempts != scenario.used+1 {
				t.Fatalf("representable retry failed: %+v err=%v", registered, err)
			}
			if err := tx.Rollback().Error; err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, readRepositoryTask(t, db, task.ID)) {
				t.Fatal("rollback retained a new state, attempt or consumed opportunity")
			}
		})
	}
}

func retryRepositoryTime(t *testing.T, db *gorm.DB) time.Time {
	t.Helper()
	var now time.Time
	if err := db.Raw("SELECT clock_timestamp()").Scan(&now).Error; err != nil {
		t.Fatal(err)
	}
	return now
}

func retryRepositoryTransaction(t *testing.T, db *gorm.DB, instanceID int64) (*gorm.DB, time.Time) {
	t.Helper()
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	t.Cleanup(func() { tx.Rollback() })
	instances, err := NewJobInstanceRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := instances.LockForExecution(context.Background(), instanceID)
	if err != nil || parent == nil || parent.Status != model.InstanceRunning {
		t.Fatalf("lock retry parent: %+v err=%v", parent, err)
	}
	registeredAt, err := instances.DatabaseTime(context.Background())
	if err != nil || parent.DeadlineAt == nil || !registeredAt.Before(*parent.DeadlineAt) {
		t.Fatalf("retry fixture is not authorized by database time: %v", err)
	}
	return tx, registeredAt
}
