//go:build integration

package repository

import (
	"context"
	"errors"
	"fmt"
	"math"
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

func TestTaskDispatchRepositoryPersistsOutcomesAndDuplicateIsNoop(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	for _, scenario := range []struct {
		name       string
		busy       bool
		used       int32
		maxRetries int32
		wantStatus model.TaskStatus
		wantUsed   int32
	}{
		{"BUSY refunds initial opportunity", true, 1, 3, model.TaskRetryWaiting, 0},
		{"BUSY refunds later opportunity", true, 3, 3, model.TaskRetryWaiting, 2},
		{"rejection retains opportunity", false, 1, 3, model.TaskRetryWaiting, 1},
		{"rejection exhausts opportunity", false, 4, 3, model.TaskFailed, 4},
		{"rejection without retries", false, 1, 0, model.TaskFailed, 1},
		{"total opportunities does not overflow", false, math.MaxInt32, math.MaxInt32, model.TaskRetryWaiting, math.MaxInt32},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			instance, task := createResultTask(t, db, job)
			// An old terminal payload must be cleared explicitly. A large attempt
			// number must not replace UsedAttempts when deciding retry eligibility.
			if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(map[string]any{
				"attempt_no": 99, "used_attempts": scenario.used,
				"result_data": model.TaskResultData(`{"old":true}`), "result_summary": "old summary",
				"next_dispatch_at": task.CreatedAt, "finished_at": task.CreatedAt, "end_reason": "old reason",
			}).Error; err != nil {
				t.Fatal(err)
			}
			before := readRepositoryTask(t, db, task.ID)
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
			if err != nil {
				t.Fatal(err)
			}
			busyWrite, rejection := dispatchBusyFixture(), dispatchRejectionFixture()
			busyWrite.TaskID, busyWrite.InstanceID, busyWrite.AttemptNo = task.ID, instance.ID, 99
			rejection.TaskID, rejection.InstanceID, rejection.AttemptNo = task.ID, instance.ID, 99
			rejection.MaxRetries, rejection.Summary = scenario.maxRetries, ""
			save := func() (bool, error) {
				if scenario.busy {
					return repo.SaveDispatchBusy(context.Background(), busyWrite)
				}
				return repo.SaveDispatchRejection(context.Background(), rejection)
			}
			var beforeWrite time.Time
			if err := db.Raw("SELECT clock_timestamp()").Scan(&beforeWrite).Error; err != nil {
				t.Fatal(err)
			}
			if applied, err := save(); !applied || err != nil {
				t.Fatalf("save: applied=%v err=%v", applied, err)
			}
			assertSingleDispatchUpdate(t, counter)
			after := readRepositoryTask(t, db, task.ID)
			summary, delay := rejection.Summary, time.Duration(rejection.RetryIntervalSeconds)*time.Second
			if scenario.busy {
				summary, delay = "executor is busy; task was not accepted", busyWrite.Delay
			}
			assertDispatchOutcome(t, before, after, scenario.wantStatus, scenario.wantUsed, summary, delay)
			if after.UpdatedAt.Before(beforeWrite) {
				t.Fatal("response time came from a prior registration")
			}
			counter.count, counter.statements = 0, nil
			if applied, err := save(); applied || err != nil {
				t.Fatalf("duplicate: applied=%v err=%v", applied, err)
			}
			assertSingleDispatchUpdate(t, counter)
			if duplicate := readRepositoryTask(t, db, task.ID); !reflect.DeepEqual(after, duplicate) {
				t.Fatal("duplicate response refunded or postponed again")
			}
		})
	}
}

func TestTaskDispatchRepositoryRechecksIdentityWithoutParentRestrictions(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	identity := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	for _, busy := range []bool{true, false} {
		for _, scenario := range []struct {
			name                     string
			fields                   map[string]any
			wrongTask, wrongInstance bool
			apply                    bool
		}{
			{name: "expired stopping parent", apply: true},
			{name: "missing task", wrongTask: true},
			{name: "wrong instance", wrongInstance: true},
			{name: "new attempt", fields: map[string]any{"attempt_no": 2}},
			{name: "no opportunity", fields: map[string]any{"used_attempts": 0}},
			{name: "prepare", fields: map[string]any{"task_type": model.TaskTypePrepare}},
			{name: "shard", fields: map[string]any{"task_type": model.TaskTypeShard}},
			{name: "post", fields: map[string]any{"task_type": model.TaskTypePost}},
			{name: "retry waiting", fields: map[string]any{"status": model.TaskRetryWaiting}},
			{name: "succeeded", fields: map[string]any{"status": model.TaskSucceeded}},
			{name: "canceled", fields: map[string]any{"status": model.TaskCanceled}},
			{name: "full binding", fields: map[string]any{"executor_instance_id": identity, "executor_address": "127.0.0.1:9000"}},
			{name: "identity only", fields: map[string]any{"executor_instance_id": identity}},
			{name: "address only", fields: map[string]any{"executor_address": "127.0.0.1:9000"}},
		} {
			t.Run(fmt.Sprintf("busy=%v/%s", busy, scenario.name), func(t *testing.T) {
				instance, task := createResultTask(t, db, job)
				if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).
					Updates(map[string]any{"status": model.InstanceStopping, "end_reason": "manual_stop"}).Error; err != nil {
					t.Fatal(err)
				}
				var parentBefore model.JobInstance
				if err := db.Where("id = ?", instance.ID).Take(&parentBefore).Error; err != nil {
					t.Fatal(err)
				}
				if len(scenario.fields) > 0 {
					if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(scenario.fields).Error; err != nil {
						t.Fatal(err)
					}
				}
				before := readRepositoryTask(t, db, task.ID)
				counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
				repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
				if err != nil {
					t.Fatal(err)
				}
				taskID, instanceID := task.ID, instance.ID
				if scenario.wrongTask {
					taskID += 1000000
				}
				if scenario.wrongInstance {
					instanceID += 1000000
				}
				if applied, err := saveDispatchFixture(context.Background(), repo, busy, taskID, instanceID); applied != scenario.apply || err != nil {
					t.Fatalf("conditional write: applied=%v err=%v", applied, err)
				}
				assertSingleDispatchUpdate(t, counter)
				after := readRepositoryTask(t, db, task.ID)
				if !scenario.apply && !reflect.DeepEqual(before, after) {
					t.Fatal("stale response changed persisted facts")
				}
				if scenario.apply {
					used, summary, delay := int32(1), dispatchRejectionFixture().Summary, 10*time.Second
					if busy {
						used, summary, delay = 0, "executor is busy; task was not accepted", dispatchBusyFixture().Delay
					}
					assertDispatchOutcome(t, before, after, model.TaskRetryWaiting, used, summary, delay)
				}
				var parentAfter model.JobInstance
				if err := db.Where("id = ?", instance.ID).Take(&parentAfter).Error; err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(parentBefore, parentAfter) {
					t.Fatal("response changed the parent")
				}
			})
		}
	}
}

func TestTaskDispatchRepositoryPreservesDatabaseErrors(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, task := createResultTask(t, db, job)
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, busy := range []bool{true, false} {
		if applied, err := saveDispatchFixture(ctx, repo, busy, task.ID, instance.ID); applied || !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled write: applied=%v err=%v", applied, err)
		}
	}
	if err := db.Exec("DROP TABLE job_task").Error; err != nil {
		t.Fatal(err)
	}
	for _, busy := range []bool{true, false} {
		applied, err := saveDispatchFixture(context.Background(), repo, busy, task.ID, instance.ID)
		var pgErr *pgconn.PgError
		if applied || !errors.Is(err, ErrDispatchWriteRejected) || !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
			t.Fatalf("missing table lost cause: applied=%v err=%v", applied, err)
		}
	}
}

func assertSingleDispatchUpdate(t *testing.T, counter *queryCounter) {
	t.Helper()
	if counter.count != 1 || len(counter.statements) != 1 || !strings.HasPrefix(counter.statements[0], "UPDATE ") ||
		strings.Count(counter.statements[0], "clock_timestamp()") != 1 || strings.Contains(counter.statements[0], "job_instance") ||
		strings.Contains(counter.statements[0], "FOR UPDATE") {
		t.Fatalf("response exceeded one child UPDATE/clock: %v", counter.statements)
	}
}

func assertDispatchOutcome(t *testing.T, before, after model.JobTask, status model.TaskStatus, used int32, summary string, delay time.Duration) {
	t.Helper()
	if after.Status != status || after.UsedAttempts != used || after.ResultData != nil || after.ResultSummary == nil || *after.ResultSummary != summary ||
		!after.UpdatedAt.After(before.UpdatedAt) {
		t.Fatalf("incorrect dispatch outcome: %+v", after)
	}
	if status == model.TaskRetryWaiting {
		if after.FinishedAt != nil || after.EndReason != nil || after.NextDispatchAt == nil || !after.NextDispatchAt.Equal(after.UpdatedAt.Add(delay)) {
			t.Fatalf("incorrect delayed retry: %+v", after)
		}
	} else if after.NextDispatchAt != nil || after.FinishedAt == nil || !after.FinishedAt.Equal(after.UpdatedAt) ||
		after.EndReason == nil || *after.EndReason != model.EndReasonDispatchRejected {
		t.Fatalf("incorrect terminal rejection: %+v", after)
	}
	expected := before
	expected.Status, expected.UsedAttempts = after.Status, after.UsedAttempts
	expected.ResultData, expected.ResultSummary = after.ResultData, after.ResultSummary
	expected.NextDispatchAt, expected.FinishedAt, expected.EndReason, expected.UpdatedAt = after.NextDispatchAt, after.FinishedAt, after.EndReason, after.UpdatedAt
	if !reflect.DeepEqual(expected, after) {
		t.Fatalf("response changed unrelated facts: expected=%+v after=%+v", expected, after)
	}
}
