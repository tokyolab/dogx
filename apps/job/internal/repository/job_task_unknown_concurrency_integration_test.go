//go:build integration

package repository

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestUnknownSettlementRechecksAfterCompetingUpdate(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	processedAt := retryRepositoryTime(t, db)
	binding := taskExecutorBinding(1, 1)
	for _, change := range []struct {
		name    string
		fields  map[string]any
		applies bool
	}{
		{"ACK binding", map[string]any{"executor_instance_id": binding.ExecutorInstanceID, "executor_address": binding.ExecutorAddress}, false},
		{"business result", map[string]any{"status": model.TaskSucceeded, "result_data": model.TaskData(`{"saved":true}`), "result_summary": "completed", "finished_at": processedAt, "end_reason": model.EndReasonSuccess}, false},
		{"BUSY", map[string]any{"status": model.TaskRetryWaiting, "used_attempts": 0, "next_dispatch_at": processedAt.Add(time.Minute), "result_summary": "executor is busy; task was not accepted"}, false},
		{"other unknown retry", map[string]any{"status": model.TaskRetryWaiting, "next_dispatch_at": processedAt.Add(time.Minute), "result_summary": unknownResultSummary}, false},
		{"other unknown failure", map[string]any{"status": model.TaskFailed, "used_attempts": 4, "finished_at": processedAt, "end_reason": model.EndReasonResultUnknown, "result_summary": unknownResultSummary}, false},
		{"child cancellation", map[string]any{"status": model.TaskCanceled, "finished_at": processedAt, "end_reason": "manual_stop"}, false},
		{"used opportunities changed", map[string]any{"used_attempts": 4}, true},
		{"new attempt", map[string]any{"attempt_no": 2, "used_attempts": 2}, false},
		{"observation restarted", map[string]any{"attempt_registered_at": processedAt}, false},
	} {
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/commit=%t", change.name, commit), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				instance, task := createResultTask(t, db, job)
				before := readRepositoryTask(t, db, task.ID)
				write := unknownConcurrencyWrite(task, processedAt)
				first := db.WithContext(ctx).Begin()
				if first.Error != nil {
					t.Fatal(first.Error)
				}
				defer first.Rollback()
				var blocker int
				if err := first.Raw("SELECT pg_backend_pid()").Scan(&blocker).Error; err != nil {
					t.Fatal(err)
				}
				// Hold a competing persisted outcome before commit. The UPDATE's
				// own row lock makes the later statement recheck its predicates and
				// CASE against the winner under the default READ COMMITTED level.
				updated := first.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(change.fields)
				if updated.Error != nil || updated.RowsAffected != 1 {
					t.Fatalf("competing update: rows=%d err=%v", updated.RowsAffected, updated.Error)
				}
				winner := readRepositoryTask(t, first, task.ID)
				finished, done, pid := startUnknownConcurrencyWrite(ctx, db, write)
				defer func() { cancel(); first.Rollback(); <-done }()
				var waiter int
				select {
				case waiter = <-pid:
				case response := <-finished:
					t.Fatalf("settlement ended before contention: %+v", response)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if waiter == blocker {
					t.Fatal("competitors used the same PostgreSQL connection")
				}
				// A third connection observes the real wait, rather than treating
				// elapsed time as proof that the competing statement reached SQL.
				waitForTaskBindingBlock(t, ctx, db, blocker, waiter)
				var endErr error
				if commit {
					endErr = first.Commit().Error
				} else {
					endErr = first.Rollback().Error
				}
				if endErr != nil {
					t.Fatal(endErr)
				}
				wantApplied := !commit || change.applies
				var response unknownConcurrencyResult
				select {
				case response = <-finished:
					if response.err != nil || (response.settled != nil) != wantApplied || response.statements != 1 || !response.onlyUpdate {
						t.Fatalf("settlement after wait: %+v wantApplied=%v", response, wantApplied)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				after := readRepositoryTask(t, db, task.ID)
				if !wantApplied {
					if !reflect.DeepEqual(winner, after) {
						t.Fatal("unknown settlement overwrote a committed competing outcome")
					}
					return
				}
				expected := before
				if commit {
					expected = winner
				}
				assertUnknownConcurrencyOutcome(t, expected, after, write)
				if *response.settled != (SettledTask{TaskID: task.ID, InstanceID: instance.ID, AttemptNo: task.AttemptNo, Status: after.Status}) {
					t.Fatalf("returned facts do not match the committed transition: %+v", response.settled)
				}
			})
		}
	}
}

func TestUnknownSettlementClosesOriginalAttemptBeforeLateResponses(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	for _, used := range []int32{1, 4} {
		t.Run(fmt.Sprintf("used=%d", used), func(t *testing.T) {
			instance, task := createResultTask(t, db, job)
			if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Update("used_attempts", used).Error; err != nil {
				t.Fatal(err)
			}
			repo, err := NewJobTaskRepository(db)
			if err != nil {
				t.Fatal(err)
			}
			write := unknownConcurrencyWrite(task, retryRepositoryTime(t, db))
			if settled, err := repo.SettleUnknown(context.Background(), write); settled == nil || err != nil {
				t.Fatalf("initial settlement: settled=%+v err=%v", settled, err)
			}
			closed := readRepositoryTask(t, db, task.ID)
			if closed.AttemptNo != task.AttemptNo || closed.UsedAttempts != used {
				t.Fatal("settlement changed the attempt identity or consumed opportunities")
			}
			if applied, err := repo.BindExecutor(context.Background(), taskExecutorBinding(task.ID, instance.ID)); applied || err != nil {
				t.Fatalf("late ACK reopened the original attempt: applied=%v err=%v", applied, err)
			}
			// An ingress ACK or queued payload does not establish a persisted
			// result. Every possible result disposition still rechecks Task state.
			for _, status := range []model.TaskStatus{model.TaskSucceeded, model.TaskRetryWaiting, model.TaskFailed} {
				result := resultWriteFixture()
				result.TaskID, result.InstanceID, result.Status, result.RetryIntervalSeconds = task.ID, instance.ID, status, 10
				if applied, err := repo.SaveResult(context.Background(), result); applied || err != nil {
					t.Fatalf("late result reopened the original attempt: status=%v applied=%v err=%v", status, applied, err)
				}
			}
			if applied, err := saveDispatchFixture(context.Background(), repo, true, task.ID, instance.ID); applied || err != nil {
				t.Fatalf("late BUSY refunded an already closed attempt: applied=%v err=%v", applied, err)
			}
			write.ProcessedAt = write.ProcessedAt.Add(time.Minute)
			write.ObservationCutoff = write.ObservationCutoff.Add(time.Minute)
			write.RetryAt = write.RetryAt.Add(time.Minute)
			if settled, err := repo.SettleUnknown(context.Background(), write); settled != nil || err != nil {
				t.Fatalf("duplicate settlement: settled=%+v err=%v", settled, err)
			}
			if after := readRepositoryTask(t, db, task.ID); !reflect.DeepEqual(closed, after) {
				t.Fatal("late response or duplicate settlement changed the closed attempt")
			}
		})
	}
}

func TestUnknownSettlementPreservesStoppingAfterParentRead(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	for _, used := range []int32{1, 4} {
		t.Run(fmt.Sprintf("used=%d", used), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			instance, task := createResultTask(t, db, job)
			processedAt := retryRepositoryTime(t, db)
			if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).
				Updates(map[string]any{"started_at": processedAt.Add(-time.Minute), "deadline_at": processedAt.Add(time.Hour)}).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Update("used_attempts", used).Error; err != nil {
				t.Fatal(err)
			}
			before := readRepositoryTask(t, db, task.ID)
			var observed model.JobInstance
			if err := db.WithContext(ctx).Where("id = ?", instance.ID).Take(&observed).Error; err != nil {
				t.Fatal(err)
			}
			if observed.Status != model.InstanceRunning || observed.DeadlineAt == nil || !processedAt.Before(*observed.DeadlineAt) {
				t.Fatal("fixture did not allow the original parent precheck")
			}
			if err := db.WithContext(ctx).Model(&model.JobInstance{}).Where("id = ?", instance.ID).
				Updates(map[string]any{"status": model.InstanceStopping, "end_reason": "manual_stop"}).Error; err != nil {
				t.Fatal(err)
			}
			var stopped model.JobInstance
			if err := db.WithContext(ctx).Where("id = ?", instance.ID).Take(&stopped).Error; err != nil {
				t.Fatal(err)
			}
			// Keep the stopped parent busy on another connection. Child settlement
			// must commit without waiting for or rewriting this parent; a subsequent
			// retry still requires the Registrar's separate parent authorization.
			parentTx := db.WithContext(ctx).Begin()
			if parentTx.Error != nil {
				t.Fatal(parentTx.Error)
			}
			defer parentTx.Rollback()
			if err := parentTx.Model(&model.JobInstance{}).Where("id = ?", instance.ID).
				Update("updated_at", gorm.Expr("clock_timestamp()")).Error; err != nil {
				t.Fatal(err)
			}
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
			if err != nil {
				t.Fatal(err)
			}
			write := unknownConcurrencyWrite(task, processedAt)
			settled, err := repo.SettleUnknown(ctx, write)
			if err != nil || settled == nil || counter.count != 1 || len(counter.statements) != 1 ||
				!strings.HasPrefix(counter.statements[0], "UPDATE ") || strings.Contains(counter.statements[0], "job_instance") ||
				strings.Contains(counter.statements[0], "FOR UPDATE") {
				t.Fatalf("child settlement acquired or changed the parent: settled=%+v err=%v statements=%v", settled, err, counter.statements)
			}
			if err := parentTx.Rollback().Error; err != nil {
				t.Fatal(err)
			}
			assertUnknownConcurrencyOutcome(t, before, readRepositoryTask(t, db, task.ID), write)
			var after model.JobInstance
			if err := db.WithContext(ctx).Where("id = ?", instance.ID).Take(&after).Error; err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(stopped, after) {
				t.Fatal("unknown settlement changed the committed parent stopping decision")
			}
		})
	}
}

func unknownConcurrencyWrite(task model.JobTask, processedAt time.Time) UnknownSettlement {
	return UnknownSettlement{TaskID: task.ID, InstanceID: task.InstanceID, ExpectedAttemptNo: task.AttemptNo,
		MaxAttempts: 4, ProcessedAt: processedAt, ObservationCutoff: processedAt.Add(-30 * time.Second), RetryAt: processedAt.Add(10 * time.Second)}
}

func assertUnknownConcurrencyOutcome(t *testing.T, before, after model.JobTask, write UnknownSettlement) {
	t.Helper()
	if after.ResultData != nil || after.ResultSummary == nil || *after.ResultSummary != unknownResultSummary || !after.UpdatedAt.Equal(write.ProcessedAt) {
		t.Fatalf("unknown settlement did not save its diagnosis and processing time: %+v", after)
	}
	if int64(before.UsedAttempts) < write.MaxAttempts {
		if after.Status != model.TaskRetryWaiting || after.NextDispatchAt == nil || !after.NextDispatchAt.Equal(write.RetryAt) || after.FinishedAt != nil || after.EndReason != nil {
			t.Fatalf("incorrect unknown retry: %+v", after)
		}
	} else if after.Status != model.TaskFailed || after.NextDispatchAt != nil || after.FinishedAt == nil || !after.FinishedAt.Equal(write.ProcessedAt) ||
		after.EndReason == nil || *after.EndReason != model.EndReasonResultUnknown {
		t.Fatalf("incorrect final unknown failure: %+v", after)
	}
	expected := before
	expected.Status, expected.ResultData, expected.ResultSummary = after.Status, after.ResultData, after.ResultSummary
	expected.NextDispatchAt, expected.FinishedAt, expected.EndReason, expected.UpdatedAt = after.NextDispatchAt, after.FinishedAt, after.EndReason, after.UpdatedAt
	if !reflect.DeepEqual(expected, after) {
		t.Fatal("unknown settlement changed binding, attempt identity, opportunities or registration facts")
	}
}

type unknownConcurrencyResult struct {
	settled    *SettledTask
	err        error
	statements int
	onlyUpdate bool
}

func startUnknownConcurrencyWrite(ctx context.Context, db *gorm.DB, write UnknownSettlement) (<-chan unknownConcurrencyResult, <-chan struct{}, <-chan int) {
	finished, done, pid := make(chan unknownConcurrencyResult, 1), make(chan struct{}), make(chan int, 1)
	go func() {
		defer close(done)
		response := unknownConcurrencyResult{}
		// Pin the backend without an outer transaction so the repository can
		// confirm its own commit rather than returning from a savepoint.
		response.err = db.WithContext(ctx).Connection(func(conn *gorm.DB) error {
			var backend int
			if err := conn.Raw("SELECT pg_backend_pid()").Scan(&backend).Error; err != nil {
				return err
			}
			pid <- backend
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			repo, err := NewJobTaskRepository(conn.Session(&gorm.Session{Logger: counter}))
			if err != nil {
				return err
			}
			response.settled, err = repo.SettleUnknown(ctx, write)
			response.statements = counter.count
			response.onlyUpdate = len(counter.statements) == 1 && strings.HasPrefix(counter.statements[0], "UPDATE ") &&
				!strings.Contains(counter.statements[0], "job_instance") && !strings.Contains(counter.statements[0], "FOR UPDATE")
			return err
		})
		finished <- response
	}()
	return finished, done, pid
}
