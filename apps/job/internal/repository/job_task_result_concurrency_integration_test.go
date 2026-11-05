//go:build integration

package repository

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestTaskResultRepositoryRechecksAfterRowLockWait(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	identity := resultWriteFixture().ExecutorInstanceID
	other := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	for _, change := range []struct {
		name    string
		fields  map[string]any
		accepts bool
	}{
		{"already succeeded", map[string]any{"status": model.TaskSucceeded, "result_data": model.TaskResultData(`{"first":true}`), "end_reason": model.EndReasonSuccess}, false},
		{"already canceled", map[string]any{"status": model.TaskCanceled, "end_reason": "manual_stop"}, false},
		{"new attempt", map[string]any{"attempt_no": 2, "used_attempts": 2}, false},
		{"other executor", map[string]any{"executor_instance_id": other, "executor_address": "127.0.0.1:9001"}, false},
		{"same executor", map[string]any{"executor_instance_id": identity, "executor_address": "127.0.0.1:9002"}, true},
	} {
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/commit=%t", change.name, commit), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				instance, task := createResultTask(t, db, job)
				before := readRepositoryTask(t, db, task.ID)
				var parentBefore model.JobInstance
				if err := db.Where("id = ?", instance.ID).Take(&parentBefore).Error; err != nil {
					t.Fatal(err)
				}
				first := db.WithContext(ctx).Begin()
				if first.Error != nil {
					t.Fatal(first.Error)
				}
				defer first.Rollback()
				var blocker int
				if err := first.Raw("SELECT pg_backend_pid()").Scan(&blocker).Error; err != nil {
					t.Fatal(err)
				}
				// A normal UPDATE takes the contested lock. The result writer uses
				// its own independently committed UPDATE and no explicit row lock.
				updated := first.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(change.fields)
				if updated.Error != nil || updated.RowsAffected != 1 {
					t.Fatalf("competing update: rows=%d err=%v", updated.RowsAffected, updated.Error)
				}
				firstVersion := readRepositoryTask(t, first, task.ID)
				write := resultWriteFixture()
				write.TaskID, write.InstanceID = task.ID, instance.ID
				finished, workerDone, waiterPID := startResultWrite(t, ctx, db, write)
				defer func() {
					cancel()
					first.Rollback()
					<-workerDone
				}()
				var waiter int
				select {
				case waiter = <-waiterPID:
				case result := <-finished:
					t.Fatalf("result writer stopped before lock contention: %+v", result)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if waiter == blocker {
					t.Fatal("competing operations used the same connection")
				}
				// This existing helper queries pg_blocking_pids on a third connection;
				// reaching it is evidence of an actual wait, not a timing assumption.
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
				wantApplied := !commit || change.accepts
				select {
				case result := <-finished:
					if result.err != nil || result.applied != wantApplied || result.statements != 1 || !result.onlyUpdate {
						t.Fatalf("result after contention: %+v wantApplied=%v", result, wantApplied)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				after := readRepositoryTask(t, db, task.ID)
				expected := before
				if commit {
					expected = firstVersion
				}
				if wantApplied {
					if after.Status != model.TaskSucceeded || string(after.ResultData) != "null" ||
						after.ResultSummary == nil || *after.ResultSummary != write.Summary ||
						after.EndReason == nil || *after.EndReason != model.EndReasonSuccess ||
						after.FinishedAt == nil || !after.FinishedAt.Equal(after.UpdatedAt) || after.NextDispatchAt != nil ||
						!after.UpdatedAt.After(before.UpdatedAt) {
						t.Fatalf("incorrect successful result: %+v", after)
					}
					expected.Status, expected.ResultData, expected.ResultSummary = after.Status, after.ResultData, after.ResultSummary
					expected.EndReason, expected.FinishedAt, expected.NextDispatchAt, expected.UpdatedAt = after.EndReason, after.FinishedAt, after.NextDispatchAt, after.UpdatedAt
				}
				if !reflect.DeepEqual(expected, after) {
					t.Fatalf("result changed competing or unrelated facts: expected=%+v after=%+v", expected, after)
				}
				var parentAfter model.JobInstance
				if err := db.Where("id = ?", instance.ID).Take(&parentAfter).Error; err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(parentBefore, parentAfter) {
					t.Fatal("result write changed its parent")
				}
			})
		}
	}
}

func TestTaskResultRepositoryCommittedResultPreventsLateBinding(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, task := createResultTask(t, db, job)
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	write := resultWriteFixture()
	write.TaskID, write.InstanceID = task.ID, instance.ID
	if applied, err := repo.SaveResult(context.Background(), write); !applied || err != nil {
		t.Fatalf("save before ACK: applied=%v err=%v", applied, err)
	}
	before := readRepositoryTask(t, db, task.ID)
	if applied, err := repo.BindExecutor(context.Background(), taskExecutorBinding(task.ID, instance.ID)); applied || err != nil {
		t.Fatalf("late ACK: applied=%v err=%v", applied, err)
	}
	if after := readRepositoryTask(t, db, task.ID); !reflect.DeepEqual(before, after) || after.ExecutorInstanceID != nil || after.ExecutorAddress != nil {
		t.Fatalf("late ACK changed the committed result: %+v", after)
	}
}

type concurrentResultWrite struct {
	applied    bool
	err        error
	statements int
	onlyUpdate bool
}

func startResultWrite(t *testing.T, ctx context.Context, db *gorm.DB, write TaskResultWrite) (<-chan concurrentResultWrite, <-chan struct{}, <-chan int) {
	t.Helper()
	finished, done, pid := make(chan concurrentResultWrite, 1), make(chan struct{}), make(chan int, 1)
	go func() {
		defer close(done)
		result := concurrentResultWrite{}
		// Pin a connection only to publish its backend PID. This is not an outer
		// transaction: SaveResult still owns its independent commit boundary.
		result.err = db.WithContext(ctx).Connection(func(conn *gorm.DB) error {
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
			result.applied, err = repo.SaveResult(ctx, write)
			result.statements = counter.count
			result.onlyUpdate = len(counter.statements) == 1 && strings.HasPrefix(counter.statements[0], "UPDATE ")
			return err
		})
		finished <- result
	}()
	return finished, done, pid
}
