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

func TestTaskDispatchRepositoryRechecksAfterRowLockWait(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	identity := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	for _, busy := range []bool{true, false} {
		for _, change := range []struct {
			name    string
			fields  map[string]any
			accepts bool
		}{
			{"opportunities changed", map[string]any{"used_attempts": 4}, true},
			{"already settled", map[string]any{"status": model.TaskRetryWaiting}, false},
			{"business result won", map[string]any{"status": model.TaskSucceeded, "result_data": model.TaskData(`{"first":true}`)}, false},
			{"new attempt", map[string]any{"attempt_no": 2, "used_attempts": 2}, false},
			{"full binding", map[string]any{"executor_instance_id": identity, "executor_address": "127.0.0.1:9000"}, false},
			{"identity only", map[string]any{"executor_instance_id": identity}, false},
			{"address only", map[string]any{"executor_address": "127.0.0.1:9000"}, false},
		} {
			for _, commit := range []bool{false, true} {
				t.Run(fmt.Sprintf("busy=%v/%s/commit=%v", busy, change.name, commit), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					instance, task := createResultTask(t, db, job)
					before := readRepositoryTask(t, db, task.ID)
					first := db.WithContext(ctx).Begin()
					if first.Error != nil {
						t.Fatal(first.Error)
					}
					defer first.Rollback()
					var blocker int
					if err := first.Raw("SELECT pg_backend_pid()").Scan(&blocker).Error; err != nil {
						t.Fatal(err)
					}
					// Hold a real competing UPDATE open. The response must wait for
					// this row and then reevaluate both its conditions and CASE values.
					result := first.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(change.fields)
					if result.Error != nil || result.RowsAffected != 1 {
						t.Fatalf("competing update: rows=%d err=%v", result.RowsAffected, result.Error)
					}
					firstVersion := readRepositoryTask(t, first, task.ID)
					finished, done, waiterPID := startDispatchWrite(ctx, db, busy, task.ID, instance.ID)
					defer func() { cancel(); first.Rollback(); <-done }()
					var waiter int
					select {
					case waiter = <-waiterPID:
					case response := <-finished:
						t.Fatalf("writer ended before contention: %+v", response)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if waiter == blocker {
						t.Fatal("competitors used the same connection")
					}
					// pg_blocking_pids on a third connection proves the contested wait;
					// no sleep is used as evidence that the response has reached SQL.
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
					case response := <-finished:
						if response.err != nil || response.applied != wantApplied || response.statements != 1 || !response.onlyUpdate {
							t.Fatalf("response after wait: %+v wantApplied=%v", response, wantApplied)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					after := readRepositoryTask(t, db, task.ID)
					expected := before
					if commit {
						expected = firstVersion
					}
					if !wantApplied {
						if !reflect.DeepEqual(expected, after) {
							t.Fatal("stale response changed competing facts")
						}
						return
					}
					status, used := model.TaskRetryWaiting, expected.UsedAttempts
					summary, delay := dispatchRejectionFixture().Summary, 10*time.Second
					if busy {
						used--
						summary, delay = "executor is busy; task was not accepted", dispatchBusyFixture().Delay
					} else if used >= 4 {
						status = model.TaskFailed
					}
					assertDispatchOutcome(t, expected, after, status, used, summary, delay)
				})
			}
		}
	}
}

func startDispatchWrite(ctx context.Context, db *gorm.DB, busy bool, taskID, instanceID int64) (<-chan concurrentResultWrite, <-chan struct{}, <-chan int) {
	finished, done, pid := make(chan concurrentResultWrite, 1), make(chan struct{}), make(chan int, 1)
	go func() {
		defer close(done)
		result := concurrentResultWrite{}
		// Pin a connection for its backend PID without opening a transaction.
		// The repository still confirms its own independent write and commit.
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
			result.applied, err = saveDispatchFixture(ctx, repo, busy, taskID, instanceID)
			result.statements = counter.count
			result.onlyUpdate = len(counter.statements) == 1 && strings.HasPrefix(counter.statements[0], "UPDATE ")
			return err
		})
		finished <- result
	}()
	return finished, done, pid
}
