//go:build integration

package repository

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
)

func TestTaskRepositoryBindingRechecksConcurrentUpdates(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	group := createReferenceTestGroup(t, db, "binding")
	job := referenceTestJob(group.ID)
	if err := db.Create(job).Error; err != nil {
		t.Fatal(err)
	}
	otherIdentity := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	for _, change := range []struct {
		name   string
		fields map[string]any
	}{
		{"state", map[string]any{"status": model.TaskSucceeded, "end_reason": "success", "result_data": model.TaskResultData(`{"done":true}`)}},
		{"attempt", map[string]any{"attempt_no": 2, "used_attempts": 2}},
		{"binding", map[string]any{"executor_instance_id": otherIdentity, "executor_address": "127.0.0.1:9091"}},
	} {
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/commit=%t", change.name, commit), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				started := time.Date(2000, 1, 2, 3, 4, 5, 123456000, time.UTC)
				deadline := started.Add(15 * time.Minute)
				requestID, operatorID := uuid.New(), int64(1)
				instance := model.JobInstance{JobID: job.ID, JobName: job.Name, ExecutorGroupID: group.ID,
					TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID,
					ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: job.ProcessorName,
						ExecutionMode: model.ExecutionModeNormal, RetryIntervalSeconds: 10, TimeoutSeconds: 900},
					Status: model.InstanceRunning, StartedAt: &started, DeadlineAt: &deadline, CreatedAt: started, UpdatedAt: started}
				task := model.JobTask{TaskType: model.TaskTypeOrdinary, Status: model.TaskRunning,
					AttemptNo: 1, UsedAttempts: 1, AttemptRegisteredAt: &started, StartedAt: &started,
					CreatedAt: started, UpdatedAt: started}
				// Finish the fixture transaction before opening the two competing
				// connections; a surrounding savepoint would hide these rows from them.
				if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
					if err := tx.Create(&instance).Error; err != nil {
						return err
					}
					task.InstanceID = instance.ID
					return tx.Create(&task).Error
				}); err != nil {
					t.Fatal(err)
				}
				before := readRepositoryTask(t, db, task.ID)
				firstTx := db.WithContext(ctx).Begin()
				if firstTx.Error != nil {
					t.Fatal(firstTx.Error)
				}
				defer firstTx.Rollback()
				var firstPID int
				if err := firstTx.Raw("SELECT pg_backend_pid()").Scan(&firstPID).Error; err != nil {
					t.Fatal(err)
				}
				// The ordinary UPDATE itself holds the row lock. No extra product
				// lock or test-only SELECT FOR UPDATE changes the binding protocol.
				result := firstTx.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(change.fields)
				if result.Error != nil || result.RowsAffected != 1 {
					t.Fatalf("first update: rows = %d, error = %v", result.RowsAffected, result.Error)
				}
				firstVersion := readRepositoryTask(t, firstTx, task.ID)
				binding := taskExecutorBinding(task.ID, instance.ID)
				secondPID := make(chan int, 1)
				type bindResult struct {
					applied bool
					err     error
				}
				finished := make(chan bindResult, 1)
				workerDone := make(chan struct{})
				go func() {
					defer close(workerDone)
					var applied bool
					err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
						var pid int
						if err := tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
							return err
						}
						secondPID <- pid
						repo, err := NewJobTaskRepository(tx)
						if err != nil {
							return err
						}
						applied, err = repo.BindExecutor(ctx, binding)
						return err
					})
					finished <- bindResult{applied: applied, err: err}
				}()
				defer func() {
					cancel()
					firstTx.Rollback()
					<-workerDone
				}()
				var waiter int
				select {
				case waiter = <-secondPID:
				case result := <-finished:
					t.Fatalf("binding stopped before its update: %+v", result)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if waiter == firstPID {
					t.Fatal("competing operations used the same PostgreSQL connection")
				}
				waitForTaskBindingBlock(t, ctx, db, firstPID, waiter)
				var endErr error
				if commit {
					endErr = firstTx.Commit().Error
				} else {
					endErr = firstTx.Rollback().Error
				}
				if endErr != nil {
					t.Fatal(endErr)
				}
				select {
				case result := <-finished:
					if result.err != nil || result.applied != !commit {
						t.Fatalf("binding after competing transaction: applied = %v, want = %v, error = %v", result.applied, !commit, result.err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				after := readRepositoryTask(t, db, task.ID)
				if commit {
					if !reflect.DeepEqual(firstVersion, after) {
						t.Fatalf("late binding overwrote committed execution facts: before = %+v, after = %+v", firstVersion, after)
					}
					return
				}
				if !after.UpdatedAt.After(before.UpdatedAt) {
					t.Fatal("successful binding did not update its audit timestamp")
				}
				before.ExecutorInstanceID, before.ExecutorAddress, before.UpdatedAt = &binding.ExecutorInstanceID, &binding.ExecutorAddress, after.UpdatedAt
				if !reflect.DeepEqual(before, after) {
					t.Fatalf("binding after rollback changed unrelated facts: expected = %+v, actual = %+v", before, after)
				}
			})
		}
	}
}

func waitForTaskBindingBlock(t *testing.T, ctx context.Context, db *gorm.DB, blocker, waiter int) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := db.WithContext(ctx).Raw("SELECT ? = ANY(pg_blocking_pids(?))", blocker, waiter).Scan(&blocked).Error; err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("binding did not wait for the competing Task update")
		case <-ticker.C:
		}
	}
}
