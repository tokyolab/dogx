//go:build integration

package repository

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
)

func TestOrdinaryAttemptRegistrationRechecksConcurrentTaskChanges(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	for _, change := range []struct {
		name   string
		fields map[string]any
	}{
		{"final state", map[string]any{"status": model.TaskSucceeded, "end_reason": model.EndReasonSuccess}},
		{"newer attempt", map[string]any{"attempt_no": 2, "used_attempts": 2}},
		{"later due time", map[string]any{"next_dispatch_at": now.Add(time.Hour)}},
		{"exhausted budget", map[string]any{"used_attempts": 4}},
	} {
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/commit=%t", change.name, commit), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				instance, task := createRetryRepositoryTask(t, db, job, now.Add(-time.Second), now)
				first := db.WithContext(ctx).Begin()
				if first.Error != nil {
					t.Fatal(first.Error)
				}
				defer first.Rollback()
				var blocker int
				if err := first.Raw("SELECT pg_backend_pid()").Scan(&blocker).Error; err != nil {
					t.Fatal(err)
				}
				updated := first.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(change.fields)
				if updated.Error != nil || updated.RowsAffected != 1 {
					t.Fatalf("competing Task update: rows=%d err=%v", updated.RowsAffected, updated.Error)
				}
				competing := readRepositoryTask(t, first, task.ID)
				type retryResult struct {
					registered *RegisteredTask
					err        error
				}
				pidReady := make(chan int, 1)
				finished := make(chan retryResult, 1)
				workerDone := make(chan struct{})
				go func() {
					defer close(workerDone)
					var registered *RegisteredTask
					err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
						var pid int
						if err := tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
							return err
						}
						instances, err := NewJobInstanceRepository(tx)
						if err != nil {
							return err
						}
						parent, err := instances.LockForExecution(ctx, instance.ID)
						if err != nil {
							return err
						}
						registeredAt, err := instances.DatabaseTime(ctx)
						if err != nil {
							return err
						}
						if parent == nil || parent.Status != model.InstanceRunning || parent.DeadlineAt == nil || !registeredAt.Before(*parent.DeadlineAt) {
							return fmt.Errorf("retry parent is not eligible: %+v", parent)
						}
						repo, err := NewJobTaskRepository(tx)
						if err != nil {
							return err
						}
						pidReady <- pid
						registered, err = repo.RegisterAttempt(ctx, AttemptRegistration{TaskType: model.TaskTypeOrdinary, ExpectedStatus: model.TaskRetryWaiting,
							TaskID: task.ID, InstanceID: instance.ID, ExpectedAttemptNo: 1, MaxAttempts: 4, RegisteredAt: registeredAt,
						})
						return err
					})
					finished <- retryResult{registered: registered, err: err}
				}()
				defer func() {
					cancel()
					first.Rollback()
					<-workerDone
				}()
				var waiter int
				select {
				case waiter = <-pidReady:
				case result := <-finished:
					t.Fatalf("registration stopped before its update: %+v", result)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
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
				select {
				case result := <-finished:
					if result.err != nil || (result.registered == nil) != commit {
						t.Fatalf("retry after competing transaction: %+v err=%v commit=%t", result.registered, result.err, commit)
					}
					if !commit && (result.registered.AttemptNo != 2 || result.registered.UsedAttempts != 2) {
						t.Fatalf("rolled-back competitor changed registered counts: %+v", result.registered)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				after := readRepositoryTask(t, db, task.ID)
				if commit && !reflect.DeepEqual(competing, after) {
					t.Fatal("retry overwrote the committed competing Task version")
				}
				if !commit && (after.Status != model.TaskRunning || after.AttemptNo != 2 || after.UsedAttempts != 2 || after.NextDispatchAt != nil) {
					t.Fatalf("retry was not committed after competitor rollback: %+v", after)
				}
			})
		}
	}
}
