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

func TestInstanceCompletionRechecksParentAfterCompetingWrite(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	for _, competing := range []struct {
		status model.InstanceStatus
		reason string
	}{
		{model.InstanceStopping, "manual_stop"},
		{model.InstanceFailed, model.EndReasonExecutionFailed},
	} {
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("status=%d/commit=%t", competing.status, commit), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				instance, _ := createResultTask(t, db, job)
				before := readCompletionInstance(t, db, instance.ID)
				first := db.WithContext(ctx).Begin()
				if first.Error != nil {
					t.Fatal(first.Error)
				}
				defer first.Rollback()
				var blocker int
				if err := first.Raw("SELECT pg_backend_pid()").Scan(&blocker).Error; err != nil {
					t.Fatal(err)
				}
				if err := first.Model(&model.JobInstance{}).Where("id = ?", instance.ID).
					Updates(map[string]any{"status": competing.status, "end_reason": competing.reason, "result_summary": "competing decision"}).Error; err != nil {
					t.Fatal(err)
				}
				competingVersion := readCompletionInstance(t, first, instance.ID)
				finished, done, pid := startCompletionWrite(t, ctx, db, instance.ID)
				defer func() {
					cancel()
					first.Rollback()
					<-done
				}()
				var waiter int
				select {
				case waiter = <-pid:
				case result := <-finished:
					t.Fatalf("completion stopped before reaching contention: %+v", result)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				// Observe an actual PostgreSQL lock wait before releasing the first
				// writer. No sleep assumes that the competing UPDATE has begun.
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
					if result.err != nil || result.applied != !commit || result.statements != 1 || !result.onlyUpdate {
						t.Fatalf("completion after contention: %+v", result)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				after := readCompletionInstance(t, db, instance.ID)
				if commit {
					if !reflect.DeepEqual(competingVersion, after) {
						t.Fatal("late completion overwrote the committed stop/final decision")
					}
					return
				}
				if after.Status != model.InstanceSucceeded || after.EndReason == nil || *after.EndReason != model.EndReasonSuccess ||
					after.ResultSummary == nil || *after.ResultSummary != "completion won" || after.FinishedAt == nil || !after.UpdatedAt.After(before.UpdatedAt) {
					t.Fatalf("completion did not proceed after rollback: %+v", after)
				}
				before.Status, before.EndReason, before.ResultSummary = after.Status, after.EndReason, after.ResultSummary
				before.FinishedAt, before.UpdatedAt = after.FinishedAt, after.UpdatedAt
				if !reflect.DeepEqual(before, after) {
					t.Fatal("completion after rollback changed unrelated parent facts")
				}
			})
		}
	}
}

func TestInstanceCompletionContextCancelsLockWait(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, _ := createResultTask(t, db, job)
	before := readCompletionInstance(t, db, instance.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first := db.WithContext(ctx).Begin()
	if first.Error != nil {
		t.Fatal(first.Error)
	}
	defer first.Rollback()
	var blocker int
	if err := first.Raw("SELECT pg_backend_pid()").Scan(&blocker).Error; err != nil {
		t.Fatal(err)
	}
	if err := first.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("result_summary", "held update").Error; err != nil {
		t.Fatal(err)
	}
	writeCtx, stopWrite := context.WithCancel(ctx)
	defer stopWrite()
	finished, done, pid := startCompletionWrite(t, writeCtx, db, instance.ID)
	defer func() { stopWrite(); first.Rollback(); <-done }()
	var waiter int
	select {
	case waiter = <-pid:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitForTaskBindingBlock(t, ctx, db, blocker, waiter)
	stopWrite()
	select {
	case result := <-finished:
		if result.applied || result.err == nil {
			t.Fatalf("canceled lock wait appeared committed: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("completion did not stop its canceled lock wait")
	}
	if err := first.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, readCompletionInstance(t, db, instance.ID)) {
		t.Fatal("canceled completion left a parent transition")
	}
}

func startCompletionWrite(t *testing.T, ctx context.Context, db *gorm.DB, instanceID int64) (<-chan concurrentResultWrite, <-chan struct{}, <-chan int) {
	t.Helper()
	finished, done, pid := make(chan concurrentResultWrite, 1), make(chan struct{}), make(chan int, 1)
	go func() {
		defer close(done)
		result := concurrentResultWrite{}
		// Pin only a connection for backend-PID observation. Complete still owns
		// its explicit transaction and must wait for that commit before success.
		result.err = db.WithContext(ctx).Connection(func(conn *gorm.DB) error {
			var backend int
			if err := conn.Raw("SELECT pg_backend_pid()").Scan(&backend).Error; err != nil {
				return err
			}
			pid <- backend
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			repo, err := NewJobInstanceRepository(conn.Session(&gorm.Session{Logger: counter}))
			if err != nil {
				return err
			}
			summary := "completion won"
			result.applied, err = repo.Complete(ctx, instanceID, model.InstanceSucceeded, &summary)
			result.statements = counter.count
			result.onlyUpdate = len(counter.statements) == 1 && strings.HasPrefix(counter.statements[0], "UPDATE ")
			return err
		})
		finished <- result
	}()
	return finished, done, pid
}
