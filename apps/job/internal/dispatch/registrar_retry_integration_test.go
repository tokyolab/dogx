//go:build integration

package dispatch

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
)

func TestRetryRegistrarsCompeteWithoutResettingFrozenExecution(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	instance := createRegistrarInstance(t, db)
	initial, err := newIntegrationRegistrar(t, db).StartInstance(context.Background(), instance.ID)
	if err != nil || initial == nil {
		t.Fatalf("initial registration=%+v err=%v", initial, err)
	}
	parentBefore := loadRegistrarInstance(t, db, instance.ID)
	dueAt := registrarDatabaseTime(t, db).Add(-time.Second)
	if err := db.Model(&model.JobTask{}).Where("id = ?", initial.TaskID).
		Updates(map[string]any{"status": model.TaskRetryWaiting, "next_dispatch_at": dueAt, "result_summary": "previous failure"}).Error; err != nil {
		t.Fatal(err)
	}
	taskBefore := onlyRegistrarTask(t, db, instance.ID)
	// The current definition deliberately disagrees with the running snapshot.
	job := model.JobDefinition{Name: "changed job", ExecutorGroupID: instance.ExecutorGroupID + 1,
		ProcessorName: "replacement.processor", ExecutionMode: model.ExecutionModeSharded, PostEnabled: true,
		CronExpr: "0 * * * * *", MaxRetries: 0, RetryIntervalSeconds: 45, TimeoutSeconds: 1}
	job.ID = instance.JobID
	if err := db.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	candidate := repository.DispatchCandidate{TaskType: model.TaskTypeOrdinary, Status: model.TaskRetryWaiting, TaskID: initial.TaskID, InstanceID: instance.ID,
		AttemptNo: initial.AttemptNo, NextDispatchAt: dueAt}
	run := registrarTransactionRunner(t, db)
	ready, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseCommit := sync.OnceFunc(func() { close(release) })
	first, err := NewRegistrar(func(ctx context.Context, fn func(repository.Repositories) error) error {
		return run(ctx, func(repos repository.Repositories) error {
			if err := fn(repos); err != nil {
				return err
			}
			close(ready)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	second := newIntegrationRegistrar(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	type outcome struct {
		attempt *RegisteredAttempt
		err     error
	}
	result := make(chan outcome, 1)
	go func() {
		defer close(done)
		attempt, err := first.RegisterTask(ctx, candidate)
		result <- outcome{attempt: attempt, err: err}
	}()
	defer func() {
		releaseCommit()
		cancel()
		<-done
	}()
	select {
	case <-ready:
	case got := <-result:
		t.Fatalf("retry did not reach commit barrier: attempt=%+v err=%v", got.attempt, got.err)
	case <-ctx.Done():
		t.Fatal("retry did not finish registration")
	}
	if visible := onlyRegistrarTask(t, db, instance.ID); !reflect.DeepEqual(visible, taskBefore) {
		t.Fatal("uncommitted retry became visible outside its transaction")
	}
	contenderCtx, stop := context.WithTimeout(ctx, time.Second)
	contender, err := second.RegisterTask(contenderCtx, candidate)
	stop()
	if contender != nil || !errors.Is(err, repository.ErrInstanceLocked) {
		t.Fatalf("contending retry=%+v err=%v", contender, err)
	}
	releaseCommit()
	winner := <-result
	if winner.err != nil || winner.attempt == nil || winner.attempt.AttemptNo != 2 ||
		winner.attempt.ExecutionConfig != instance.ExecutionConfig || winner.attempt.ExecutorGroupID != instance.ExecutorGroupID ||
		!winner.attempt.DeadlineAt.Equal(initial.DeadlineAt) {
		t.Fatalf("committed retry=%+v err=%v", winner.attempt, winner.err)
	}
	if after := loadRegistrarInstance(t, db, instance.ID); !reflect.DeepEqual(after, parentBefore) {
		t.Fatal("retry changed the parent's first start, deadline or frozen facts")
	}
	task := onlyRegistrarTask(t, db, instance.ID)
	if task.AttemptNo != 2 || task.UsedAttempts != 2 || task.Status != model.TaskRunning ||
		task.AttemptRegisteredAt == nil || !task.AttemptRegisteredAt.Equal(winner.attempt.RegisteredAt) ||
		task.StartedAt == nil || !task.StartedAt.Equal(*taskBefore.StartedAt) ||
		task.ResultSummary == nil || *task.ResultSummary != "previous failure" {
		t.Fatal("committed retry did not preserve its original lifetime and last result")
	}
	if attempt, err := second.RegisterTask(ctx, candidate); attempt != nil || err != nil {
		t.Fatalf("same candidate obtained another authorization: attempt=%+v err=%v", attempt, err)
	}
}
