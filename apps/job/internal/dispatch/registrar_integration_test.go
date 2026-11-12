//go:build integration

package dispatch

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
)

func TestRegistrarStartsOrdinaryAtomicallyAndDoesNotReplay(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	instance := createRegistrarInstance(t, db)
	run := registrarTransactionRunner(t, db)
	observedBeforeCommit := false
	registrar, err := NewRegistrar(func(ctx context.Context, fn func(repository.Repositories) error) error {
		return run(ctx, func(repos repository.Repositories) error {
			if err := fn(repos); err != nil {
				return err
			}
			// This handle uses another pool connection, outside the registration
			// transaction. Neither half of the first start may be visible yet.
			var visible model.JobInstance
			if err := db.WithContext(ctx).Where("id = ?", instance.ID).Take(&visible).Error; err != nil {
				return err
			}
			var count int64
			if err := db.WithContext(ctx).Model(&model.JobTask{}).Where("instance_id = ?", instance.ID).Count(&count).Error; err != nil {
				return err
			}
			if visible.Status != model.InstanceWaiting || visible.StartedAt != nil || visible.DeadlineAt != nil || count != 0 {
				return errors.New("uncommitted first-start writes became visible")
			}
			observedBeforeCommit = true
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	before := registrarDatabaseTime(t, db)
	attempt, err := registrar.StartInstance(context.Background(), instance.ID)
	after := registrarDatabaseTime(t, db)
	if err != nil || attempt == nil {
		t.Fatalf("first start: attempt=%v error=%v", attempt, err)
	}
	if !observedBeforeCommit {
		t.Fatal("registration did not reach the pre-commit visibility check")
	}
	stored := loadRegistrarInstance(t, db, instance.ID)
	task := onlyRegistrarTask(t, db, instance.ID)
	if stored.Status != model.InstanceRunning || stored.StartedAt == nil || stored.DeadlineAt == nil {
		t.Fatalf("invalid started Instance: %+v", stored)
	}
	if stored.StartedAt.Before(before) || stored.StartedAt.After(after) {
		t.Fatalf("start time %s is outside database registration interval [%s, %s]", stored.StartedAt, before, after)
	}
	if !stored.DeadlineAt.Equal(stored.StartedAt.Add(time.Duration(instance.ExecutionConfig.TimeoutSeconds) * time.Second)) {
		t.Fatal("round deadline does not use the frozen timeout from the registration time")
	}
	if !stored.CreatedAt.Equal(instance.CreatedAt) || !stored.UpdatedAt.Equal(*stored.StartedAt) || stored.FinishedAt != nil || stored.EndReason != nil || stored.ResultSummary != nil {
		t.Fatal("first start changed creation or result fields, or failed to maintain the update time")
	}
	if task.TaskType != model.TaskTypeOrdinary || task.Status != model.TaskRunning || task.AttemptNo != 1 || task.UsedAttempts != 1 {
		t.Fatalf("invalid first Task: %+v", task)
	}
	if task.StartedAt == nil || !task.StartedAt.Equal(*stored.StartedAt) ||
		task.AttemptRegisteredAt == nil || !task.AttemptRegisteredAt.Equal(*stored.StartedAt) ||
		!task.CreatedAt.Equal(*stored.StartedAt) || !task.UpdatedAt.Equal(*stored.StartedAt) {
		t.Fatal("parent and Task do not share one registration time")
	}
	if task.ExecutorInstanceID != nil || task.ExecutorAddress != nil || task.NextDispatchAt != nil ||
		task.ResultData != nil || task.ResultSummary != nil || task.EndReason != nil || task.FinishedAt != nil {
		t.Fatal("first registration populated binding, result, dispatch-delay or completion fields")
	}
	if attempt.JobID != instance.JobID || attempt.InstanceID != instance.ID || attempt.ExecutorGroupID != instance.ExecutorGroupID ||
		attempt.TaskID != task.ID || attempt.AttemptNo != task.AttemptNo || attempt.ExecutionConfig != instance.ExecutionConfig ||
		!attempt.RegisteredAt.Equal(*stored.StartedAt) || !attempt.DeadlineAt.Equal(*stored.DeadlineAt) {
		t.Fatalf("returned attempt does not match committed execution facts: %+v", attempt)
	}

	registrar = newIntegrationRegistrar(t, db)
	replayed, err := registrar.StartInstance(context.Background(), instance.ID)
	if err != nil || replayed != nil {
		t.Fatalf("repeat start exposed an old attempt: attempt=%v error=%v", replayed, err)
	}
	if got := loadRegistrarInstance(t, db, instance.ID); !reflect.DeepEqual(got, stored) {
		t.Fatalf("repeat start changed the Instance: before=%+v after=%+v", stored, got)
	}
	if got := onlyRegistrarTask(t, db, instance.ID); !reflect.DeepEqual(got, task) {
		t.Fatalf("repeat start changed the attempt: before=%+v after=%+v", task, got)
	}
}

func TestRegistrarUsesFrozenInstanceConfiguration(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	instance := createRegistrarInstance(t, db)
	job := model.JobDefinition{
		Name: "edited job", ExecutorGroupID: instance.ExecutorGroupID + 1,
		ProcessorName: "replacement.processor", ExecutionMode: model.ExecutionModeSharded, PostEnabled: true,
		Params: "changed parameters", CronExpr: "0 * * * * *", MaxRetries: 9,
		RetryIntervalSeconds: 45, TimeoutSeconds: 1,
	}
	job.ID = instance.JobID
	if err := db.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	attempt, err := newIntegrationRegistrar(t, db).StartInstance(context.Background(), instance.ID)
	if err != nil || attempt == nil {
		t.Fatalf("start with edited current Job: attempt=%v error=%v", attempt, err)
	}
	if attempt.ExecutorGroupID != instance.ExecutorGroupID || attempt.ExecutionConfig != instance.ExecutionConfig ||
		attempt.DeadlineAt.Sub(attempt.RegisteredAt) != time.Duration(instance.ExecutionConfig.TimeoutSeconds)*time.Second {
		t.Fatalf("registration followed the current Job instead of the Instance snapshot: %+v", attempt)
	}
}

func TestRegistrarSkipsLockedParentAndCanStartAfterRollback(t *testing.T) {
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	locked := createRegistrarInstance(t, db)
	other := createRegistrarInstance(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	holder, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback() }()
	var lockedID int64
	// Hold only this parent row to reproduce another node's first-start or stop
	// transaction. A competing registration must skip it without waiting.
	if err := holder.QueryRowContext(ctx, "SELECT id FROM job_instance WHERE id = $1 FOR UPDATE", locked.ID).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.ExecContext(ctx, "UPDATE job_instance SET status = 6 WHERE id = $1", locked.ID); err != nil {
		t.Fatal(err)
	}
	registrar := newIntegrationRegistrar(t, db)
	attemptCtx, attemptCancel := context.WithTimeout(ctx, time.Second)
	started := time.Now()
	attempt, err := registrar.StartInstance(attemptCtx, locked.ID)
	elapsed := time.Since(started)
	attemptCancel()
	if !errors.Is(err, repository.ErrInstanceLocked) || attempt != nil || elapsed >= time.Second {
		t.Fatalf("NOWAIT contention did not skip promptly: attempt=%v error=%v elapsed=%s", attempt, err, elapsed)
	}
	if got := loadRegistrarInstance(t, db, locked.ID); !reflect.DeepEqual(got, locked) {
		t.Fatal("contending registration changed the committed parent")
	}
	assertNoRegistrarTasks(t, db, locked.ID)
	attempt, err = registrar.StartInstance(ctx, other.ID)
	if err != nil || attempt == nil || attempt.InstanceID != other.ID {
		t.Fatalf("locked candidate prevented another Instance from starting: attempt=%v error=%v", attempt, err)
	}
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	attempt, err = registrar.StartInstance(ctx, locked.ID)
	if err != nil || attempt == nil || attempt.InstanceID != locked.ID {
		t.Fatalf("rollback did not permit a later first start: attempt=%v error=%v", attempt, err)
	}
	if task := onlyRegistrarTask(t, db, locked.ID); task.AttemptNo != 1 || task.UsedAttempts != 1 {
		t.Fatalf("lock contention consumed execution attempts: %+v", task)
	}
}

func TestConcurrentRegistrarsGrantOnlyOneCommittedAttempt(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	instance := createRegistrarInstance(t, db)
	run := registrarTransactionRunner(t, db)
	ready, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseCommit := func() { releaseOnce.Do(func() { close(release) }) }
	first, err := NewRegistrar(func(ctx context.Context, fn func(repository.Repositories) error) error {
		return run(ctx, func(repos repository.Repositories) error {
			if err := fn(repos); err != nil {
				return err
			}
			// Keep the real registration writes uncommitted until the competing
			// Registrar has attempted the same parent on another connection.
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
		attempt, err := first.StartInstance(ctx, instance.ID)
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
		t.Fatalf("first Registrar returned before the commit barrier: attempt=%v error=%v", got.attempt, got.err)
	case <-ctx.Done():
		t.Fatalf("first Registrar did not reach the commit barrier: %v", ctx.Err())
	}
	contenderCtx, contenderCancel := context.WithTimeout(ctx, time.Second)
	contender, err := second.StartInstance(contenderCtx, instance.ID)
	contenderCancel()
	if !errors.Is(err, repository.ErrInstanceLocked) || contender != nil {
		t.Fatalf("competing Registrar did not skip the locked parent: attempt=%v error=%v", contender, err)
	}
	releaseCommit()
	winner := <-result
	if winner.err != nil || winner.attempt == nil {
		t.Fatalf("first Registrar did not confirm its new attempt: attempt=%v error=%v", winner.attempt, winner.err)
	}
	stored := loadRegistrarInstance(t, db, instance.ID)
	task := onlyRegistrarTask(t, db, instance.ID)
	if task.ID != winner.attempt.TaskID || task.AttemptNo != 1 || task.UsedAttempts != 1 ||
		stored.StartedAt == nil || !stored.StartedAt.Equal(winner.attempt.RegisteredAt) ||
		stored.DeadlineAt == nil || !stored.DeadlineAt.Equal(winner.attempt.DeadlineAt) {
		t.Fatal("the winning attempt does not match the sole committed registration")
	}
	repeated, err := second.StartInstance(ctx, instance.ID)
	if err != nil || repeated != nil {
		t.Fatalf("post-commit contender exposed the old attempt: attempt=%v error=%v", repeated, err)
	}
	if got := loadRegistrarInstance(t, db, instance.ID); !reflect.DeepEqual(got, stored) {
		t.Fatal("post-commit contention changed the first-start times or parent state")
	}
	if got := onlyRegistrarTask(t, db, instance.ID); !reflect.DeepEqual(got, task) {
		t.Fatal("post-commit contention changed or duplicated the first attempt")
	}
}

func TestRegistrarSingletonConflictRollsBackParent(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	instance := createRegistrarInstance(t, db)
	oldTask := model.JobTask{InstanceID: instance.ID, TaskType: model.TaskTypeOrdinary,
		Status: model.TaskFailed, AttemptNo: 7, UsedAttempts: 3}
	if err := db.Omit("created_at", "updated_at").Create(&oldTask).Error; err != nil {
		t.Fatal(err)
	}
	before := onlyRegistrarTask(t, db, instance.ID)
	attempt, err := newIntegrationRegistrar(t, db).StartInstance(context.Background(), instance.ID)
	if err == nil || attempt != nil {
		t.Fatalf("singleton conflict was not an error without an attempt: attempt=%v error=%v", attempt, err)
	}
	if got := loadRegistrarInstance(t, db, instance.ID); !reflect.DeepEqual(got, instance) {
		t.Fatalf("failed Task insertion left a partial parent update: before=%+v after=%+v", instance, got)
	}
	if got := onlyRegistrarTask(t, db, instance.ID); !reflect.DeepEqual(got, before) {
		t.Fatalf("singleton conflict replaced or changed the old Task: before=%+v after=%+v", before, got)
	}
}

func TestRegistrarSkipsIneligibleInstances(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	registrar := newIntegrationRegistrar(t, db)
	t.Run("missing", func(t *testing.T) {
		attempt, err := registrar.StartInstance(context.Background(), 99999)
		if err != nil || attempt != nil {
			t.Fatalf("missing Instance: attempt=%v error=%v", attempt, err)
		}
	})
	for _, state := range []model.InstanceStatus{model.InstanceRunning, model.InstanceStopping,
		model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled} {
		t.Run(fmt.Sprintf("state_%d", state), func(t *testing.T) {
			instance := createRegistrarInstance(t, db)
			if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("status", state).Error; err != nil {
				t.Fatal(err)
			}
			before := loadRegistrarInstance(t, db, instance.ID)
			attempt, err := registrar.StartInstance(context.Background(), instance.ID)
			if err != nil || attempt != nil {
				t.Fatalf("ineligible state: attempt=%v error=%v", attempt, err)
			}
			if got := loadRegistrarInstance(t, db, instance.ID); !reflect.DeepEqual(got, before) {
				t.Fatal("ineligible Instance was modified")
			}
			assertNoRegistrarTasks(t, db, instance.ID)
		})
	}
	t.Run("sharded", func(t *testing.T) {
		instance := createRegistrarInstance(t, db)
		config := instance.ExecutionConfig
		config.ExecutionMode, config.PostEnabled = model.ExecutionModeSharded, true
		if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("execution_config", config).Error; err != nil {
			t.Fatal(err)
		}
		attempt, err := registrar.StartInstance(context.Background(), instance.ID)
		if err != nil || attempt == nil || attempt.TaskType != model.TaskTypePrepare || !attempt.ExecutionConfig.PostEnabled {
			t.Fatalf("sharded registration: attempt=%v error=%v", attempt, err)
		}
		if got := loadRegistrarInstance(t, db, instance.ID); got.Status != model.InstanceRunning || got.StartedAt == nil || got.DeadlineAt == nil {
			t.Fatal("sharded registration did not atomically start the Instance")
		}
		var tasks []model.JobTask
		if err := db.Where("instance_id = ?", instance.ID).Find(&tasks).Error; err != nil {
			t.Fatal(err)
		}
		if len(tasks) != 1 || tasks[0].TaskType != model.TaskTypePrepare || tasks[0].Status != model.TaskRunning || tasks[0].AttemptNo != 1 || tasks[0].UsedAttempts != 1 {
			t.Fatalf("unexpected initial preparation: %+v", tasks)
		}
	})
}

func TestRegistrarRejectsInconsistentWaitingTimes(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	registrar := newIntegrationRegistrar(t, db)
	for _, column := range []string{"started_at", "deadline_at", "finished_at"} {
		t.Run(column, func(t *testing.T) {
			instance := createRegistrarInstance(t, db)
			if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).
				Update(column, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)).Error; err != nil {
				t.Fatal(err)
			}
			before := loadRegistrarInstance(t, db, instance.ID)
			attempt, err := registrar.StartInstance(context.Background(), instance.ID)
			if !errors.Is(err, ErrInvalidInstanceState) || attempt != nil {
				t.Fatalf("inconsistent waiting time: attempt=%v error=%v", attempt, err)
			}
			if got := loadRegistrarInstance(t, db, instance.ID); !reflect.DeepEqual(got, before) {
				t.Fatal("registration repaired inconsistent waiting data")
			}
			assertNoRegistrarTasks(t, db, instance.ID)
		})
	}
}

func TestRegistrarDoesNotExposeAttemptWhenCommitIsUnconfirmed(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed_%t", committed), func(t *testing.T) {
			db, _ := testutil.OpenMigratedPostgres(t)
			instance := createRegistrarInstance(t, db)
			run := registrarTransactionRunner(t, db)
			commitErr := errors.New("simulated commit confirmation failure")
			callbackCompleted := false
			registrar, err := NewRegistrar(func(ctx context.Context, fn func(repository.Repositories) error) error {
				err := run(ctx, func(repos repository.Repositories) error {
					if err := fn(repos); err != nil {
						return err
					}
					callbackCompleted = true
					if !committed {
						return commitErr
					}
					return nil
				})
				if err != nil {
					return err
				}
				// Simulate a lost confirmation after a real commit. This is fault
				// injection at the runner boundary, not a real network interruption.
				return commitErr
			})
			if err != nil {
				t.Fatal(err)
			}
			attempt, err := registrar.StartInstance(context.Background(), instance.ID)
			if !callbackCompleted || !errors.Is(err, commitErr) || attempt != nil {
				t.Fatalf("unconfirmed commit exposed registration: callback=%v attempt=%v error=%v", callbackCompleted, attempt, err)
			}
			if !committed {
				if got := loadRegistrarInstance(t, db, instance.ID); !reflect.DeepEqual(got, instance) {
					t.Fatal("forced rollback left parent changes")
				}
				assertNoRegistrarTasks(t, db, instance.ID)
				return
			}
			stored := loadRegistrarInstance(t, db, instance.ID)
			task := onlyRegistrarTask(t, db, instance.ID)
			if stored.Status != model.InstanceRunning || task.AttemptNo != 1 || task.UsedAttempts != 1 {
				t.Fatal("committed attempt did not remain durable after simulated confirmation loss")
			}
			attempt, err = newIntegrationRegistrar(t, db).StartInstance(context.Background(), instance.ID)
			if err != nil || attempt != nil {
				t.Fatalf("later registration resent an unconfirmed old attempt: attempt=%v error=%v", attempt, err)
			}
			if got := loadRegistrarInstance(t, db, instance.ID); !reflect.DeepEqual(got, stored) {
				t.Fatal("retry after confirmation loss reset parent execution times")
			}
			if got := onlyRegistrarTask(t, db, instance.ID); !reflect.DeepEqual(got, task) {
				t.Fatal("retry after confirmation loss changed or replaced the committed attempt")
			}
		})
	}
}

func createRegistrarInstance(t *testing.T, db *gorm.DB) model.JobInstance {
	t.Helper()
	requestID, operatorID := uuid.New(), int64(1)
	instance := model.JobInstance{JobID: 11, JobName: "frozen job", ExecutorGroupID: 17,
		TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: "system.cleanup",
			ExecutionMode: model.ExecutionModeNormal, Params: "frozen parameters", MaxRetries: 2,
			RetryIntervalSeconds: 7, TimeoutSeconds: 123}}
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := instances.Insert(context.Background(), &instance); err != nil || !created {
		t.Fatalf("create waiting Instance: created=%v error=%v", created, err)
	}
	return loadRegistrarInstance(t, db, instance.ID)
}

func registrarTransactionRunner(t *testing.T, db *gorm.DB) repository.TransactionRunner {
	t.Helper()
	run, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func newIntegrationRegistrar(t *testing.T, db *gorm.DB) *Registrar {
	t.Helper()
	registrar, err := NewRegistrar(registrarTransactionRunner(t, db))
	if err != nil {
		t.Fatal(err)
	}
	return registrar
}

func loadRegistrarInstance(t *testing.T, db *gorm.DB, id int64) model.JobInstance {
	t.Helper()
	var instance model.JobInstance
	if err := db.Where("id = ?", id).Take(&instance).Error; err != nil {
		t.Fatal(err)
	}
	return instance
}

func onlyRegistrarTask(t *testing.T, db *gorm.DB, instanceID int64) model.JobTask {
	t.Helper()
	var tasks []model.JobTask
	if err := db.Where("instance_id = ?", instanceID).Find(&tasks).Error; err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("Instance %d has %d Tasks, want exactly one", instanceID, len(tasks))
	}
	return tasks[0]
}

func assertNoRegistrarTasks(t *testing.T, db *gorm.DB, instanceID int64) {
	t.Helper()
	var count int64
	if err := db.Model(&model.JobTask{}).Where("instance_id = ?", instanceID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("Instance %d has %d unexpected Tasks", instanceID, count)
	}
}

func registrarDatabaseTime(t *testing.T, db *gorm.DB) time.Time {
	t.Helper()
	var now time.Time
	if err := db.Raw("SELECT clock_timestamp()").Scan(&now).Error; err != nil {
		t.Fatal(err)
	}
	return now
}
