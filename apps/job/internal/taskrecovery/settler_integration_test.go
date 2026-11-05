//go:build integration

package taskrecovery_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/dispatch"
	"github.com/tokyolab/dogx/apps/job/internal/instancecheck"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/taskrecovery"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
)

func TestUnknownObservationFeedsSharedRetryAndParentCompletion(t *testing.T) {
	for _, retries := range []int32{0, 1} {
		t.Run(fmt.Sprintf("max_retries_%d", retries), func(t *testing.T) {
			db, _ := testutil.OpenMigratedPostgres(t)
			instances, tasks := recoveryRepositories(t, db)
			parent, original := recoveryFixture(t, db, instances, retries)
			checker, err := instancecheck.NewChecker(instances)
			if err != nil {
				t.Fatal(err)
			}
			checks, err := instancecheck.NewQueue(checker, instancecheck.Config{Capacity: 8, Concurrency: 2, Timeout: 2 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if err := checks.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := checks.Drain(ctx); err != nil {
					t.Error(err)
				}
				if err := checks.Wait(ctx); err != nil {
					t.Error(err)
				}
			})
			hint := func(id int64) { checks.Hint(id) }
			saver, err := taskresult.NewSaver(tasks, taskresult.WithInstanceHint(hint))
			if err != nil {
				t.Fatal(err)
			}
			sender := &recoveryResultSender{saver: saver, executorID: uuid.New()}
			runner, err := repository.NewTransactionRunner(db)
			if err != nil {
				t.Fatal(err)
			}
			registrar, err := dispatch.NewRegistrar(runner)
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := dispatch.NewRuntime(instances, registrar, sender, tasks, dispatch.RuntimeConfig{
				Concurrency: 2, WaitingCapacity: 8, ScanInterval: 20 * time.Millisecond,
				QueryTimeout: time.Second, TotalTimeout: 10 * time.Second, BusyDelay: time.Second,
			}, hint)
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := runtime.Drain(ctx); err != nil {
					t.Error(err)
				}
				if err := runtime.Wait(ctx); err != nil {
					t.Error(err)
				}
			})
			settler, err := taskrecovery.NewSettler(instances, tasks, taskrecovery.WithInstanceHint(hint))
			if err != nil {
				t.Fatal(err)
			}
			// Two independent scanners share the durable backlog. Conditional
			// settlement must still feed one authorized retry or one final failure.
			for range 2 {
				scan, err := taskrecovery.NewRuntime(instances, tasks, settler, taskrecovery.Config{
					ScanInterval: 20 * time.Millisecond, PageSize: 2, RunTimeout: 5 * time.Second,
					OperationTimeout: time.Second, ObservationPeriod: taskrecovery.DefaultObservationPeriod,
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := scan.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					scan.Stop()
					if err := scan.Wait(context.Background()); err != nil {
						t.Error(err)
					}
				})
			}
			wantParent, wantTask, reason := model.InstanceFailed, model.TaskFailed, model.EndReasonResultUnknown
			if retries > 0 {
				wantParent, wantTask, reason = model.InstanceSucceeded, model.TaskSucceeded, model.EndReasonSuccess
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var stored model.JobInstance
			for {
				if err := db.WithContext(ctx).First(&stored, parent.ID).Error; err != nil {
					t.Fatal(err)
				}
				if stored.Status == wantParent {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("settled attempt did not reach its expected parent outcome")
				case <-time.After(10 * time.Millisecond):
				}
			}
			var current []model.JobTask
			if err := db.Where("instance_id = ?", parent.ID).Find(&current).Error; err != nil {
				t.Fatal(err)
			}
			if len(current) != 1 {
				t.Fatal("unknown settlement recreated the ordinary Task")
			}
			task := current[0]
			if task.ID != original.ID || task.Status != wantTask || task.AttemptNo != int64(1+retries) || task.UsedAttempts != 1+retries ||
				task.EndReason == nil || *task.EndReason != reason || task.StartedAt == nil || !task.StartedAt.Equal(*original.StartedAt) ||
				!stored.DeadlineAt.Equal(*parent.DeadlineAt) || stored.ExecutionConfig != parent.ExecutionConfig || sender.calls.Load() != retries {
				t.Fatal("settlement changed lifetime, budget, original identity or the expected final reason")
			}
			if retries == 0 && (stored.EndReason == nil || *stored.EndReason != model.EndReasonExecutionFailed) {
				t.Fatal("parent final failure lost its stable reason while the Task retained result_unknown")
			}
			if applied, err := tasks.BindExecutor(ctx, repository.ExecutorBinding{TaskID: original.ID, InstanceID: parent.ID,
				AttemptNo: original.AttemptNo, ExecutorInstanceID: sender.executorID, ExecutorAddress: "127.0.0.1:19001"}); err != nil || applied {
				t.Fatalf("late ACK reopened the settled attempt: applied=%v err=%v", applied, err)
			}
			late := taskresult.Report{GroupID: parent.ExecutorGroupID, JobID: parent.JobID, InstanceID: parent.ID,
				TaskID: original.ID, AttemptNo: original.AttemptNo, ExecutorInstanceID: sender.executorID.String(), Outcome: taskresult.OutcomeSuccess}
			if result, err := saver.Save(ctx, late); err != nil || result.Applied {
				t.Fatalf("late result reopened the settled attempt: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestUnknownSettlementCannotAuthorizeRetryAfterConcurrentParentStop(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	instances, tasks := recoveryRepositories(t, db)
	parent, original := recoveryFixture(t, db, instances, 1)
	store := &recoveryStoppingStore{JobTaskRepository: tasks, db: db, parentID: parent.ID}
	settler, err := taskrecovery.NewSettler(instances, store)
	if err != nil {
		t.Fatal(err)
	}
	result, err := settler.SettleBatch(context.Background(), []repository.UnboundCandidate{recoveryCandidate(original)})
	if err != nil || result.Applied != 1 || result.Processed != 1 {
		t.Fatalf("interleaved settlement: result=%+v err=%v", result, err)
	}
	var task model.JobTask
	if err := db.First(&task, original.ID).Error; err != nil {
		t.Fatal(err)
	}
	if task.Status != model.TaskRetryWaiting {
		t.Fatal("test did not produce the allowed interleaved retry candidate")
	}
	// Make the child due so only the stopped parent can prevent registration.
	now, err := instances.DatabaseTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	due := now.Add(-time.Second)
	if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Update("next_dispatch_at", due).Error; err != nil {
		t.Fatal(err)
	}
	task.NextDispatchAt = &due
	runner, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	registrar, err := dispatch.NewRegistrar(runner)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := registrar.RetryOrdinary(context.Background(), repository.RetryCandidate{
		TaskID: task.ID, InstanceID: parent.ID, AttemptNo: task.AttemptNo, NextDispatchAt: *task.NextDispatchAt,
	})
	if err != nil || attempt != nil {
		t.Fatalf("stopping parent granted retry: attempt=%+v err=%v", attempt, err)
	}
	var stored model.JobInstance
	if err := db.First(&stored, parent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.InstanceStopping || !stored.DeadlineAt.Equal(*parent.DeadlineAt) {
		t.Fatal("child settlement or retry registration restored the stopped parent")
	}
}

func recoveryRepositories(t *testing.T, db *gorm.DB) (repository.JobInstanceRepository, repository.JobTaskRepository) {
	t.Helper()
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := repository.NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	return instances, tasks
}

func recoveryFixture(t *testing.T, db *gorm.DB, instances repository.JobInstanceRepository, retries int32) (model.JobInstance, model.JobTask) {
	t.Helper()
	now, err := instances.DatabaseTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	started, deadline := now.Add(-time.Minute), now.Add(4*time.Minute)
	requestID := uuid.New()
	operatorID := int64(42)
	parent := model.JobInstance{JobID: 11, JobName: "unbound fixture", ExecutorGroupID: 17,
		TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID, Status: model.InstanceRunning,
		StartedAt: &started, DeadlineAt: &deadline, CreatedAt: started, UpdatedAt: started,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: "test.recovery", ExecutionMode: model.ExecutionModeNormal,
			Params: "frozen recovery input", MaxRetries: retries, RetryIntervalSeconds: 1, TimeoutSeconds: 300}}
	if err := db.Create(&parent).Error; err != nil {
		t.Fatal(err)
	}
	task := model.JobTask{InstanceID: parent.ID, TaskType: model.TaskTypeOrdinary, Status: model.TaskRunning,
		AttemptNo: 1, UsedAttempts: 1, AttemptRegisteredAt: &started, StartedAt: &started, CreatedAt: started, UpdatedAt: started}
	if err := db.Create(&task).Error; err != nil {
		t.Fatal(err)
	}
	return parent, task
}

func recoveryCandidate(task model.JobTask) repository.UnboundCandidate {
	return repository.UnboundCandidate{TaskID: task.ID, InstanceID: task.InstanceID, AttemptNo: task.AttemptNo, AttemptRegisteredAt: *task.AttemptRegisteredAt}
}

// recoveryResultSender substitutes only the transport/business boundary. It uses
// the real result Saver before returning its ACK to exercise result-first ordering.
type recoveryResultSender struct {
	saver      *taskresult.Saver
	executorID uuid.UUID
	calls      atomic.Int32
}

func (s *recoveryResultSender) Dispatch(ctx context.Context, attempt dispatch.RegisteredAttempt) (dispatch.Response, error) {
	s.calls.Add(1)
	result, err := s.saver.Save(ctx, taskresult.Report{GroupID: attempt.ExecutorGroupID, JobID: attempt.JobID,
		InstanceID: attempt.InstanceID, TaskID: attempt.TaskID, AttemptNo: attempt.AttemptNo,
		ExecutorInstanceID: s.executorID.String(), Outcome: taskresult.OutcomeSuccess, Summary: "retry completed"})
	if err != nil {
		return dispatch.Response{}, err
	}
	if !result.Applied {
		return dispatch.Response{}, fmt.Errorf("retry result was not applied")
	}
	return dispatch.Response{Outcome: dispatch.OutcomeAccepted, ExecutorInstanceID: s.executorID, ExecutorAddress: "127.0.0.1:19001"}, nil
}

type recoveryStoppingStore struct {
	repository.JobTaskRepository
	db       *gorm.DB
	parentID int64
}

func (s *recoveryStoppingStore) FindUnboundTargets(ctx context.Context, ids []int64) ([]repository.UnboundTarget, error) {
	targets, err := s.JobTaskRepository.FindUnboundTargets(ctx, ids)
	if err != nil {
		return nil, err
	}
	if err := s.db.WithContext(ctx).Model(&model.JobInstance{}).Where("id = ?", s.parentID).Update("status", model.InstanceStopping).Error; err != nil {
		return nil, err
	}
	return targets, nil
}
