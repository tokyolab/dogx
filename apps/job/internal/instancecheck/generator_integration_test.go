//go:build integration

package instancecheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/dispatch"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
)

func TestPreparationGeneratesAndRegistersImmutableShards(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	items := []json.RawMessage{json.RawMessage(`null`), json.RawMessage(`9007199254740993`), json.RawMessage(`{"ids":[1,2]}`)}
	parent, prepared := generationFixture(t, db, items)
	tasks, _ := repository.NewJobTaskRepository(db)
	run, _ := repository.NewTransactionRunner(db)
	hints := 0
	generator, _ := NewGenerator(run, func(id int64) {
		if id == parent.ID {
			hints++
		}
	})
	if created, err := generator.Generate(context.Background(), parent.ID, prepared); err != nil || !created || hints != 1 {
		t.Fatalf("generate: created=%v hints=%d err=%v", created, hints, err)
	}
	var stored []model.JobTask
	if err := db.Where("instance_id = ? AND task_type = ?", parent.ID, model.TaskTypeShard).Order("id").Find(&stored).Error; err != nil || len(stored) != len(items) {
		t.Fatalf("read complete shard set: count=%d err=%v", len(stored), err)
	}
	registrar, _ := dispatch.NewRegistrar(run)
	instances, _ := repository.NewJobInstanceRepository(db)
	scanNow, err := instances.DatabaseTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := tasks.ListDispatchCandidates(context.Background(), scanNow, nil, 10)
	if err != nil || len(candidates) != len(items) {
		t.Fatalf("discover generated shards: %d %v", len(candidates), err)
	}
	saver, _ := taskresult.NewSaver(tasks)
	for i, candidate := range candidates {
		before := stored[i]
		if before.Status != model.TaskWaiting || before.AttemptNo != 0 || before.UsedAttempts != 0 || before.StartedAt != nil || before.AttemptRegisteredAt != nil {
			t.Fatal("generation authorized an attempt before dispatch capacity")
		}
		attempt, err := registrar.RegisterTask(context.Background(), candidate)
		if err != nil || attempt == nil || attempt.TaskType != model.TaskTypeShard || attempt.AttemptNo != 1 || string(attempt.InputData) != string(before.InputData) {
			t.Fatalf("first shard registration: %+v %v", attempt, err)
		}
		report := taskresult.Report{GroupID: parent.ExecutorGroupID, JobID: parent.JobID, InstanceID: parent.ID,
			TaskID: attempt.TaskID, AttemptNo: 1, ExecutorInstanceID: uuid.NewString(), Outcome: taskresult.OutcomeSuccess, Data: json.RawMessage(`{"done":true}`)}
		if result, err := saver.Save(context.Background(), report); err != nil || !result.Applied {
			t.Fatalf("save shard: %+v %v", result, err)
		}
		var after model.JobTask
		if err := db.Where("id = ?", attempt.TaskID).Take(&after).Error; err != nil {
			t.Fatal(err)
		}
		if after.Status != model.TaskSucceeded || string(after.InputData) != string(before.InputData) {
			t.Fatal("result changed fixed shard input")
		}
		if attempt, err := registrar.RegisterTask(context.Background(), candidate); err != nil || attempt != nil {
			t.Fatalf("stale candidate replayed: %+v %v", attempt, err)
		}
	}
	if created, err := generator.Generate(context.Background(), parent.ID, prepared); err != nil || created || hints != 1 {
		t.Fatalf("repeat generation duplicated work: %v %v hints=%d", created, err, hints)
	}
}

func TestGenerationWholeSetCommitRollbackAndLostConfirmation(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	items := make([]json.RawMessage, 500)
	for i := range items {
		items[i] = json.RawMessage(fmt.Sprintf(`{"item":%d}`, i))
	}
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed_%v", committed), func(t *testing.T) {
			parent, prepared := generationFixture(t, db, items)
			run, _ := repository.NewTransactionRunner(db)
			cause := errors.New("synthetic commit boundary failure")
			hints := 0
			generator, _ := NewGenerator(func(ctx context.Context, fn func(repository.Repositories) error) error {
				err := run(ctx, func(repos repository.Repositories) error {
					if err := fn(repos); err != nil {
						return err
					}
					var visible int64
					if err := db.WithContext(ctx).Model(&model.JobTask{}).Where("instance_id = ? AND task_type = ?", parent.ID, model.TaskTypeShard).Count(&visible).Error; err != nil {
						return err
					}
					if visible != 0 {
						return errors.New("uncommitted shards became visible")
					}
					if !committed {
						return cause
					}
					return nil
				})
				if err != nil {
					return err
				}
				return cause
			}, func(int64) { hints++ })
			if created, err := generator.Generate(context.Background(), parent.ID, prepared); created || !errors.Is(err, cause) || hints != 0 {
				t.Fatalf("uncertain generation: %v %v hints=%d", created, err, hints)
			}
			var count int64
			if err := db.Model(&model.JobTask{}).Where("instance_id = ? AND task_type = ?", parent.ID, model.TaskTypeShard).Count(&count).Error; err != nil {
				t.Fatal(err)
			}
			want := int64(0)
			if committed {
				want = 500
			}
			if count != want {
				t.Fatalf("partial shard set: %d want %d", count, want)
			}
			generator, _ = NewGenerator(run, func(int64) { hints++ })
			if created, err := generator.Generate(context.Background(), parent.ID, prepared); err != nil || created == committed {
				t.Fatalf("generation recovery: %v %v", created, err)
			}
			if err := db.Model(&model.JobTask{}).Where("instance_id = ? AND task_type = ?", parent.ID, model.TaskTypeShard).Count(&count).Error; err != nil || count != 500 {
				t.Fatalf("recovery duplicated/lost shards: %d %v", count, err)
			}
		})
	}
}

func TestGenerationNOWAITStopDeadlineAndConcurrentCallers(t *testing.T) {
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	parent, prepared := generationFixture(t, db, []json.RawMessage{json.RawMessage(`1`), json.RawMessage(`2`)})
	run, _ := repository.NewTransactionRunner(db)
	var hints atomic.Int32
	generator, _ := NewGenerator(run, func(int64) { hints.Add(1) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	holder, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback() }()
	var id int64
	// Reproduce a competing stop decision holding only the parent primary key.
	if err := holder.QueryRowContext(ctx, "SELECT id FROM job_instance WHERE id = $1 FOR UPDATE", parent.ID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	attemptCtx, attemptCancel := context.WithTimeout(ctx, time.Second)
	created, err := generator.Generate(attemptCtx, parent.ID, prepared)
	attemptCancel()
	if created || !errors.Is(err, repository.ErrInstanceLocked) {
		t.Fatalf("generation waited for parent: %v %v", created, err)
	}
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	var createdCount atomic.Int32
	for range 6 {
		workers.Go(func() {
			created, err := generator.Generate(ctx, parent.ID, prepared)
			if err != nil && !errors.Is(err, repository.ErrInstanceLocked) {
				t.Errorf("concurrent generation: %v", err)
			}
			if created {
				createdCount.Add(1)
			}
		})
	}
	workers.Wait()
	if createdCount.Load() != 1 || hints.Load() != 1 {
		t.Fatalf("duplicate generation: commits=%d hints=%d", createdCount.Load(), hints.Load())
	}
	for _, stop := range []bool{false, true} {
		parent, prepared := generationFixture(t, db, []json.RawMessage{json.RawMessage(`1`)})
		fields := map[string]any{"started_at": time.Now().Add(-2 * time.Hour), "deadline_at": time.Now().Add(-time.Hour)}
		if stop {
			fields = map[string]any{"status": model.InstanceStopping, "end_reason": model.EndReasonManualStop}
		}
		if err := db.Model(&model.JobInstance{}).Where("id = ?", parent.ID).Updates(fields).Error; err != nil {
			t.Fatal(err)
		}
		if created, err := generator.Generate(ctx, parent.ID, prepared); err != nil || created {
			t.Fatalf("stop/deadline authorized shards: %v %v", created, err)
		}
	}
}

func generationFixture(t *testing.T, db *gorm.DB, items []json.RawMessage) (model.JobInstance, repository.PreparationResult) {
	t.Helper()
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := repository.NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	run, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	requestID, operatorID := uuid.New(), int64(1)
	parent := model.JobInstance{JobID: 11, JobName: "sharded test", ExecutorGroupID: 17, TriggerType: model.TriggerManual,
		RequestID: &requestID, TriggeredBy: &operatorID,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: "test.shards", ExecutionMode: model.ExecutionModeSharded,
			MaxRetries: 2, RetryIntervalSeconds: 1, TimeoutSeconds: 900}}
	if _, created, err := instances.Insert(context.Background(), &parent); err != nil || !created {
		t.Fatalf("create sharded parent: %v %v", created, err)
	}
	registrar, _ := dispatch.NewRegistrar(run)
	attempt, err := registrar.StartInstance(context.Background(), parent.ID)
	if err != nil || attempt == nil || attempt.TaskType != model.TaskTypePrepare {
		t.Fatalf("start preparation: %+v %v", attempt, err)
	}
	saver, _ := taskresult.NewSaver(tasks)
	result, err := saver.Save(context.Background(), taskresult.Report{GroupID: parent.ExecutorGroupID, JobID: parent.JobID, InstanceID: parent.ID,
		TaskID: attempt.TaskID, AttemptNo: attempt.AttemptNo, ExecutorInstanceID: uuid.NewString(), Outcome: taskresult.OutcomeSuccess, PreparedItems: items})
	if err != nil || !result.Applied {
		t.Fatalf("save preparation: %+v %v", result, err)
	}
	prepared, err := tasks.FindPreparationResult(context.Background(), parent.ID)
	if err != nil || prepared == nil || prepared.Status != model.TaskSucceeded {
		t.Fatalf("read committed preparation: %+v %v", prepared, err)
	}
	return parent, *prepared
}
