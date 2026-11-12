//go:build integration

package taskrecovery_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/instancecheck"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/taskrecovery"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
)

func TestPreparationResultCommittedAfterRecoveryReadSurvivesSettlement(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	instances, tasks := recoveryRepositories(t, db)
	parent, original := recoveryFixture(t, db, instances, 1)
	parent.ExecutionConfig.ExecutionMode = model.ExecutionModeSharded
	if err := db.Model(&model.JobInstance{}).Where("id = ?", parent.ID).
		Update("execution_config", parent.ExecutionConfig).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobTask{}).Where("id = ?", original.ID).
		Update("task_type", model.TaskTypePrepare).Error; err != nil {
		t.Fatal(err)
	}
	var parentBefore model.JobInstance
	if err := db.First(&parentBefore, parent.ID).Error; err != nil {
		t.Fatal(err)
	}
	saver, err := taskresult.NewSaver(tasks)
	if err != nil {
		t.Fatal(err)
	}
	var saved model.JobTask
	store := &recoveryResultRaceStore{JobTaskRepository: tasks, afterRead: func(ctx context.Context) error {
		result, err := saver.Save(ctx, taskresult.Report{GroupID: parent.ExecutorGroupID, JobID: parent.JobID,
			InstanceID: parent.ID, TaskID: original.ID, AttemptNo: original.AttemptNo, ExecutorInstanceID: uuid.NewString(),
			Outcome: taskresult.OutcomeSuccess, PreparedItems: []json.RawMessage{json.RawMessage(`{"order_id":9007199254740993}`)}})
		if err != nil {
			return err
		}
		if !result.Applied {
			return fmt.Errorf("preparation result was not saved: %s", result.Reason)
		}
		return db.WithContext(ctx).First(&saved, original.ID).Error
	}}
	hints := 0
	settler, err := taskrecovery.NewSettler(instances, store, taskrecovery.WithInstanceHint(func(int64) { hints++ }))
	if err != nil {
		t.Fatal(err)
	}
	result, err := settler.SettleBatch(context.Background(), []repository.UnboundCandidate{recoveryCandidate(original)})
	if err != nil || result != (taskrecovery.BatchResult{Processed: 1}) || store.writes != 1 || hints != 0 {
		t.Fatalf("recovery overwrote a result committed after its read: result=%+v err=%v writes=%d hints=%d", result, err, store.writes, hints)
	}
	var after model.JobTask
	if err := db.First(&after, original.ID).Error; err != nil {
		t.Fatal(err)
	}
	if after.Status != model.TaskSucceeded || !reflect.DeepEqual(after, saved) {
		t.Fatal("unknown settlement changed the successful preparation list or attempt")
	}
	var parentAfter model.JobInstance
	if err := db.First(&parentAfter, parent.ID).Error; err != nil || !reflect.DeepEqual(parentBefore, parentAfter) {
		t.Fatalf("unknown settlement changed the parent: err=%v", err)
	}
	prepared, err := tasks.FindPreparationResult(context.Background(), parent.ID)
	if err != nil || prepared == nil {
		t.Fatalf("read the surviving preparation result: result=%+v err=%v", prepared, err)
	}
	run, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	generator, err := instancecheck.NewGenerator(run, nil)
	if err != nil {
		t.Fatal(err)
	}
	if generated, err := generator.Generate(context.Background(), parent.ID, *prepared); err != nil || !generated {
		t.Fatalf("surviving preparation could not advance: generated=%v err=%v", generated, err)
	}
	var shards []model.JobTask
	if err := db.Where("instance_id = ? AND task_type = ?", parent.ID, model.TaskTypeShard).Find(&shards).Error; err != nil || len(shards) != 1 {
		t.Fatalf("surviving preparation generated the wrong Task set: count=%d err=%v", len(shards), err)
	}
	var input struct {
		OrderID int64 `json:"order_id"`
	}
	if err := json.Unmarshal(shards[0].InputData, &input); err != nil || input.OrderID != 9007199254740993 {
		t.Fatalf("surviving preparation lost its fixed shard input: err=%v", err)
	}
}

// recoveryResultRaceStore exposes the real pre-result snapshot to settlement
// after a separate Saver has committed success. No database clock, target state
// or update result is fabricated; the hook controls only the component ordering.
type recoveryResultRaceStore struct {
	repository.JobTaskRepository
	afterRead func(context.Context) error
	writes    int
}

func (s *recoveryResultRaceStore) FindUnboundTargets(ctx context.Context, ids []int64) ([]repository.UnboundTarget, error) {
	targets, err := s.JobTaskRepository.FindUnboundTargets(ctx, ids)
	if err != nil {
		return nil, err
	}
	if len(targets) != 1 || targets[0].TaskType != model.TaskTypePrepare || targets[0].Status != model.TaskRunning {
		return nil, fmt.Errorf("expected one running preparation before its result")
	}
	if err := s.afterRead(ctx); err != nil {
		return nil, err
	}
	return targets, nil
}

func (s *recoveryResultRaceStore) SettleUnknown(ctx context.Context, write repository.UnknownSettlement) (*repository.SettledTask, error) {
	s.writes++
	return s.JobTaskRepository.SettleUnknown(ctx, write)
}
