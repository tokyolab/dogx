//go:build integration

package taskresult

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/dispatch"
	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
)

func TestShardedSaverRechecksFactsChangedAfterItsLookup(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	for _, scenario := range []struct {
		name  string
		stage model.TaskType
	}{
		{"preparation_already_succeeded", model.TaskTypePrepare},
		{"shard_has_new_attempt", model.TaskTypeShard},
		{"post_canceled_with_parent", model.TaskTypePost},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			fixture := currentStageSaverFixture(t, db, scenario.stage)
			tasks, err := repository.NewJobTaskRepository(db)
			if err != nil {
				t.Fatal(err)
			}
			run, err := repository.NewTransactionRunner(db)
			if err != nil {
				t.Fatal(err)
			}
			var winner model.JobTask
			var winningParent model.JobInstance
			store := &stageInterleavingStore{JobTaskRepository: tasks}
			store.afterRead = func(ctx context.Context) {
				// Commit a competing operation after the Saver has read RUNNING.
				// The original snapshot must not authorize overwriting its successor.
				switch scenario.stage {
				case model.TaskTypePrepare:
					first := fixture.report
					first.PreparedItems = []json.RawMessage{json.RawMessage(`{"winner":9007199254740993}`), json.RawMessage(`null`)}
					if saved, err := integrationSaver(t, db).Save(ctx, first); err != nil || !saved.Applied {
						t.Fatalf("competing preparation result: result=%+v err=%v", saved, err)
					}
				case model.TaskTypeShard:
					failed := fixture.report
					failed.Outcome, failed.Data = OutcomeFailure, nil
					if saved, err := integrationSaver(t, db).Save(ctx, failed); err != nil || !saved.Applied {
						t.Fatalf("failure before replacement attempt: result=%+v err=%v", saved, err)
					}
					// Advance only the fixture's retry due time. The real Registrar
					// still checks current database time and the original parent deadline.
					due := saverDatabaseTime(t, db).Add(-time.Second)
					if err := db.WithContext(ctx).Model(&model.JobTask{}).Where("id = ?", fixture.task.ID).
						Update("next_dispatch_at", due).Error; err != nil {
						t.Fatal(err)
					}
					registrar, err := dispatch.NewRegistrar(run)
					if err != nil {
						t.Fatal(err)
					}
					attempt, err := registrar.RegisterTask(ctx, repository.DispatchCandidate{TaskID: fixture.task.ID,
						InstanceID: fixture.instance.ID, TaskType: scenario.stage, Status: model.TaskRetryWaiting,
						AttemptNo: fixture.task.AttemptNo, NextDispatchAt: due})
					if err != nil || attempt == nil || attempt.AttemptNo != fixture.task.AttemptNo+1 ||
						!reflect.DeepEqual(attempt.InputData, fixture.task.InputData) {
						t.Fatalf("replacement stage attempt: attempt=%+v err=%v", attempt, err)
					}
				case model.TaskTypePost:
					controller := stageStopController(t, run)
					if decision, err := controller.RequestManual(ctx, fixture.instance.ID); err != nil || !decision.New {
						t.Fatalf("stop Post parent: decision=%+v err=%v", decision, err)
					}
					finishStageStop(t, ctx, controller, fixture.instance.ID)
				}
				winner = loadSaverTask(t, db, fixture.task.ID)
				winningParent = loadSaverInstance(t, db, fixture.instance.ID)
			}
			var hints []int64
			saver, err := NewSaver(store, WithSaveTimeout(10*time.Second), WithInstanceHint(func(id int64) {
				hints = append(hints, id)
			}))
			if err != nil {
				t.Fatal(err)
			}
			result, err := saver.Save(context.Background(), fixture.report)
			if err != nil || result.Applied || result.Reason != "no_match" || store.reads != 1 || len(store.writes) != 1 {
				t.Fatalf("stale lookup result: result=%+v err=%v reads=%d writes=%d", result, err, store.reads, len(store.writes))
			}
			if store.writes[0].AttemptNo != fixture.report.AttemptNo || store.writes[0].TaskID != fixture.task.ID {
				t.Fatal("Saver replaced the original report identity with a newer attempt")
			}
			// An obsolete verified report may hint a fresh parent check, but it
			// cannot carry old facts into a parent transition or create a new attempt.
			if !reflect.DeepEqual(hints, []int64{fixture.instance.ID}) {
				t.Fatalf("obsolete result did not produce exactly one verified-parent hint: %v", hints)
			}
			binding := repository.ExecutorBinding{TaskID: fixture.task.ID, InstanceID: fixture.instance.ID,
				AttemptNo: fixture.report.AttemptNo, ExecutorInstanceID: uuid.MustParse(fixture.report.ExecutorInstanceID),
				ExecutorAddress: "127.0.0.1:19002"}
			if applied, err := tasks.BindExecutor(context.Background(), binding); err != nil || applied {
				t.Fatalf("old ACK changed the winning stage fact: applied=%v err=%v", applied, err)
			}
			if got := loadSaverTask(t, db, fixture.task.ID); !reflect.DeepEqual(got, winner) ||
				!reflect.DeepEqual(got.InputData, fixture.task.InputData) {
				t.Fatal("old result or ACK changed fixed input, a new attempt, or the winning output")
			}
			if got := loadSaverInstance(t, db, fixture.instance.ID); !reflect.DeepEqual(got, winningParent) {
				t.Fatal("old result or ACK changed its parent's state or stop reason")
			}
			if scenario.stage == model.TaskTypePrepare {
				var items []json.RawMessage
				if err := json.Unmarshal(winner.ResultData, &items); err != nil || winner.Status != model.TaskSucceeded ||
					len(items) != 2 || string(items[1]) != "null" {
					t.Fatalf("winning preparation list was not preserved: data=%s err=%v", winner.ResultData, err)
				}
				var first struct {
					Winner int64 `json:"winner"`
				}
				if err := json.Unmarshal(items[0], &first); err != nil || first.Winner != 9007199254740993 {
					t.Fatalf("winning preparation input changed: data=%s err=%v", items[0], err)
				}
			}
		})
	}
}

func TestStoppingShardResultCannotAuthorizeAnotherAttempt(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	fixture := currentStageSaverFixture(t, db, model.TaskTypeShard)
	run, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	controller := stageStopController(t, run)
	ctx := context.Background()
	if decision, err := controller.RequestManual(ctx, fixture.instance.ID); err != nil || !decision.New {
		t.Fatalf("stop before result: decision=%+v err=%v", decision, err)
	}
	stopping := loadSaverInstance(t, db, fixture.instance.ID)
	failed := fixture.report
	failed.Outcome, failed.Data = OutcomeFailure, nil
	saver := integrationSaver(t, db)
	if saved, err := saver.Save(ctx, failed); err != nil || !saved.Applied {
		t.Fatalf("current result was rejected merely because the parent is stopping: result=%+v err=%v", saved, err)
	}
	// Make the stored retry due so the later registration is rejected by the
	// stop decision, rather than coincidentally by its retry delay.
	if err := db.Model(&model.JobTask{}).Where("id = ?", fixture.task.ID).
		Update("next_dispatch_at", saverDatabaseTime(t, db).Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	retrying := loadSaverTask(t, db, fixture.task.ID)
	if retrying.Status != model.TaskRetryWaiting || retrying.NextDispatchAt == nil ||
		!reflect.DeepEqual(retrying.InputData, fixture.task.InputData) {
		t.Fatalf("effective failure lost its task facts: %+v", retrying)
	}
	if got := loadSaverInstance(t, db, fixture.instance.ID); !reflect.DeepEqual(got, stopping) {
		t.Fatal("child failure altered the committed stop decision")
	}
	registrar, err := dispatch.NewRegistrar(run)
	if err != nil {
		t.Fatal(err)
	}
	if attempt, err := registrar.RegisterTask(ctx, repository.DispatchCandidate{TaskID: retrying.ID,
		InstanceID: retrying.InstanceID, TaskType: retrying.TaskType, Status: retrying.Status,
		AttemptNo: retrying.AttemptNo, NextDispatchAt: *retrying.NextDispatchAt}); err != nil || attempt != nil {
		t.Fatalf("stopping parent granted another stage attempt: attempt=%+v err=%v", attempt, err)
	}
	if got := loadSaverTask(t, db, retrying.ID); !reflect.DeepEqual(got, retrying) {
		t.Fatal("rejected authorization modified the pending failure")
	}
	finishStageStop(t, ctx, controller, fixture.instance.ID)
	canceled := loadSaverTask(t, db, fixture.task.ID)
	if canceled.Status != model.TaskCanceled || canceled.AttemptNo != fixture.task.AttemptNo ||
		canceled.UsedAttempts != fixture.task.UsedAttempts || !reflect.DeepEqual(canceled.InputData, fixture.task.InputData) {
		t.Fatalf("cancellation changed fixed stage facts or authorized another attempt: %+v", canceled)
	}
	if saved, err := saver.Save(ctx, fixture.report); err != nil || saved.Applied || saved.Reason != "task_closed" {
		t.Fatalf("late success reopened a canceled shard: result=%+v err=%v", saved, err)
	}
	if got := loadSaverTask(t, db, fixture.task.ID); !reflect.DeepEqual(got, canceled) {
		t.Fatal("late shard success overwrote cancellation")
	}
	parent := loadSaverInstance(t, db, fixture.instance.ID)
	if parent.Status != model.InstanceCanceled || parent.EndReason == nil || *parent.EndReason != model.EndReasonManualStop ||
		!reflect.DeepEqual(parent.DeadlineAt, fixture.instance.DeadlineAt) {
		t.Fatalf("late stage result changed the stop outcome or deadline: %+v", parent)
	}
}

func currentStageSaverFixture(t *testing.T, db *gorm.DB, stage model.TaskType) saverIntegrationFixture {
	t.Helper()
	now := saverDatabaseTime(t, db)
	fixture := newSaverIntegrationFixture(t, db, func(parent *model.JobInstance, task *model.JobTask) {
		parent.ExecutionConfig.ExecutionMode = model.ExecutionModeSharded
		parent.ExecutionConfig.PostEnabled = stage == model.TaskTypePost
		started, deadline := now.Add(-time.Minute), now.Add(10*time.Minute)
		parent.StartedAt, parent.DeadlineAt = &started, &deadline
		task.TaskType, task.StartedAt, task.AttemptRegisteredAt = stage, &started, &started
		switch stage {
		case model.TaskTypeShard:
			task.InputData = model.TaskData(`{"order_id":9007199254740993}`)
		case model.TaskTypePost:
			task.InputData = model.TaskData(`{"total":2,"succeeded":1,"failed":1}`)
		}
	})
	if stage == model.TaskTypePrepare {
		fixture.report.Data = nil
		fixture.report.PreparedItems = []json.RawMessage{json.RawMessage(`{"late":true}`)}
	}
	return fixture
}

func stageStopController(t *testing.T, run repository.TransactionRunner) *instancestop.Controller {
	t.Helper()
	controller, err := instancestop.NewController(run)
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func finishStageStop(t *testing.T, ctx context.Context, controller *instancestop.Controller, id int64) {
	t.Helper()
	if batch, err := controller.ProcessBatch(ctx, id); err != nil || batch.Canceled != 1 || !batch.More || batch.Completed {
		t.Fatalf("cancel stage: result=%+v err=%v", batch, err)
	}
	if batch, err := controller.ProcessBatch(ctx, id); err != nil || !batch.Completed || batch.Status != model.InstanceCanceled {
		t.Fatalf("finish stage parent: result=%+v err=%v", batch, err)
	}
}

// stageInterleavingStore retains the real lookup and write while inserting an
// independently committed contender between them. It never alters the snapshot
// or a write's original identity, and is used synchronously by one Saver call.
type stageInterleavingStore struct {
	repository.JobTaskRepository
	afterRead func(context.Context)
	reads     int
	writes    []repository.TaskResultWrite
}

func (s *stageInterleavingStore) FindResultTarget(ctx context.Context, id int64) (*repository.TaskResultTarget, error) {
	target, err := s.JobTaskRepository.FindResultTarget(ctx, id)
	if err != nil || target == nil {
		return target, err
	}
	s.reads++
	if s.reads != 1 {
		return nil, fmt.Errorf("unexpected stage lookup count: %d", s.reads)
	}
	s.afterRead(ctx)
	return target, nil
}

func (s *stageInterleavingStore) SaveResult(ctx context.Context, write repository.TaskResultWrite) (bool, error) {
	s.writes = append(s.writes, write)
	return s.JobTaskRepository.SaveResult(ctx, write)
}
