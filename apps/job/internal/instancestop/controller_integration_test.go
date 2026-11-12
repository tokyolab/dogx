//go:build integration

package instancestop_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/dispatch"
	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
)

func TestManualStopKeepsDecisionSeparateFromBoundedConvergence(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	controller, instances, tasks, _ := stopDependencies(t, db)
	parent := stopParent(t, db, instances, model.InstanceRunning)
	// An expired manual stop still records the user's action. Mixed Task stages
	// deliberately exercise the shared cancellation protocol without dispatching.
	deadline := parent.StartedAt.Add(30 * time.Second)
	if err := db.Model(&parent).Update("deadline_at", deadline).Error; err != nil {
		t.Fatal(err)
	}
	parent.DeadlineAt = &deadline
	identity, address, summary := uuid.New(), "127.0.0.1:19001", "previous attempt"
	finished := *parent.StartedAt
	var before []model.JobTask
	for index, entry := range []struct {
		kind   model.TaskType
		status model.TaskStatus
	}{
		{model.TaskTypeOrdinary, model.TaskWaiting}, {model.TaskTypePrepare, model.TaskRunning},
		{model.TaskTypeShard, model.TaskRetryWaiting}, {model.TaskTypePost, model.TaskRunning},
		{model.TaskTypeShard, model.TaskWaiting}, {model.TaskTypeShard, model.TaskSucceeded},
		{model.TaskTypeShard, model.TaskFailed}, {model.TaskTypeShard, model.TaskCanceled},
	} {
		row := model.JobTask{InstanceID: parent.ID, TaskType: entry.kind, Status: entry.status,
			AttemptNo: int64(index + 1), UsedAttempts: 1, StartedAt: parent.StartedAt, AttemptRegisteredAt: parent.StartedAt,
			NextDispatchAt: parent.StartedAt, ExecutorInstanceID: &identity, ExecutorAddress: &address,
			ResultData: model.TaskData(`{"old":true}`), ResultSummary: &summary,
			CreatedAt: *parent.StartedAt, UpdatedAt: *parent.StartedAt}
		if entry.status >= model.TaskSucceeded {
			reason := model.EndReasonExecutionFailed
			row.EndReason, row.FinishedAt = &reason, &finished
		}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Where("instance_id = ?", parent.ID).Order("id").Find(&before).Error; err != nil {
		t.Fatal(err)
	}
	decision, err := controller.RequestManual(context.Background(), parent.ID)
	if err != nil || !decision.New || decision.Status != model.InstanceStopping || decision.EndReason == nil || *decision.EndReason != model.EndReasonManualStop {
		t.Fatalf("manual decision: %+v err=%v", decision, err)
	}
	stopping := readStopParent(t, db, parent.ID)
	if stopping.FinishedAt != nil || !stopping.StartedAt.Equal(*parent.StartedAt) || !stopping.DeadlineAt.Equal(*parent.DeadlineAt) || stopping.ExecutionConfig != parent.ExecutionConfig {
		t.Fatal("stop decision changed lifetime or pretended database convergence was complete")
	}
	var untouched []model.JobTask
	if err := db.Where("instance_id = ?", parent.ID).Order("id").Find(&untouched).Error; err != nil || !reflect.DeepEqual(before, untouched) {
		t.Fatal("decision transaction also changed child rows")
	}
	repeated, err := controller.RequestManual(context.Background(), parent.ID)
	if err != nil || repeated.New || repeated.Status != model.InstanceStopping || !reflect.DeepEqual(stopping, readStopParent(t, db, parent.ID)) {
		t.Fatal("repeated stopping request changed the frozen decision")
	}
	for index, want := range []int64{2, 2, 1, 0} {
		batch, err := controller.ProcessBatch(context.Background(), parent.ID)
		if err != nil || batch.Canceled != want || batch.More != (want > 0) || batch.Completed != (want == 0) {
			t.Fatalf("bounded batch %d: %+v err=%v", index, batch, err)
		}
		current := readStopParent(t, db, parent.ID)
		if index < 3 && (current.Status != model.InstanceStopping || current.FinishedAt != nil) {
			t.Fatal("parent finished before an empty child query confirmed convergence")
		}
		if !reflect.DeepEqual(current.EndReason, stopping.EndReason) || !reflect.DeepEqual(current.ResultSummary, stopping.ResultSummary) {
			t.Fatal("child batches changed the frozen parent reason or summary")
		}
	}
	ended := readStopParent(t, db, parent.ID)
	if ended.Status != model.InstanceCanceled || ended.FinishedAt == nil || !ended.FinishedAt.After(stopping.UpdatedAt) {
		t.Fatal("manual stop did not acquire a final database completion time")
	}
	var after []model.JobTask
	if err := db.Where("instance_id = ?", parent.ID).Order("id").Find(&after).Error; err != nil {
		t.Fatal(err)
	}
	for index, old := range before {
		got := after[index]
		if old.Status >= model.TaskSucceeded {
			if !reflect.DeepEqual(old, got) {
				t.Fatal("cancellation overwrote an existing child terminal result")
			}
			continue
		}
		if got.Status != model.TaskCanceled || got.EndReason == nil || *got.EndReason != model.EndReasonManualStop || got.FinishedAt == nil ||
			got.NextDispatchAt != nil || got.ResultData != nil || got.ResultSummary == nil || *got.ResultSummary == summary ||
			got.AttemptNo != old.AttemptNo || got.UsedAttempts != old.UsedAttempts || !got.StartedAt.Equal(*old.StartedAt) ||
			!got.AttemptRegisteredAt.Equal(*old.AttemptRegisteredAt) || !reflect.DeepEqual(got.ExecutorInstanceID, old.ExecutorInstanceID) ||
			!reflect.DeepEqual(got.ExecutorAddress, old.ExecutorAddress) {
			t.Fatal("cancellation lost its cause, execution facts, or bounded state transition")
		}
	}
	terminal, err := controller.RequestManual(context.Background(), parent.ID)
	if err != nil || terminal.New || terminal.Status != model.InstanceCanceled {
		t.Fatal("terminal stop request claimed a new stopping decision")
	}
	batch, err := controller.ProcessBatch(context.Background(), parent.ID)
	if err != nil || batch.More || batch.Completed || batch.Status != model.InstanceCanceled || !reflect.DeepEqual(ended, readStopParent(t, db, parent.ID)) {
		t.Fatal("repeated convergence rewrote terminal facts")
	}
	changed, err := instances.Complete(context.Background(), parent.ID, model.InstanceSucceeded, nil)
	if err != nil || changed {
		t.Fatal("late normal completion restored a stopped parent")
	}
	bound, err := tasks.BindExecutor(context.Background(), repository.ExecutorBinding{TaskID: before[0].ID, InstanceID: parent.ID,
		AttemptNo: before[0].AttemptNo, ExecutorInstanceID: identity, ExecutorAddress: address})
	if err != nil || bound {
		t.Fatal("late ACK restored a canceled child")
	}
	saver, err := taskresult.NewSaver(tasks)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := saver.Save(context.Background(), taskresult.Report{GroupID: parent.ExecutorGroupID, JobID: parent.JobID, InstanceID: parent.ID,
		TaskID: before[0].ID, AttemptNo: before[0].AttemptNo, ExecutorInstanceID: identity.String(), Outcome: taskresult.OutcomeSuccess})
	if err != nil || saved.Applied {
		t.Fatalf("late business result restored a canceled child: %+v %v", saved, err)
	}
}

func TestWaitingStopDoesNotCreateTasksOrStartTheClock(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	controller, instances, _, runner := stopDependencies(t, db)
	parent := stopParent(t, db, instances, model.InstanceWaiting)
	if decision, err := controller.RequestManual(context.Background(), parent.ID); err != nil || !decision.New {
		t.Fatalf("waiting stop: %+v %v", decision, err)
	}
	registrar, err := dispatch.NewRegistrar(runner)
	if err != nil {
		t.Fatal(err)
	}
	if registered, err := registrar.StartInstance(context.Background(), parent.ID); err != nil || registered != nil {
		t.Fatalf("stopping parent authorized a first Task: %+v %v", registered, err)
	}
	if batch, err := controller.ProcessBatch(context.Background(), parent.ID); err != nil || !batch.Completed || batch.Canceled != 0 {
		t.Fatalf("empty waiting stop: %+v %v", batch, err)
	}
	current := readStopParent(t, db, parent.ID)
	var count int64
	if err := db.Model(&model.JobTask{}).Where("instance_id = ?", parent.ID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if current.Status != model.InstanceCanceled || current.StartedAt != nil || current.DeadlineAt != nil || current.FinishedAt == nil || count != 0 {
		t.Fatal("waiting cancellation started execution or invented an empty child")
	}
}

func TestExistingTimeoutDecisionRetainsItsCauseAndConvergesToFailure(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	controller, instances, _, _ := stopDependencies(t, db)
	parent := stopParent(t, db, instances, model.InstanceRunning)
	reason, summary := model.EndReasonInstanceTimeout, "round deadline reached"
	if err := db.Model(&parent).Updates(map[string]any{"status": model.InstanceStopping, "end_reason": reason, "result_summary": summary}).Error; err != nil {
		t.Fatal(err)
	}
	child := model.JobTask{InstanceID: parent.ID, TaskType: model.TaskTypeOrdinary, Status: model.TaskRunning, AttemptNo: 1, UsedAttempts: 1}
	if err := db.Create(&child).Error; err != nil {
		t.Fatal(err)
	}
	decision, err := controller.RequestManual(context.Background(), parent.ID)
	if err != nil || decision.New || decision.EndReason == nil || *decision.EndReason != reason {
		t.Fatal("manual request overwrote an existing timeout decision")
	}
	for range 2 {
		if _, err := controller.ProcessBatch(context.Background(), parent.ID); err != nil {
			t.Fatal(err)
		}
	}
	got := readStopParent(t, db, parent.ID)
	if err := db.First(&child, child.ID).Error; err != nil {
		t.Fatal(err)
	}
	if got.Status != model.InstanceFailed || got.EndReason == nil || *got.EndReason != reason || got.ResultSummary == nil || *got.ResultSummary != summary ||
		child.Status != model.TaskCanceled || child.EndReason == nil || *child.EndReason != reason {
		t.Fatal("timeout convergence changed the frozen reason or target statuses")
	}
}

func TestStoppingParentContentionReturnsWithoutChildWork(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	controller, instances, _, _ := stopDependencies(t, db)
	parent := stopParent(t, db, instances, model.InstanceWaiting)
	if _, err := controller.RequestManual(context.Background(), parent.ID); err != nil {
		t.Fatal(err)
	}
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	locked, err := repository.NewJobInstanceRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := locked.LockForStop(context.Background(), parent.ID, true); err != nil {
		t.Fatal(err)
	}
	batch, err := controller.ProcessBatch(context.Background(), parent.ID)
	if !errors.Is(err, repository.ErrInstanceLocked) || batch.More || batch.Completed || batch.Canceled != 0 {
		t.Fatalf("contending batch claimed progress: %+v %v", batch, err)
	}
	if readStopParent(t, db, parent.ID).Status != model.InstanceStopping {
		t.Fatal("contending batch completed the parent without owning its lock")
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if batch, err := controller.ProcessBatch(context.Background(), parent.ID); err != nil || !batch.Completed {
		t.Fatalf("new work could not resume after contention ended: %+v %v", batch, err)
	}
}

func TestStopBatchRechecksChildAfterResultTransactionCommitOrRollback(t *testing.T) {
	for _, commit := range []bool{true, false} {
		name := "result rollback"
		if commit {
			name = "result commit"
		}
		t.Run(name, func(t *testing.T) {
			db, sqlDB := testutil.OpenMigratedPostgres(t)
			_, instances, _, runner := stopDependencies(t, db)
			controller, err := instancestop.NewController(runner, instancestop.WithSQLTimeout(5*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			parent := stopParent(t, db, instances, model.InstanceRunning)
			child := model.JobTask{InstanceID: parent.ID, TaskType: model.TaskTypeOrdinary, Status: model.TaskRunning,
				AttemptNo: 1, UsedAttempts: 1, StartedAt: parent.StartedAt, AttemptRegisteredAt: parent.StartedAt}
			if err := db.Create(&child).Error; err != nil {
				t.Fatal(err)
			}
			if _, err := controller.RequestManual(context.Background(), parent.ID); err != nil {
				t.Fatal(err)
			}
			tx := db.Begin()
			if tx.Error != nil {
				t.Fatal(tx.Error)
			}
			defer tx.Rollback()
			var blocker int
			if err := tx.Raw("SELECT pg_backend_pid()").Scan(&blocker).Error; err != nil {
				t.Fatal(err)
			}
			if err := tx.Model(&model.JobTask{}).Where("id = ?", child.ID).Updates(map[string]any{
				"status": model.TaskSucceeded, "result_data": model.TaskData(`{"winner":true}`),
				"result_summary": "business completed", "end_reason": model.EndReasonSuccess,
				"finished_at": gorm.Expr("clock_timestamp()"),
			}).Error; err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			done := make(chan struct{})
			var batch instancestop.BatchResult
			var batchErr error
			go func() {
				batch, batchErr = controller.ProcessBatch(ctx, parent.ID)
				close(done)
			}()
			defer func() { _ = tx.Rollback().Error; cancel(); <-done }()
			observed, stopObserving := context.WithTimeout(ctx, 3*time.Second)
			defer stopObserving()
			for {
				var waiting bool
				if err := sqlDB.QueryRowContext(observed, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))", blocker).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case <-done:
					t.Fatalf("batch did not wait for the child version: %+v %v", batch, batchErr)
				case <-observed.Done():
					t.Fatal("did not observe the actual child row wait")
				case <-time.After(5 * time.Millisecond):
				}
			}
			if commit {
				err = tx.Commit().Error
			} else {
				err = tx.Rollback().Error
			}
			if err != nil {
				t.Fatal(err)
			}
			<-done
			wantCanceled := int64(1)
			if commit {
				wantCanceled = 0
			}
			if batchErr != nil || !batch.More || batch.Completed || batch.Canceled != wantCanceled {
				t.Fatalf("child result arbitration: %+v %v", batch, batchErr)
			}
			if err := db.First(&child, child.ID).Error; err != nil {
				t.Fatal(err)
			}
			if commit {
				if child.Status != model.TaskSucceeded || child.EndReason == nil || *child.EndReason != model.EndReasonSuccess || child.ResultSummary == nil || *child.ResultSummary != "business completed" || len(child.ResultData) == 0 {
					t.Fatal("batch overwrote the committed business result")
				}
			} else if child.Status != model.TaskCanceled || child.ResultData != nil || child.EndReason == nil || *child.EndReason != model.EndReasonManualStop {
				t.Fatal("rolled-back result prevented child cancellation")
			}
			if final, err := controller.ProcessBatch(ctx, parent.ID); err != nil || !final.Completed || final.Status != model.InstanceCanceled {
				t.Fatalf("parent did not retain its committed stopping decision: %+v %v", final, err)
			}
		})
	}
}

func stopDependencies(t *testing.T, db *gorm.DB) (*instancestop.Controller, repository.JobInstanceRepository, repository.JobTaskRepository, repository.TransactionRunner) {
	t.Helper()
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := repository.NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := instancestop.NewController(runner, instancestop.WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	return controller, instances, tasks, runner
}

func stopParent(t *testing.T, db *gorm.DB, instances repository.JobInstanceRepository, status model.InstanceStatus) model.JobInstance {
	t.Helper()
	now, err := instances.DatabaseTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	requestID, operatorID := uuid.New(), int64(42)
	parent := model.JobInstance{JobID: 11, JobName: "stop integration fixture", ExecutorGroupID: 17,
		TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID, Status: status,
		CreatedAt: now, UpdatedAt: now, ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: "test.stop",
			ExecutionMode: model.ExecutionModeNormal, Params: "immutable input", MaxRetries: 3, RetryIntervalSeconds: 1, TimeoutSeconds: 300}}
	if status == model.InstanceRunning {
		started, deadline := now.Add(-time.Minute), now.Add(4*time.Minute)
		parent.StartedAt, parent.DeadlineAt = &started, &deadline
	}
	if err := db.Create(&parent).Error; err != nil {
		t.Fatal(err)
	}
	return readStopParent(t, db, parent.ID)
}

func readStopParent(t *testing.T, db *gorm.DB, id int64) model.JobInstance {
	t.Helper()
	var parent model.JobInstance
	if err := db.First(&parent, id).Error; err != nil {
		t.Fatal(err)
	}
	return parent
}
