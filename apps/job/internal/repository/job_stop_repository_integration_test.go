//go:build integration

package repository

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestStopDecisionFreezesReasonWithoutChangingExecutionFacts(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	for _, input := range []struct {
		status model.InstanceStatus
		reason string
	}{
		{model.InstanceWaiting, model.EndReasonManualStop},
		{model.InstanceRunning, model.EndReasonManualStop},
		{model.InstanceRunning, model.EndReasonInstanceTimeout},
	} {
		t.Run(fmt.Sprintf("status_%d/%s", input.status, input.reason), func(t *testing.T) {
			parent, child := createResultTask(t, db, job)
			fields := map[string]any{"status": input.status, "finished_at": time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC),
				"end_reason": "old reason", "result_summary": "old summary"}
			if input.status == model.InstanceWaiting {
				fields["started_at"], fields["deadline_at"] = nil, nil
			}
			if err := db.Model(&model.JobInstance{}).Where("id = ?", parent.ID).Updates(fields).Error; err != nil {
				t.Fatal(err)
			}
			before, childBefore := readCompletionInstance(t, db, parent.ID), readRepositoryTask(t, db, child.ID)
			runner, err := NewTransactionRunner(db)
			if err != nil {
				t.Fatal(err)
			}
			var decided time.Time
			if err := runner(context.Background(), func(repos Repositories) error {
				locked, err := repos.Instances.LockForStop(context.Background(), parent.ID, false)
				if err != nil || locked == nil || locked.Status != input.status || locked.ExecutorGroupID != parent.ExecutorGroupID {
					return fmt.Errorf("locked stop facts: row=%+v err=%w", locked, err)
				}
				decided, err = repos.Instances.DatabaseTime(context.Background())
				if err != nil {
					return err
				}
				changed, err := repos.Instances.RecordStop(context.Background(), parent.ID, locked.Status, input.reason, "whole Instance stopped", decided)
				if err != nil {
					return err
				}
				if !changed {
					return errors.New("stop decision did not update its expected state")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			after := readCompletionInstance(t, db, parent.ID)
			want := before
			want.Status, want.FinishedAt, want.UpdatedAt = model.InstanceStopping, nil, decided
			reason, summary := input.reason, "whole Instance stopped"
			want.EndReason, want.ResultSummary = &reason, &summary
			if !reflect.DeepEqual(want, after) || !reflect.DeepEqual(childBefore, readRepositoryTask(t, db, child.ID)) {
				t.Fatal("stop decision changed child, lifetime, snapshot or frozen decision facts")
			}
			instances, err := NewJobInstanceRepository(db)
			if err != nil {
				t.Fatal(err)
			}
			if active, err := instances.HasActive(context.Background(), parent.JobID); err != nil || !active {
				t.Fatalf("stopping parent stopped blocking new rounds: active=%v err=%v", active, err)
			}
			if changed, err := instances.Complete(context.Background(), parent.ID, model.InstanceSucceeded, nil); changed || err != nil {
				t.Fatalf("normal completion overwrote stop decision: changed=%v err=%v", changed, err)
			}
			if err := runner(context.Background(), func(repos Repositories) error {
				if _, err := repos.Instances.LockForStop(context.Background(), parent.ID, false); err != nil {
					return err
				}
				changed, err := repos.Instances.RecordStop(context.Background(), parent.ID, input.status, model.EndReasonManualStop, "replacement", decided.Add(time.Hour))
				if changed {
					return errors.New("duplicate decision refreshed the frozen reason or summary")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, readCompletionInstance(t, db, parent.ID)) {
				t.Fatal("repeated stop changed an existing stopping parent")
			}
		})
	}
}

func TestStopLockProjectsOnlyNeededFactsAndLeavesCommitToCaller(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	parent, _ := createResultTask(t, db, job)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", parent.ID).
		Update("execution_config", gorm.Expr("'null'::jsonb")).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	tx := db.Session(&gorm.Session{Logger: counter}).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	repo, err := NewJobInstanceRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	for _, nowait := range []bool{false, true} {
		counter.count, counter.statements = 0, nil
		row, err := repo.LockForStop(context.Background(), parent.ID, nowait)
		if err != nil || row == nil || row.ID != parent.ID || row.StartedAt == nil || row.DeadlineAt == nil || counter.count != 1 {
			t.Fatalf("invalid stop projection: row=%+v err=%v statements=%v", row, err, counter.statements)
		}
		statement := counter.statements[0]
		if !strings.Contains(statement, "FOR UPDATE") || strings.Contains(statement, "NOWAIT") != nowait {
			t.Fatalf("wrong parent lock mode: %s", statement)
		}
		for _, forbidden := range []string{"execution_config", "result_summary", "job_task", "JOIN", "SELECT *"} {
			if strings.Contains(statement, forbidden) {
				t.Fatalf("stop lock unnecessarily loaded %s", forbidden)
			}
		}
	}
	if row, err := repo.LockForStop(context.Background(), parent.ID+100000, false); row != nil || err != nil {
		t.Fatalf("missing stop parent: row=%+v err=%v", row, err)
	}
	decided, err := repo.DatabaseTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := repo.RecordStop(context.Background(), parent.ID, model.InstanceRunning, model.EndReasonManualStop, "stop requested", decided); !changed || err != nil {
		t.Fatalf("transaction-local decision: changed=%v err=%v", changed, err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	var stored struct{ Status model.InstanceStatus }
	if err := db.Model(&model.JobInstance{}).Select("status").Where("id = ?", parent.ID).Take(&stored).Error; err != nil || stored.Status != model.InstanceRunning {
		t.Fatalf("rolled-back decision was published as committed: status=%d err=%v", stored.Status, err)
	}
}

func TestStopCancellationUsesBoundedBatchesAndPreservesTerminalRows(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	for _, reason := range []string{model.EndReasonManualStop, model.EndReasonInstanceTimeout} {
		t.Run(reason, func(t *testing.T) {
			parent, ordinary := createResultTask(t, db, job)
			if err := db.Delete(&model.JobTask{}, ordinary.ID).Error; err != nil {
				t.Fatal(err)
			}
			now := retryRepositoryTime(t, db)
			if err := db.Model(&model.JobInstance{}).Where("id = ?", parent.ID).
				Updates(map[string]any{"status": model.InstanceStopping, "end_reason": reason, "result_summary": "frozen parent summary"}).Error; err != nil {
				t.Fatal(err)
			}
			var before []model.JobTask
			for _, kind := range []model.TaskType{model.TaskTypeOrdinary, model.TaskTypePrepare, model.TaskTypeShard, model.TaskTypePost} {
				for _, state := range []model.TaskStatus{model.TaskWaiting, model.TaskRunning, model.TaskRetryWaiting, model.TaskSucceeded, model.TaskFailed, model.TaskCanceled} {
					// Only shards may repeat across states within this parent; each
					// singleton stage contributes one running row.
					if kind != model.TaskTypeShard && state != model.TaskRunning {
						continue
					}
					registered, started, next, finished := now.Add(-time.Minute), now.Add(-time.Hour), now.Add(time.Minute), now.Add(-time.Second)
					identity, address, summary, oldReason := uuid.New(), "127.0.0.1:19001", "previous summary", "previous reason"
					task := model.JobTask{InstanceID: parent.ID, TaskType: kind, Status: state, AttemptNo: 7, UsedAttempts: 3,
						AttemptRegisteredAt: &registered, StartedAt: &started, NextDispatchAt: &next, FinishedAt: &finished,
						ExecutorInstanceID: &identity, ExecutorAddress: &address, ResultData: model.TaskResultData(`{"old":true}`),
						ResultSummary: &summary, EndReason: &oldReason, CreatedAt: started, UpdatedAt: registered}
					if err := db.Create(&task).Error; err != nil {
						t.Fatal(err)
					}
					before = append(before, readRepositoryTask(t, db, task.ID))
				}
			}
			_, unrelated := createResultTask(t, db, job)
			unrelatedBefore := readRepositoryTask(t, db, unrelated.ID)
			parentBefore := readCompletionInstance(t, db, parent.ID)
			runner, err := NewTransactionRunner(db)
			if err != nil {
				t.Fatal(err)
			}
			var canceled []int64
			processedTimes := make(map[int64]time.Time)
			for {
				var empty bool
				err := runner(context.Background(), func(repos Repositories) error {
					locked, err := repos.Instances.LockForStop(context.Background(), parent.ID, true)
					if err != nil || locked == nil || locked.Status != model.InstanceStopping {
						return fmt.Errorf("batch parent lock: row=%+v err=%w", locked, err)
					}
					ids, err := repos.Tasks.ListUnfinishedIDs(context.Background(), parent.ID, 2)
					if err != nil {
						return err
					}
					empty = len(ids) == 0
					if empty {
						if ids == nil {
							return errors.New("empty batch must be a non-nil slice")
						}
						changed, err := repos.Instances.FinishStop(context.Background(), parent.ID, reason)
						if !changed && err == nil {
							return errors.New("empty stopped parent did not finish")
						}
						return err
					}
					if len(ids) > 2 || (len(ids) == 2 && ids[0] >= ids[1]) {
						return errors.New("child batch lost its bound or order")
					}
					if changed, err := repos.Instances.FinishStop(context.Background(), parent.ID, reason); changed || err != nil {
						return fmt.Errorf("unfinished children allowed completion: changed=%v err=%w", changed, err)
					}
					processed, err := repos.Instances.DatabaseTime(context.Background())
					if err != nil {
						return err
					}
					writeIDs := append(append([]int64{}, ids...), ids[0], unrelated.ID, unrelated.ID+100000)
					count, err := repos.Tasks.CancelUnfinished(context.Background(), TaskCancellation{InstanceID: parent.ID,
						TaskIDs: writeIDs, Reason: reason, Summary: "canceled by whole Instance stop", ProcessedAt: processed})
					if err != nil {
						return err
					}
					if count != int64(len(ids)) {
						return fmt.Errorf("batch changed %d rows, want %d", count, len(ids))
					}
					canceled = append(canceled, ids...)
					for _, id := range ids {
						processedTimes[id] = processed
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				if empty {
					break
				}
				if !reflect.DeepEqual(parentBefore, readCompletionInstance(t, db, parent.ID)) {
					t.Fatal("a child batch prematurely ended or rewrote its parent")
				}
			}
			if len(canceled) != 6 {
				t.Fatalf("wrong unfinished set size: %v", canceled)
			}
			for _, old := range before {
				want := old
				if old.Status == model.TaskWaiting || old.Status == model.TaskRunning || old.Status == model.TaskRetryWaiting {
					summary := "canceled by whole Instance stop"
					processed := processedTimes[old.ID]
					want.Status, want.NextDispatchAt, want.ResultData = model.TaskCanceled, nil, nil
					want.FinishedAt, want.UpdatedAt, want.EndReason, want.ResultSummary = &processed, processed, &reason, &summary
				}
				if !reflect.DeepEqual(want, readRepositoryTask(t, db, old.ID)) {
					t.Fatalf("cancellation changed protected facts or terminal Task %d", old.ID)
				}
			}
			if !reflect.DeepEqual(unrelatedBefore, readRepositoryTask(t, db, unrelated.ID)) {
				t.Fatal("cancellation crossed its parent boundary")
			}
			finished := readCompletionInstance(t, db, parent.ID)
			want := parentBefore
			want.Status = model.InstanceCanceled
			if reason == model.EndReasonInstanceTimeout {
				want.Status = model.InstanceFailed
			}
			want.FinishedAt, want.UpdatedAt = finished.FinishedAt, finished.UpdatedAt
			if finished.FinishedAt == nil || finished.FinishedAt.Before(now) || !reflect.DeepEqual(want, finished) {
				t.Fatal("final parent state did not follow its frozen stop reason")
			}
		})
	}
}

func TestStopCompletionChecksFrozenReasonAndRollsBackWithChildBatch(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	parent, task := createResultTask(t, db, job)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", parent.ID).
		Updates(map[string]any{"status": model.InstanceStopping, "end_reason": model.EndReasonManualStop, "result_summary": "frozen"}).Error; err != nil {
		t.Fatal(err)
	}
	before, taskBefore := readCompletionInstance(t, db, parent.ID), readRepositoryTask(t, db, task.ID)
	runner, err := NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("synthetic stop rollback")
	var committed model.JobInstance
	for _, abort := range []bool{true, false} {
		err := runner(context.Background(), func(repos Repositories) error {
			if _, err := repos.Instances.LockForStop(context.Background(), parent.ID, true); err != nil {
				return err
			}
			processed, err := repos.Instances.DatabaseTime(context.Background())
			if err != nil {
				return err
			}
			if count, err := repos.Tasks.CancelUnfinished(context.Background(), TaskCancellation{InstanceID: parent.ID,
				TaskIDs: []int64{task.ID}, Reason: model.EndReasonManualStop, Summary: "canceled", ProcessedAt: processed}); err != nil || count != 1 {
				return fmt.Errorf("transactional child cancellation: count=%d err=%w", count, err)
			}
			if changed, err := repos.Instances.FinishStop(context.Background(), parent.ID, model.EndReasonInstanceTimeout); changed || err != nil {
				return fmt.Errorf("wrong reason ended parent: changed=%v err=%w", changed, err)
			}
			if changed, err := repos.Instances.FinishStop(context.Background(), parent.ID, model.EndReasonManualStop); !changed || err != nil {
				return fmt.Errorf("matching reason did not end parent: changed=%v err=%w", changed, err)
			}
			if abort {
				return rollback
			}
			return nil
		})
		if abort {
			if !errors.Is(err, rollback) || !reflect.DeepEqual(before, readCompletionInstance(t, db, parent.ID)) ||
				!reflect.DeepEqual(taskBefore, readRepositoryTask(t, db, task.ID)) {
				t.Fatalf("outer rollback did not undo parent and child: %v", err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			committed = readCompletionInstance(t, db, parent.ID)
		}
	}
	if err := runner(context.Background(), func(repos Repositories) error {
		if _, err := repos.Instances.LockForStop(context.Background(), parent.ID, true); err != nil {
			return err
		}
		if changed, err := repos.Instances.FinishStop(context.Background(), parent.ID, model.EndReasonManualStop); changed || err != nil {
			return fmt.Errorf("repeated completion: changed=%v err=%w", changed, err)
		}
		count, err := repos.Tasks.CancelUnfinished(context.Background(), TaskCancellation{InstanceID: parent.ID, TaskIDs: []int64{task.ID},
			Reason: model.EndReasonManualStop, Summary: "replacement", ProcessedAt: time.Now()})
		if count != 0 {
			return errors.New("repeated cancellation rewrote an existing terminal Task")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(committed, readCompletionInstance(t, db, parent.ID)) {
		t.Fatal("repeated completion refreshed the original terminal parent")
	}
}

func TestStopWaitingParentWithoutTasksKeepsOriginalNullTimes(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	parent, task := createResultTask(t, db, job)
	if err := db.Delete(&model.JobTask{}, task.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobInstance{}).Where("id = ?", parent.ID).
		Updates(map[string]any{"status": model.InstanceWaiting, "started_at": nil, "deadline_at": nil}).Error; err != nil {
		t.Fatal(err)
	}
	runner, err := NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner(context.Background(), func(repos Repositories) error {
		if _, err := repos.Instances.LockForStop(context.Background(), parent.ID, false); err != nil {
			return err
		}
		now, err := repos.Instances.DatabaseTime(context.Background())
		if err != nil {
			return err
		}
		if changed, err := repos.Instances.RecordStop(context.Background(), parent.ID, model.InstanceRunning, model.EndReasonManualStop, "wrong expected state", now); changed || err != nil {
			return errors.New("stop decision ignored its expected state")
		}
		changed, err := repos.Instances.RecordStop(context.Background(), parent.ID, model.InstanceWaiting, model.EndReasonManualStop, "stopped before start", now)
		if !changed && err == nil {
			return errors.New("waiting stop was not saved")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// Finalization owns a later parent transaction, never the decision transaction.
	if err := runner(context.Background(), func(repos Repositories) error {
		if _, err := repos.Instances.LockForStop(context.Background(), parent.ID, true); err != nil {
			return err
		}
		ids, err := repos.Tasks.ListUnfinishedIDs(context.Background(), parent.ID, 256)
		if err != nil || ids == nil || len(ids) != 0 {
			return fmt.Errorf("empty parent cancellation query: ids=%v err=%w", ids, err)
		}
		changed, err := repos.Instances.FinishStop(context.Background(), parent.ID, model.EndReasonManualStop)
		if !changed && err == nil {
			return errors.New("empty stopping parent did not finish")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	after := readCompletionInstance(t, db, parent.ID)
	if after.Status != model.InstanceCanceled || after.StartedAt != nil || after.DeadlineAt != nil || after.FinishedAt == nil {
		t.Fatal("empty waiting stop invented start or deadline facts")
	}
	var count int64
	if err := db.Model(&model.JobTask{}).Where("instance_id = ?", parent.ID).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("empty waiting stop created a Task")
	}
}

func TestCanceledTaskRejectsOldBindingBusyRejectionAndResult(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	parent, task := createResultTask(t, db, job)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", parent.ID).
		Updates(map[string]any{"status": model.InstanceStopping, "end_reason": model.EndReasonManualStop}).Error; err != nil {
		t.Fatal(err)
	}
	runner, err := NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner(context.Background(), func(repos Repositories) error {
		if _, err := repos.Instances.LockForStop(context.Background(), parent.ID, true); err != nil {
			return err
		}
		now, err := repos.Instances.DatabaseTime(context.Background())
		if err != nil {
			return err
		}
		count, err := repos.Tasks.CancelUnfinished(context.Background(), TaskCancellation{InstanceID: parent.ID, TaskIDs: []int64{task.ID},
			Reason: model.EndReasonManualStop, Summary: "canceled by Instance stop", ProcessedAt: now})
		if count != 1 && err == nil {
			return errors.New("initial child cancellation did not apply")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	before := readRepositoryTask(t, db, task.ID)
	tasks, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	binding := taskExecutorBinding(task.ID, parent.ID)
	if changed, err := tasks.BindExecutor(context.Background(), binding); changed || err != nil {
		t.Fatalf("late ACK changed canceled Task: changed=%v err=%v", changed, err)
	}
	if changed, err := tasks.SaveDispatchBusy(context.Background(), DispatchBusyWrite{TaskID: task.ID, InstanceID: parent.ID, AttemptNo: task.AttemptNo, Delay: time.Second}); changed || err != nil {
		t.Fatalf("late BUSY changed canceled Task: changed=%v err=%v", changed, err)
	}
	if changed, err := tasks.SaveDispatchRejection(context.Background(), DispatchRejectionWrite{TaskID: task.ID, InstanceID: parent.ID,
		AttemptNo: task.AttemptNo, MaxRetries: 3, RetryIntervalSeconds: 10, Summary: "late rejection"}); changed || err != nil {
		t.Fatalf("late rejection changed canceled Task: changed=%v err=%v", changed, err)
	}
	if changed, err := tasks.SaveResult(context.Background(), TaskResultWrite{TaskID: task.ID, InstanceID: parent.ID,
		AttemptNo: task.AttemptNo, ExecutorInstanceID: binding.ExecutorInstanceID, Status: model.TaskSucceeded,
		Data: model.TaskResultData(`{"late":true}`), Summary: "late success"}); changed || err != nil {
		t.Fatalf("late result changed canceled Task: changed=%v err=%v", changed, err)
	}
	if !reflect.DeepEqual(before, readRepositoryTask(t, db, task.ID)) {
		t.Fatal("a late response changed canceled execution facts")
	}
}

func TestStoppingQueriesShareTheUnfinishedIndex(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	parent, ordinary := createResultTask(t, db, job)
	if err := db.Delete(&model.JobTask{}, ordinary.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobInstance{}).Where("id = ?", parent.ID).
		Updates(map[string]any{"status": model.InstanceStopping, "end_reason": model.EndReasonManualStop}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO job_task (instance_id, task_type, status, attempt_no, used_attempts)
		SELECT ?, 3, CASE WHEN n % 20 = 0 THEN 2 ELSE 4 END, 1, 1 FROM generate_series(1, 30000) n`, parent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE job_task").Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	tx := db.Session(&gorm.Session{Logger: counter}).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	instances, err := NewJobInstanceRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := NewJobTaskRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := instances.LockForStop(context.Background(), parent.ID, true); err != nil {
		t.Fatal(err)
	}
	counter.count, counter.statements = 0, nil
	ids, err := tasks.ListUnfinishedIDs(context.Background(), parent.ID, 257)
	if err != nil || len(ids) != 257 || counter.count != 1 {
		t.Fatalf("bounded cancellation read: ids=%d statements=%v err=%v", len(ids), counter.statements, err)
	}
	childQuery := counter.statements[0]
	for _, forbidden := range []string{"result_data", "result_summary", "task_type", "attempt_no", "FOR UPDATE", "OFFSET", "JOIN"} {
		if strings.Contains(childQuery, forbidden) {
			t.Fatalf("unfinished query added %s", forbidden)
		}
	}
	counter.count, counter.statements = 0, nil
	if changed, err := instances.FinishStop(context.Background(), parent.ID, model.EndReasonManualStop); changed || err != nil || counter.count != 1 {
		t.Fatalf("NOT EXISTS guard: changed=%v statements=%v err=%v", changed, counter.statements, err)
	}
	finishQuery := counter.statements[0]
	if !strings.Contains(finishQuery, "NOT EXISTS") {
		t.Fatalf("final parent transition omitted child guard: %s", finishQuery)
	}
	for _, statement := range []string{childQuery, finishQuery} {
		plan := explainExecutionQuery(t, tx, statement)
		if !strings.Contains(plan, "idx_job_task_unfinished_instance") || strings.Contains(plan, "Seq Scan on job_task") || strings.Contains(plan, "Sort  (") {
			t.Fatalf("stopping query lost the shared unfinished index:\n%s", plan)
		}
	}
}
