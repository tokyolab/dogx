//go:build integration

package repository

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
)

func TestPostResultWaitingOnCancellationPreservesFixedStatistics(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, task := createResultTask(t, db, job)
	instance.ExecutionConfig.ExecutionMode = model.ExecutionModeSharded
	instance.ExecutionConfig.PostEnabled = true
	if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).
		Update("execution_config", instance.ExecutionConfig).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(map[string]any{
		"task_type":  model.TaskTypePost,
		"input_data": model.TaskData(`{"total":3,"succeeded":2,"failed":1}`),
	}).Error; err != nil {
		t.Fatal(err)
	}
	original := readRepositoryTask(t, db, task.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run, err := NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, func(repos Repositories) error {
		parent, err := repos.Instances.LockForStop(ctx, instance.ID, false)
		if err != nil {
			return err
		}
		if parent == nil || parent.Status != model.InstanceRunning {
			return fmt.Errorf("expected running Post parent before stop")
		}
		now, err := repos.Instances.DatabaseTime(ctx)
		if err != nil {
			return err
		}
		changed, err := repos.Instances.RecordStop(ctx, instance.ID, model.InstanceRunning,
			model.EndReasonManualStop, "stop before Post result", now)
		if err != nil {
			return err
		}
		if !changed {
			return fmt.Errorf("Post stop decision did not apply")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	holder := db.WithContext(ctx).Begin()
	if holder.Error != nil {
		t.Fatal(holder.Error)
	}
	defer holder.Rollback()
	parents, err := NewJobInstanceRepository(holder)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := NewJobTaskRepository(holder)
	if err != nil {
		t.Fatal(err)
	}
	if parent, err := parents.LockForStop(ctx, instance.ID, true); err != nil || parent == nil || parent.Status != model.InstanceStopping {
		t.Fatalf("lock stopping parent: parent=%+v err=%v", parent, err)
	}
	ids, err := tasks.ListUnfinishedIDs(ctx, instance.ID, 1)
	if err != nil || !reflect.DeepEqual(ids, []int64{task.ID}) {
		t.Fatalf("select canceled Post: ids=%v err=%v", ids, err)
	}
	now, err := parents.DatabaseTime(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := tasks.CancelUnfinished(ctx, TaskCancellation{InstanceID: instance.ID, TaskIDs: ids,
		Reason: model.EndReasonManualStop, Summary: "Post canceled", ProcessedAt: now}); err != nil || changed != 1 {
		t.Fatalf("cancel Post: changed=%d err=%v", changed, err)
	}
	winner := readRepositoryTask(t, holder, task.ID)
	var blocker int
	if err := holder.Raw("SELECT pg_backend_pid()").Scan(&blocker).Error; err != nil {
		t.Fatal(err)
	}
	write := resultWriteFixture()
	write.TaskID, write.InstanceID, write.AttemptNo = task.ID, instance.ID, original.AttemptNo
	finished, done, pid := startResultWrite(t, ctx, db, write)
	defer func() {
		cancel()
		holder.Rollback()
		<-done
	}()
	var waiter int
	select {
	case waiter = <-pid:
	case result := <-finished:
		t.Fatalf("Post result returned before contention: %+v", result)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Observe the real blocked backend before committing cancellation. The
	// result UPDATE must then recheck its predicate against the canceled row.
	waitForTaskBindingBlock(t, ctx, db, blocker, waiter)
	if err := holder.Commit().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-finished:
		if result.err != nil || result.applied || result.statements != 1 || !result.onlyUpdate {
			t.Fatalf("Post result ignored committed cancellation: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	committedTasks, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := committedTasks.BindExecutor(ctx, taskExecutorBinding(task.ID, instance.ID)); err != nil || applied {
		t.Fatalf("late Post ACK replaced cancellation: applied=%v err=%v", applied, err)
	}
	after := readRepositoryTask(t, db, task.ID)
	if !reflect.DeepEqual(after, winner) || !reflect.DeepEqual(after.InputData, original.InputData) ||
		after.Status != model.TaskCanceled || after.ResultData != nil || after.EndReason == nil || *after.EndReason != model.EndReasonManualStop {
		t.Fatal("Post result or ACK changed cancellation or fixed statistics")
	}
	var parent model.JobInstance
	if err := db.First(&parent, instance.ID).Error; err != nil || parent.Status != model.InstanceStopping ||
		parent.EndReason == nil || *parent.EndReason != model.EndReasonManualStop || parent.FinishedAt != nil {
		t.Fatalf("Post result altered the parent stop decision: parent=%+v err=%v", parent, err)
	}
}
