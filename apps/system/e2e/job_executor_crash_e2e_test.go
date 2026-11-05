//go:build integration && e2e

package e2e

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
)

// TestExecutorCrashEventuallyTimesOutEndToEnd kills an executor after its real
// business call and ACK binding have been persisted. The production Job process
// must eventually cancel the unfinished Task and fail its Instance by timeout,
// without receiving a fabricated result or changing the original attempt.
//
// This deliberately waits through the production four-minute recovery age gate.
// Only the deployment-configurable scan interval changes from two minutes to one
// second. No clock, Job table, timestamp or production recovery rule is changed.
func TestExecutorCrashEventuallyTimesOutEndToEnd(t *testing.T) {
	const recoveryAge = 4 * time.Minute
	f := newJobLifecycleFixture(t)
	node := f.startJob(t, "executor-crash-job", time.Second)
	f.createGroup(t, node)
	executor := f.startSystem(t, "crashed-system-executor", true)
	jobID, instanceID := f.trigger(t, node, "wait")
	accepted := f.waitRunning(t, node, instanceID, node.process, executor.process)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	response, err := node.client.GetInstance(ctx, &job.GetInstanceRequest{Id: instanceID})
	cancel()
	if err != nil {
		t.Fatalf("read Instance before executor crash: %v", err)
	}
	original := response.GetInstance()
	if original.GetStatus() != 2 || original.StartedAt == nil || original.DeadlineAt == nil || original.FinishedAt != nil {
		t.Fatal("executor crash requires a started, unfinished Instance")
	}
	startedAt, err := time.Parse(time.RFC3339Nano, *original.StartedAt)
	if err != nil {
		t.Fatalf("parse persisted Instance start: %v", err)
	}
	deadlineAt, err := time.Parse(time.RFC3339Nano, *original.DeadlineAt)
	if err != nil || !deadlineAt.After(startedAt) || deadlineAt.After(startedAt.Add(recoveryAge)) {
		t.Fatal("fixture deadline must expire before the production recovery age gate")
	}
	crashedAt := time.Now()
	// This is an explicit crash injection, unlike graceful signal tests. Only
	// this already-started fixture process is killed; Job and etcd remain alive.
	if err := executor.process.cmd.Process.Kill(); err != nil {
		t.Fatalf("crash the accepted executor process: %v", err)
	}
	var abnormalExit *exec.ExitError
	if err := waitE2EProcessExit(t, executor.process, 5*time.Second); !errors.As(err, &abnormalExit) {
		t.Fatalf("executor crash did not produce an abnormal exit: %v", err)
	}
	lifecycleAssertListenerClosed(t, executor.managementPort)
	lifecycleAssertListenerClosed(t, executor.executorPort)
	f.waitNoDatabaseConnections(t, executor.appName)
	if jobE2EBusinessState(t, f.sqlDB, accepted.Id, 1) != "running" {
		t.Fatal("crash fixture completed or cooperatively canceled its business call")
	}

	// Poll through the public management RPCs using a monotonic local budget.
	// Reading facts cannot enqueue completion hints; only production recovery
	// can discover and advance this crashed executor's bound, unfinished attempt.
	limit := time.NewTimer(5 * time.Minute)
	defer limit.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var instance *job.InstanceInfo
	var task *job.TaskListItem
	for {
		select {
		case <-node.process.done:
			t.Fatal(node.process.failure())
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		parent, parentErr := node.client.GetInstance(ctx, &job.GetInstanceRequest{Id: instanceID})
		children, taskErr := node.client.ListInstanceTasks(ctx, &job.ListInstanceTasksRequest{InstanceId: instanceID, Page: 1, PageSize: 20})
		cancel()
		if parentErr != nil || taskErr != nil {
			t.Fatalf("query crashed executor recovery: Instance=%v Task=%v", parentErr, taskErr)
		}
		instance = parent.GetInstance()
		if instance.GetId() != instanceID || instance.GetJobId() != jobID || instance.GetStartedAt() != original.GetStartedAt() ||
			instance.GetDeadlineAt() != original.GetDeadlineAt() || children.Total != 1 || len(children.Items) != 1 {
			t.Fatal("executor recovery changed the round identity, lifetime or Task count")
		}
		task = children.Items[0]
		if task.Id != accepted.Id || task.InstanceId != instanceID || task.AttemptNo != accepted.AttemptNo || task.UsedAttempts != accepted.UsedAttempts ||
			task.GetExecutorInstanceId() != accepted.GetExecutorInstanceId() || task.GetExecutorAddress() != accepted.GetExecutorAddress() ||
			task.NextDispatchAt != nil {
			t.Fatal("executor crash replaced or rescheduled the original bound attempt")
		}
		if task.Status != 2 && task.Status != 6 {
			t.Fatalf("executor crash manufactured a Task outcome: status=%d reason=%s", task.Status, task.GetEndReason())
		}
		if task.Status == 6 && (task.GetEndReason() != "instance_timeout" || task.FinishedAt == nil) {
			t.Fatal("crashed Task was canceled without its committed timeout facts")
		}
		switch instance.Status {
		case 2:
			if instance.EndReason != nil || instance.FinishedAt != nil {
				t.Fatal("running Instance acquired terminal timeout facts prematurely")
			}
		case 6:
			if instance.GetEndReason() != "instance_timeout" || instance.FinishedAt != nil {
				t.Fatal("Instance stop decision lost its timeout reason or finalized too early")
			}
		case 4:
			if instance.GetEndReason() != "instance_timeout" || instance.FinishedAt == nil || task.Status != 6 {
				t.Fatal("timeout recovery did not finish both the Instance and its Task")
			}
		default:
			t.Fatalf("executor crash produced an unexpected Instance outcome: status=%d reason=%s", instance.Status, instance.GetEndReason())
		}
		if instance.Status == 4 {
			break
		}
		select {
		case <-node.process.done:
			t.Fatal(node.process.failure())
		case <-limit.C:
			t.Fatalf("executor crash did not converge within five minutes: Instance=%d Task=%d", instance.Status, task.Status)
		case <-ticker.C:
		}
	}
	finishedAt, err := time.Parse(time.RFC3339Nano, instance.GetFinishedAt())
	if err != nil || finishedAt.Before(startedAt.Add(recoveryAge)) || finishedAt.Before(deadlineAt) {
		t.Fatal("timeout completion bypassed the persisted deadline or four-minute recovery age")
	}
	if jobE2EBusinessState(t, f.sqlDB, accepted.Id, 1) != "running" {
		t.Fatal("scheduler recovery fabricated a completed business record")
	}
	jobE2EAssertBusiness(t, f.sqlDB, accepted.Id, instanceID, jobID, "lifecycle-wait", 1)
	t.Logf("Executor crash converged by timeout: elapsed=%s recovery_age=%s scan_interval=1s", time.Since(crashedAt), recoveryAge)
}
