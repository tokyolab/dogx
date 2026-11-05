//go:build integration && e2e

package e2e

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// TestSystemExecutorProcessShutdownDeadlineEndToEnd exercises the production
// System host's shutdown deadline with test-only business that ignores its
// context. The test never kills the process before assessing its actual exit.
// An unfinished business call must not become a successful result or a fresh
// scheduler attempt merely because its executor process reached that deadline.
func TestSystemExecutorProcessShutdownDeadlineEndToEnd(t *testing.T) {
	const (
		budget    = 4 * time.Second
		exitLimit = 15 * time.Second
	)
	f := newJobLifecycleFixture(t)
	node := f.startJob(t, "deadline-job")
	f.createGroup(t, node)
	executor := f.startSystem(t, "deadline-system-executor", true, budget)
	jobID, instanceID := f.trigger(t, node, "stubborn")
	accepted := f.waitRunning(t, node, instanceID, node.process, executor.process)
	management := lifecycleConnection(t, executor.managementPort)
	execution := lifecycleConnection(t, executor.executorPort)
	lifecycleWaitHealth(t, management, executor.process)
	lifecycleWaitHealth(t, execution, executor.process)

	// Include the Windows sender's startup and signal delivery in the elapsed
	// time. Measuring only after signalE2EProcess would hide that part of shutdown.
	started := time.Now()
	signalE2EProcess(t, executor.process)
	for _, conn := range []*grpc.ClientConn{management, execution} {
		jobE2EEventually(t, []*runningProcess{node.process, executor.process}, "stopped deadline-test RPC admission", func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
			return status.Code(err) == codes.Unavailable
		})
	}
	// One probe proves the accepted business can still use its own database
	// after both listeners stop admission. It leaves the business gate closed.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	result, err := f.sqlDB.ExecContext(ctx, `UPDATE system_job_e2e_records SET state = 'probe'
		WHERE task_id = $1 AND invocation_no = 1 AND state = 'running'`, accepted.Id)
	cancel()
	if err != nil {
		t.Fatalf("request shutdown dependency probe: %v", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatal("accepted business was not waiting before the shutdown deadline")
	}
	jobE2EEventually(t, []*runningProcess{node.process, executor.process}, "business database probe during shutdown", func() bool {
		return jobE2EBusinessState(t, f.sqlDB, accepted.Id, 1) == "running"
	})
	remaining := exitLimit - time.Since(started)
	if remaining <= 0 {
		t.Fatal("shutdown observation exceeded the process exit limit")
	}
	exitErr := waitE2EProcessExit(t, executor.process, remaining)
	elapsed := time.Since(started)
	t.Logf("System executor deadline exit: budget=%s elapsed=%s", budget, elapsed)
	var abnormalExit *exec.ExitError
	if !errors.As(exitErr, &abnormalExit) {
		t.Fatalf("unfinished business must cause an abnormal process exit, got %v", exitErr)
	}
	// Unix may exit through go-zero's watchdog signal rather than exit code 1.
	// Allow scheduling and sender overhead without accepting an immediate exit
	// or an indefinitely extended budget for the second server or business drain.
	if elapsed < budget-time.Second || elapsed > exitLimit {
		t.Fatalf("shutdown elapsed %s outside the allowed interval for budget %s", elapsed, budget)
	}
	lifecycleAssertListenerClosed(t, executor.managementPort)
	lifecycleAssertListenerClosed(t, executor.executorPort)
	f.waitNoDatabaseConnections(t, executor.appName)
	if jobE2EBusinessState(t, f.sqlDB, accepted.Id, 1) != "running" {
		t.Fatal("unfinished business was recorded as completed during process exit")
	}
	jobE2EAssertBusiness(t, f.sqlDB, accepted.Id, instanceID, jobID, "lifecycle-stubborn", 1)

	// Recheck persisted facts over a short observation window. Losing an executor
	// must not immediately fabricate completion or register another attempt;
	// this does not measure scan frequency or eventual Instance timeout recovery.
	until := time.NewTimer(time.Second)
	defer until.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		parent, parentErr := node.client.GetInstance(ctx, &job.GetInstanceRequest{Id: instanceID})
		children, taskErr := node.client.ListInstanceTasks(ctx, &job.ListInstanceTasksRequest{InstanceId: instanceID, Page: 1, PageSize: 20})
		cancel()
		if parentErr != nil || taskErr != nil {
			t.Fatalf("query execution after executor deadline: parent=%v task=%v", parentErr, taskErr)
		}
		instance := parent.GetInstance()
		if instance.GetId() != instanceID || instance.GetStatus() != 2 || instance.FinishedAt != nil || instance.EndReason != nil {
			t.Fatal("executor shutdown fabricated a terminal Instance result")
		}
		if children.Total != 1 || len(children.Items) != 1 {
			t.Fatal("executor shutdown changed the ordinary Task count")
		}
		task := children.Items[0]
		if task.Id != accepted.Id || task.Status != 2 || task.AttemptNo != 1 || task.UsedAttempts != 1 ||
			task.GetExecutorInstanceId() != accepted.GetExecutorInstanceId() || task.GetExecutorAddress() != accepted.GetExecutorAddress() ||
			task.FinishedAt != nil || task.EndReason != nil || task.NextDispatchAt != nil {
			t.Fatal("executor shutdown changed the accepted attempt or manufactured a result")
		}
		select {
		case <-until.C:
			return
		case <-ticker.C:
		}
	}
}
