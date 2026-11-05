//go:build integration && e2e

package e2e

import (
	"testing"
	"time"
)

// TestJobSchedulerProcessCrashRecoveryEndToEnd kills the production scheduler
// after its accepted attempt is bound. Real business completes while no Job
// process is running; its executor remains alive and reports to the replacement
// through the unchanged discovery key. This does not test recovering results
// already acknowledged by a scheduler or surviving an executor process crash.
func TestJobSchedulerProcessCrashRecoveryEndToEnd(t *testing.T) {
	fixture := newJobLifecycleFixture(t)
	nodeA := fixture.startJob(t, "crash-job-a")
	fixture.createGroup(t, nodeA)
	executor := fixture.startSystem(t, "crash-system-executor", true)
	jobID, instanceID := fixture.trigger(t, nodeA, "gated")
	accepted := fixture.waitRunning(t, nodeA, instanceID, nodeA.process, executor.process)
	if accepted.GetId() <= 0 || accepted.GetAttemptNo() != 1 || accepted.GetUsedAttempts() != 1 {
		t.Fatal("the crash must follow the first accepted and bound attempt")
	}
	taskID := accepted.GetId()

	// Kill bypasses the production signal handler and its drain budget. Wait
	// observes actual OS exit before asserting that A's database pool is gone.
	if err := nodeA.process.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill scheduler process A: %v", err)
	}
	if err := waitE2EProcessExit(t, nodeA.process, 5*time.Second); err == nil {
		t.Fatal("the killed scheduler unexpectedly reported a clean exit")
	}
	fixture.waitNoDatabaseConnections(t, nodeA.appName)

	// The only release writes the System fixture's business control state. The
	// processor computes its own result, and the real Runtime owns reporting;
	// the test never writes Job state or constructs a Task result message.
	fixture.releaseBusiness(t, taskID)
	jobE2EEventually(t, []*runningProcess{executor.process}, "business completion while the scheduler is absent", func() bool {
		return jobE2EBusinessState(t, fixture.sqlDB, taskID, 1) == "succeeded"
	})

	nodeB := fixture.startJob(t, "crash-job-b")
	fixture.waitSuccess(t, nodeB, jobID, instanceID, taskID, nodeB.process, executor.process)
	t.Log("a replacement production scheduler persisted the retained executor outcome for the original Task and attempt; business ran once")
}
