//go:build integration && e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// TestProductionRPCProcessSignalShutdownEndToEnd exercises the real Job and
// System main functions with initialized PostgreSQL, Redis and etcd dependencies.
// Both dual-server processes are idle: active business draining is tested below
// through the System test binary because no production processor is registered.
func TestProductionRPCProcessSignalShutdownEndToEnd(t *testing.T) {
	f := newJobLifecycleFixture(t)
	node := f.startJob(t, "signal-job")
	system := f.startSystem(t, "signal-system", false)
	for _, target := range []struct {
		process *runningProcess
		appName string
		ports   []int
	}{
		{node.process, node.appName, []int{node.managementPort, node.resultPort}},
		{system.process, system.appName, []int{system.managementPort, system.executorPort}},
	} {
		signalE2EProcess(t, target.process)
		if err := waitE2EProcessExit(t, target.process, 15*time.Second); err != nil {
			t.Fatal(target.process.failure())
		}
		for _, port := range target.ports {
			lifecycleAssertListenerClosed(t, port)
		}
		f.waitNoDatabaseConnections(t, target.appName)
	}
}

// TestSystemExecutorProcessSignalDrainEndToEnd sends an OS signal while real
// business is gated. After observing both stopped RPC admissions, it allows the
// already accepted processor to finish and requires its result to reach Job.
// The processor exists only in the System test binary; server assembly, Runtime,
// dependency initialization and shutdown all use the production run function.
func TestSystemExecutorProcessSignalDrainEndToEnd(t *testing.T) {
	f := newJobLifecycleFixture(t)
	node := f.startJob(t, "drain-job")
	f.createGroup(t, node)
	executor := f.startSystem(t, "drain-system-executor", true)
	jobID, instanceID := f.trigger(t, node, "gated")
	accepted := f.waitRunning(t, node, instanceID, node.process, executor.process)
	management := lifecycleConnection(t, executor.managementPort)
	execution := lifecycleConnection(t, executor.executorPort)
	lifecycleWaitHealth(t, management, executor.process)
	lifecycleWaitHealth(t, execution, executor.process)

	signalE2EProcess(t, executor.process)
	for _, conn := range []*grpc.ClientConn{management, execution} {
		jobE2EEventually(t, []*runningProcess{node.process, executor.process}, "stopped System RPC admission", func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
			return status.Code(err) == codes.Unavailable
		})
	}
	if jobE2EBusinessState(t, f.sqlDB, accepted.Id, 1) != "running" {
		t.Fatal("business completed before shutdown admission was observed")
	}
	f.releaseBusiness(t, accepted.Id)
	if err := waitE2EProcessExit(t, executor.process, 15*time.Second); err != nil {
		t.Fatal(executor.process.failure())
	}
	f.waitSuccess(t, node, jobID, instanceID, accepted.Id, node.process)
	f.waitNoDatabaseConnections(t, executor.appName)
	lifecycleAssertListenerClosed(t, executor.managementPort)
	lifecycleAssertListenerClosed(t, executor.executorPort)
}
