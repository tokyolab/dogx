package jobexecutor

import (
	"context"
	"testing"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/rpchost"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestExecutorServerAppliesConfiguredAdmissionTimeout(t *testing.T) {
	if executorProviderSubprocess(t, "DOGX_EXECUTOR_TIMEOUT_TEST", "TestExecutorServerAppliesConfiguredAdmissionTimeout") {
		return
	}
	config, shutdown := executorProviderConfig(t)
	config.MethodTimeouts = []zrpc.MethodTimeoutConf{
		{FullMethod: protocol.Executor_DispatchTask_FullMethodName, Timeout: 5 * time.Second},
		{FullMethod: protocol.Executor_CancelInstance_FullMethodName, Timeout: 9 * time.Second},
	}
	admission := &serverTimeoutAdmissionStub{budgets: make(chan time.Duration, 3)}
	adapter := newTestRPCAdapter(t, admission)
	server, err := NewServer(config, func() *RPCAdapter { return adapter })
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- rpchost.Run(runCtx, shutdown, rpchost.Hooks{}, server) }()
	t.Cleanup(func() {
		stop()
		select {
		case err := <-finished:
			if err != nil {
				t.Errorf("stop executor timeout host: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("executor timeout host did not finish shutdown")
		}
	})
	conn, err := grpc.NewClient("passthrough:///"+config.ListenOn, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readyCancel()
	if _, err := grpc_health_v1.NewHealthClient(conn).Check(readyCtx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("executor readiness: %v", err)
	}
	client := protocol.NewExecutorClient(conn)
	// Read the deadline seen by admission without waiting for a timeout. The raw
	// client supplies ten seconds, so either the old three-second override or
	// the base two-second interceptor would be observable in this real RPC.
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rpcCancel()
	ack, err := client.DispatchTask(rpcCtx, validAdapterDispatch())
	if err != nil || ack.GetStatus() != protocol.DispatchStatus_ACCEPTED {
		t.Fatalf("configured dispatch: status=%v err=%v", ack.GetStatus(), err)
	}
	if budget := <-admission.budgets; budget <= 3*time.Second || budget > 5*time.Second {
		t.Fatalf("configured admission budget was truncated or absent: %v", budget)
	}
	_, err = client.CancelInstance(rpcCtx, &protocol.CancelInstanceRequest{
		Auth:       &protocol.GroupAuth{GroupName: adapterTestGroup, Token: adapterTestToken},
		InstanceId: 2, Reason: protocol.CancelReason_MANUAL_STOP,
	})
	if err != nil {
		t.Fatalf("cancellation: %v", err)
	}
	if budget := <-admission.budgets; budget <= 0 || budget > time.Second {
		t.Fatalf("cancellation lost its fixed one-second budget: %v", budget)
	}
	shortCtx, shortCancel := context.WithTimeout(context.Background(), time.Second)
	defer shortCancel()
	if _, err := client.DispatchTask(shortCtx, validAdapterDispatch()); err != nil {
		t.Fatal(err)
	}
	if budget := <-admission.budgets; budget <= 0 || budget > time.Second {
		t.Fatalf("configured admission extended the caller deadline: %v", budget)
	}
}

// serverTimeoutAdmissionStub observes the transport Context without doing any
// business work. Its bounded channel records only the three sequential RPCs.
type serverTimeoutAdmissionStub struct{ budgets chan time.Duration }

func (s *serverTimeoutAdmissionStub) Accept(ctx context.Context, _ Invocation) (bool, error) {
	s.record(ctx)
	return true, nil
}

func (s *serverTimeoutAdmissionStub) Cancel(ctx context.Context, _ int64, _ CancelReason) error {
	s.record(ctx)
	return nil
}

func (s *serverTimeoutAdmissionStub) record(ctx context.Context) {
	deadline, ok := ctx.Deadline()
	if !ok {
		s.budgets <- 0
		return
	}
	s.budgets <- time.Until(deadline)
}
