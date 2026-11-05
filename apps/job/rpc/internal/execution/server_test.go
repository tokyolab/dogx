package execution

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/rpchost"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Each transport test process owns go-zero's global logging and shutdown hooks.
// Unix zrpc Start waits for process shutdown callbacks even after gRPC stops;
// a test must not mistake waiting for Start for an inbound completion barrier.
func TestExecutionServerTransport(t *testing.T) {
	if os.Getenv("DOGX_EXECUTION_TRANSPORT_TEST") == "1" {
		testExecutionServerTransport(t)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExecutionServerTransport$", "-test.v")
	cmd.Env = append(os.Environ(), "DOGX_EXECUTION_TRANSPORT_TEST=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("transport subprocess: %v\n%s", err, output)
	}
}

func testExecutionServerTransport(t *testing.T) {
	registry := jobexecutor.NewRegistry()
	processor := &transportProcessor{}
	if err := registry.Register("test.process", processor); err != nil {
		t.Fatal(err)
	}
	auth, err := jobexecutor.NewAuthenticator("test-workers", receiverTestToken)
	if err != nil {
		t.Fatal(err)
	}
	admission := &transportAdmission{work: make(chan jobexecutor.Invocation, 1)}
	adapter, err := jobexecutor.NewRPCAdapter(auth, registry, receiverTestID, admission)
	if err != nil {
		t.Fatal(err)
	}
	groupAuth := &transportGroupAuth{}
	results := &transportResults{reports: make(chan taskresult.Report, 1)}
	receiver, err := NewReceiver(groupAuth, results)
	if err != nil {
		t.Fatal(err)
	}

	executorConfig := transportServerConfig(t)
	resultConfig := transportServerConfig(t)
	businessConfig := transportServerConfig(t)
	// A single source supplies process settings; listener settings stay distinct.
	resultConfig.ServiceConf = executorConfig.ServiceConf
	businessConfig.ServiceConf = executorConfig.ServiceConf
	executor, err := jobexecutor.NewServer(executorConfig, func() *jobexecutor.RPCAdapter { return adapter })
	if err != nil {
		t.Fatal(err)
	}
	resultServer, err := NewServer(resultConfig, func() *Receiver { return receiver })
	if err != nil {
		t.Fatal(err)
	}
	business, err := rpchost.NewServer(businessConfig, func(*grpc.Server) {})
	if err != nil {
		t.Fatal(err)
	}
	var logs transportLogBuffer
	logx.SetWriter(logx.NewWriter(&logs))
	for _, server := range []*rpchost.Server{business, executor, resultServer} {
		go server.Start()
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for _, server := range []*rpchost.Server{executor, resultServer, business} {
			if err := server.Shutdown(ctx); err != nil {
				t.Errorf("stop transport server: %v", err)
			}
		}
		if err := logx.Close(); err != nil {
			t.Errorf("close transport test logs: %v", err)
		}
	})
	executorConn := transportClient(t, executorConfig.ListenOn, false)
	resultConn := transportClient(t, resultConfig.ListenOn, true)
	businessConn := transportClient(t, businessConfig.ListenOn, false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, conn := range []*grpc.ClientConn{executorConn, resultConn, businessConn} {
		if _, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true)); err != nil {
			t.Fatalf("credential-free health: %v", err)
		}
	}
	executorClient := protocol.NewExecutorClient(executorConn)
	resultClient := protocol.NewResultReceiverClient(resultConn)
	request := &protocol.DispatchTaskRequest{
		Auth:  &protocol.GroupAuth{GroupName: "test-workers", Token: receiverTestToken},
		JobId: 11, InstanceId: 22, TaskId: 33, AttemptNo: 1,
		ProcessorName: "test.process", Params: "synthetic-private-input",
		DeadlineAt: timestamppb.New(time.Now().Add(time.Minute)),
		Input:      &protocol.DispatchTaskRequest_Ordinary{Ordinary: &protocol.OrdinaryInput{}},
	}
	if _, err := executorClient.DispatchTask(ctx, &protocol.DispatchTaskRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated dispatch: %v", err)
	}
	var remote peer.Peer
	ack, err := executorClient.DispatchTask(ctx, request, grpc.Peer(&remote))
	if err != nil || ack.GetStatus() != protocol.DispatchStatus_ACCEPTED || ack.GetExecutorInstanceId() != receiverTestID {
		t.Fatalf("dispatch ACK: %v, %v", ack, err)
	}
	if remote.Addr == nil || remote.Addr.String() != executorConfig.ListenOn {
		t.Fatalf("peer = %v, want listening endpoint %s", remote.Addr, executorConfig.ListenOn)
	}
	if processor.calls.Load() != 0 {
		t.Fatal("transport adapter executed business code synchronously")
	}
	busyRequest := proto.Clone(request).(*protocol.DispatchTaskRequest)
	busyRequest.TaskId++
	busy, err := executorClient.DispatchTask(ctx, busyRequest)
	if err != nil || busy.GetStatus() != protocol.DispatchStatus_BUSY || busy.GetExecutorInstanceId() != "" {
		t.Fatalf("capacity refusal: %v, %v", busy, err)
	}
	if _, err := executorClient.CancelInstance(ctx, &protocol.CancelInstanceRequest{
		Auth: request.Auth, InstanceId: 22, Reason: protocol.CancelReason_MANUAL_STOP,
	}); err != nil || admission.cancels.Load() != 1 {
		t.Fatalf("instance cancellation: %v", err)
	}
	report := validReceiverRequest()
	report.Summary = "synthetic-private-result"
	if _, err := resultClient.ReportTaskResult(ctx, report); err != nil {
		t.Fatalf("report ACK: %v", err)
	}
	if _, err := resultClient.ReportTaskResult(ctx, report); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("full result buffer: %v", err)
	}
	retained := <-results.reports
	if retained.GroupID != 41 || retained.TaskID != report.TaskId {
		t.Fatalf("trusted result identity: %+v", retained)
	}
	groupAuth.unavailable.Store(true)
	if _, err := resultClient.ReportTaskResult(ctx, report); status.Code(err) != codes.Unavailable {
		t.Fatalf("unavailable authentication dependency: %v", err)
	}
	if _, err := grpc_health_v1.NewHealthClient(businessConn).Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatalf("independent listener stopped during result dependency failure: %v", err)
	}
	if _, err := protocol.NewExecutorClient(businessConn).DispatchTask(ctx, request); status.Code(err) != codes.Unimplemented {
		t.Fatalf("business listener exposed execution method: %v", err)
	}
	if _, err := protocol.NewExecutorClient(resultConn).DispatchTask(ctx, request); status.Code(err) != codes.Unimplemented {
		t.Fatalf("result listener exposed dispatch: %v", err)
	}
	if _, err := protocol.NewResultReceiverClient(executorConn).ReportTaskResult(ctx, report); status.Code(err) != codes.Unimplemented {
		t.Fatalf("executor listener exposed result receiver: %v", err)
	}
	for _, conn := range []*grpc.ClientConn{executorConn, resultConn} {
		stream, err := grpc_reflection_v1.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
		if err == nil {
			err = stream.Send(&grpc_reflection_v1.ServerReflectionRequest{MessageRequest: &grpc_reflection_v1.ServerReflectionRequest_ListServices{ListServices: ""}})
			// The server may reject the unknown stream before Send. gRPC reports
			// a server-side send failure as io.EOF; Recv obtains the final RPC
			// status. EOF alone does not establish that reflection is disabled.
			if err == nil || errors.Is(err, io.EOF) {
				_, err = stream.Recv()
			}
		}
		if status.Code(err) != codes.Unimplemented {
			t.Fatalf("execution reflection should be absent: %v", err)
		}
	}
	// Force all calls into the stat interceptor's slow path, then ensure both
	// successful and error calls retain method diagnostics without their bodies.
	output := logs.String()
	if !strings.Contains(output, protocol.Executor_DispatchTask_FullMethodName) || !strings.Contains(output, protocol.ResultReceiver_ReportTaskResult_FullMethodName) {
		t.Fatal("expected RPC diagnostics were not captured")
	}
	for _, secret := range []string{receiverTestToken, request.Params, report.Summary} {
		if strings.Contains(output, secret) {
			t.Fatal("execution RPC log contains credential or business content")
		}
	}
}

func transportServerConfig(t *testing.T) zrpc.RpcServerConf {
	t.Helper()
	var c zrpc.RpcServerConf
	if err := conf.FillDefault(&c); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c.ListenOn = listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	c.Name, c.Mode = "execution-transport-test", service.TestMode
	// Only the explicitly bound loopback RPC listeners belong to this fixture.
	// go-zero otherwise starts its default all-interface development HTTP server.
	c.DevServer.Enabled = false
	c.Middlewares.StatConf.SlowThreshold = time.Nanosecond
	c.CpuThreshold = 0
	return c
}

func transportClient(t *testing.T, address string, report bool) *grpc.ClientConn {
	t.Helper()
	var c zrpc.RpcClientConf
	if err := conf.FillDefault(&c); err != nil {
		t.Fatal(err)
	}
	c.Endpoints = []string{address}
	var client zrpc.Client
	var err error
	if report {
		client, err = jobexecutor.NewReportClient(c)
	} else {
		client, err = NewExecutorClient(c, DefaultDispatchRPCTimeout)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Conn().Close(); err != nil {
			t.Errorf("close test client: %v", err)
		}
	})
	return client.Conn()
}

type transportAdmission struct {
	work    chan jobexecutor.Invocation
	cancels atomic.Int64
}

func (a *transportAdmission) Accept(_ context.Context, input jobexecutor.Invocation) (bool, error) {
	select {
	case a.work <- input:
		return true, nil
	default:
		return false, nil
	}
}

func (a *transportAdmission) Cancel(context.Context, int64, jobexecutor.CancelReason) error {
	a.cancels.Add(1)
	return nil
}

type transportProcessor struct{ calls atomic.Int64 }

func (p *transportProcessor) Execute(context.Context, jobexecutor.Input) (jobexecutor.Result, error) {
	p.calls.Add(1)
	return jobexecutor.Result{}, nil
}

type transportGroupAuth struct{ unavailable atomic.Bool }

func (a *transportGroupAuth) Authenticate(_ context.Context, group, token string) (int64, error) {
	if a.unavailable.Load() {
		return 0, errors.New("synthetic authentication lookup failure")
	}
	if group != "test-workers" || token != receiverTestToken {
		return 0, ErrInvalidCredentials
	}
	return 41, nil
}

type transportResults struct{ reports chan taskresult.Report }

func (r *transportResults) Accept(_ context.Context, report taskresult.Report) (bool, error) {
	select {
	case r.reports <- report:
		return true, nil
	default:
		return false, nil
	}
}

type transportLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *transportLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *transportLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}
