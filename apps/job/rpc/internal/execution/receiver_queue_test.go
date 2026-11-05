package execution

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestReceiverQueueTransportAdmissionAndLifetime(t *testing.T) {
	if queueReceiverSubprocess(t, "DOGX_RESULT_QUEUE_TRANSPORT_TEST", "TestReceiverQueueTransportAdmissionAndLifetime") {
		return
	}
	t.Cleanup(func() { _ = logx.Close() })
	saver := &queueTransportSaver{entered: make(chan queueSaveCall, 1), release: make(chan struct{})}
	queue, err := taskresult.NewQueue(saver, taskresult.QueueConfig{Capacity: 1, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		saver.unblock()
		stopReceiverQueue(t, queue)
	})
	receiver := testReceiver(t, &transportGroupAuth{}, queue)
	inboundContexts := make(chan context.Context, 1)
	client := queueReceiverClient(t, receiver, func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if req, ok := request.(*protocol.ReportTaskResultRequest); ok && req.TaskId == 33 {
			inboundContexts <- ctx
		}
		return handler(ctx, request)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request := validReceiverRequest()
	request.TaskId = 32
	if _, err := client.ReportTaskResult(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatalf("queue before Start: %v", err)
	}
	if err := queue.Start(); err != nil {
		t.Fatal(err)
	}
	request.TaskId = 33
	rpcCtx, rpcCancel := context.WithCancel(ctx)
	defer rpcCancel()
	if _, err := client.ReportTaskResult(rpcCtx, request); err != nil {
		t.Fatalf("ACK waited for blocked persistence: %v", err)
	}
	rpcCancel()
	var call queueSaveCall
	select {
	case call = <-saver.entered:
	case <-ctx.Done():
		t.Fatal("accepted report never reached the real queue's saver")
	}
	var inbound context.Context
	select {
	case inbound = <-inboundContexts:
	case <-ctx.Done():
		t.Fatal("inbound RPC context was not observed")
	}
	// Wait for the server-side request to finish, rather than assuming that a
	// local client cancellation already propagated to the remote handler.
	select {
	case <-inbound.Done():
	case <-ctx.Done():
		t.Fatal("completed RPC did not release its request context")
	}
	if call.ctx.Err() != nil || call.report.GroupID != 41 || call.report.TaskID != 33 {
		t.Fatalf("worker inherited request lifetime or lost trusted identity: ctx=%v group=%d task=%d", call.ctx.Err(), call.report.GroupID, call.report.TaskID)
	}
	request.TaskId = 34
	if _, err := client.ReportTaskResult(ctx, request); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("in-flight result did not occupy bounded retention: %v", err)
	}
	queue.Stop()
	request.TaskId = 35
	if _, err := client.ReportTaskResult(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatalf("stopped queue admission: %v", err)
	}
	if call.ctx.Err() != nil {
		t.Fatalf("Stop canceled accepted work before drain deadline: %v", call.ctx.Err())
	}
	saver.unblock()
	if err := queue.Drain(ctx); err != nil {
		t.Fatalf("drain accepted result: %v", err)
	}
	if err := queue.Wait(ctx); err != nil {
		t.Fatalf("wait for actual worker completion: %v", err)
	}
}

// queueReceiverSubprocess isolates go-zero's process-global shutdown and log
// state. Returning false means the caller is already in the isolated child.
func queueReceiverSubprocess(t *testing.T, environment, name string) bool {
	t.Helper()
	if os.Getenv(environment) == "1" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+name+"$", "-test.v")
	command.Env = append(os.Environ(), environment+"=1")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("queue receiver subprocess: %v\n%s", err, output)
	}
	return true
}

func queueReceiverClient(t *testing.T, receiver *Receiver, interceptors ...grpc.UnaryServerInterceptor) protocol.ResultReceiverClient {
	t.Helper()
	config := transportServerConfig(t)
	server, err := NewServer(config, func() *Receiver { return receiver })
	if err != nil {
		t.Fatal(err)
	}
	server.AddUnaryInterceptors(interceptors...)
	go server.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("stop queue receiver server: %v", err)
		}
	})
	conn := transportClient(t, config.ListenOn, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("wait for queue receiver transport: %v", err)
	}
	return protocol.NewResultReceiverClient(conn)
}

func stopReceiverQueue(t *testing.T, queue *taskresult.Queue) {
	t.Helper()
	queue.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := queue.Drain(ctx); err != nil {
		t.Errorf("drain queue receiver: %v", err)
	}
	if err := queue.Wait(ctx); err != nil {
		t.Errorf("wait for queue receiver workers: %v", err)
	}
}

type queueSaveCall struct {
	ctx    context.Context
	report taskresult.Report
}

// queueTransportSaver blocks actual persistence while the real queue and RPC
// adapter handle admission, retained capacity and independent worker lifetime.
type queueTransportSaver struct {
	entered chan queueSaveCall
	release chan struct{}
	once    sync.Once
}

func (s *queueTransportSaver) Save(ctx context.Context, report taskresult.Report) (taskresult.SaveResult, error) {
	select {
	case s.entered <- queueSaveCall{ctx: ctx, report: report}:
	case <-ctx.Done():
		return taskresult.SaveResult{}, ctx.Err()
	}
	select {
	case <-s.release:
		return taskresult.SaveResult{Applied: true, Reason: "saved"}, nil
	case <-ctx.Done():
		return taskresult.SaveResult{}, ctx.Err()
	}
}

func (s *queueTransportSaver) unblock() { s.once.Do(func() { close(s.release) }) }
