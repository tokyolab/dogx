package execution

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/dispatch"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestSenderTransportUsesActualPeerAndReusesBorrowedClient(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	executor := &senderTransportExecutor{}
	server := grpc.NewServer(grpc.UnaryInterceptor(bizerror.UnaryServerInterceptor))
	protocol.RegisterExecutorServer(server, executor)
	grpc_health_v1.RegisterHealthServer(server, health.NewServer())
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	var logs executorClientLogBuffer
	previous := logx.Reset()
	logx.SetWriter(logx.NewWriter(&logs))
	t.Cleanup(func() { logx.SetWriter(previous) })
	connection, err := NewExecutorClient(zrpc.RpcClientConf{
		Endpoints: []string{listener.Addr().String()}, NonBlock: true,
		Middlewares: zrpc.ClientMiddlewaresConf{Duration: true},
	}, DefaultDispatchRPCTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Conn().Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := grpc_health_v1.NewHealthClient(connection.Conn()).Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("wait for test executor: %v", err)
	}
	client := protocol.NewExecutorClient(connection.Conn())
	provider := &senderProviderStub{target: senderTarget(client)}
	sender := newTestSender(t, provider)
	attempt := senderAttempt()
	for _, test := range []struct {
		taskID int64
		want   dispatch.Outcome
	}{
		{33, dispatch.OutcomeAccepted},
		{34, dispatch.OutcomeAccepted},
		{35, dispatch.OutcomeBusy},
		{36, dispatch.OutcomeRejected},
		{37, dispatch.OutcomeRejected},
		{38, dispatch.OutcomeUnknown},
		{39, dispatch.OutcomeUnknown},
		{40, dispatch.OutcomeUnknown},
	} {
		attempt.TaskID = test.taskID
		response, err := sender.Dispatch(ctx, attempt)
		if response.Outcome != test.want || (err != nil) != (test.want == dispatch.OutcomeUnknown) {
			t.Fatalf("task %d transport outcome=%v error=%v", test.taskID, response.Outcome, err)
		}
		if test.want == dispatch.OutcomeAccepted && response.ExecutorAddress != listener.Addr().String() {
			t.Fatal("accepted binding did not use the actual selected executor listening address")
		}
		if test.want != dispatch.OutcomeAccepted && response.ExecutorAddress != "" {
			t.Fatal("unaccepted result unexpectedly produced an executor binding")
		}
		if strings.Contains(response.Summary, attempt.ExecutionConfig.Params) || strings.Contains(response.Summary, receiverTestToken) {
			t.Fatal("dispatch summary copied credential or business content")
		}
	}
	executor.mu.Lock()
	observed := append([]senderTransportCall(nil), executor.calls...)
	executor.mu.Unlock()
	if len(observed) != 8 || len(provider.groups) != 8 || provider.target.Client != client {
		t.Fatal("dispatch created replacement clients or automatically retried a registered attempt")
	}
	for _, call := range observed {
		request := call.request
		if request.Auth.GetGroupName() != provider.target.Group.Name || request.Auth.GetToken() != receiverTestToken ||
			request.JobId != attempt.JobID || request.InstanceId != attempt.InstanceID || request.AttemptNo != attempt.AttemptNo ||
			request.Params != attempt.ExecutionConfig.Params || request.ProcessorName != attempt.ExecutionConfig.ProcessorName ||
			!request.DeadlineAt.AsTime().Equal(attempt.DeadlineAt) || request.GetOrdinary() == nil {
			t.Fatal("real dispatch transport changed frozen input or protocol credentials")
		}
		if len(call.metadata.Get("app")) != 0 || len(call.metadata.Get("token")) != 0 {
			t.Fatal("group credentials escaped into native RPC authentication metadata")
		}
		if call.budget <= 0 || call.budget > 3*time.Second {
			t.Fatalf("real dispatch call budget=%v", call.budget)
		}
	}
	output := logs.String()
	if strings.Contains(output, receiverTestToken) || strings.Contains(output, attempt.ExecutionConfig.Params) {
		t.Fatal("dispatch client logged credentials or frozen input")
	}
	// The borrowed connection still serves calls after all Sender operations.
	if _, err := grpc_health_v1.NewHealthClient(connection.Conn()).Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatalf("sender closed the provider-owned connection: %v", err)
	}
}

type senderTransportCall struct {
	request  *protocol.DispatchTaskRequest
	metadata metadata.MD
	budget   time.Duration
}

// senderTransportExecutor records protocol traffic on one real loopback listener;
// individual Task IDs select responses without invoking a production processor.
type senderTransportExecutor struct {
	protocol.UnimplementedExecutorServer
	mu    sync.Mutex
	calls []senderTransportCall
}

func (s *senderTransportExecutor) DispatchTask(ctx context.Context, request *protocol.DispatchTaskRequest) (*protocol.DispatchTaskResponse, error) {
	deadline, _ := ctx.Deadline()
	md, _ := metadata.FromIncomingContext(ctx)
	s.mu.Lock()
	s.calls = append(s.calls, senderTransportCall{request: proto.Clone(request).(*protocol.DispatchTaskRequest), metadata: md.Copy(), budget: time.Until(deadline)})
	s.mu.Unlock()
	switch request.TaskId {
	case 35:
		return &protocol.DispatchTaskResponse{Status: protocol.DispatchStatus_BUSY}, nil
	case 36:
		return nil, bizerror.New(protocol.DispatchRejected, "synthetic dispatch refusal")
	case 37:
		return nil, status.Error(codes.Unauthenticated, "synthetic authentication refusal")
	case 38:
		st, err := status.New(codes.Code(bizerror.DefaultCode+1), "synthetic unknown code").WithDetails(&errdetails.ErrorInfo{Reason: protocol.DispatchRejected})
		if err != nil {
			return nil, err
		}
		return nil, st.Err()
	case 39:
		return nil, status.Error(codes.ResourceExhausted, "BUSY")
	case 40:
		return &protocol.DispatchTaskResponse{Status: protocol.DispatchStatus_ACCEPTED, ExecutorInstanceId: "invalid"}, nil
	default:
		return senderAcceptedACK(), nil
	}
}
