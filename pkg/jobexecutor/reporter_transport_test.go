package jobexecutor

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/subcode"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestReporterTransportRetriesTheSameResultAndRecognizesBusinessRejection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	receiver := &reportTransportReceiver{calls: make(map[int64][]reportTransportCall)}
	server := grpc.NewServer(grpc.UnaryInterceptor(bizerror.UnaryServerInterceptor))
	protocol.RegisterResultReceiverServer(server, receiver)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	client, err := NewReportClient(zrpc.RpcClientConf{
		Target: "passthrough:///" + listener.Addr().String(), NonBlock: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Conn().Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	r := startTestReporter(t, protocol.NewResultReceiverClient(client.Conn()), ctx, 2, 2)
	result := reporterResult(1)
	result.data, result.summary = json.RawMessage(`{"count":9007199254740993}`), "transport result"
	if err := r.submit(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	if err := r.submit(context.Background(), reporterResult(2)); err != nil {
		t.Fatal(err)
	}
	r.stop()
	if err := r.wait(ctx); err != nil {
		t.Fatalf("drain actual result RPCs: %v", err)
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	retried, rejected := receiver.calls[1], receiver.calls[2]
	if len(retried) != 2 || len(rejected) != 1 {
		t.Fatalf("actual transport calls: retried=%d rejected=%d", len(retried), len(rejected))
	}
	if !proto.Equal(retried[0].request, retried[1].request) {
		t.Fatal("temporary transport failure changed the retried business result")
	}
	request := retried[1].request
	if request.Auth.GetGroupName() != adapterTestGroup || request.Auth.GetToken() != adapterTestToken ||
		request.ExecutorInstanceId != adapterTestID || request.AttemptNo != 3 ||
		request.Outcome != protocol.TaskOutcome_SUCCESS || string(request.GetDataJson()) != string(result.data) {
		t.Fatal("actual transport changed credentials, attempt identity, outcome or JSON precision")
	}
	for _, call := range append(retried, rejected...) {
		if call.budget <= 0 || call.budget > 2*time.Second {
			t.Fatalf("actual report deadline budget = %v", call.budget)
		}
	}
}

type reportTransportCall struct {
	request *protocol.ReportTaskResultRequest
	budget  time.Duration
}

// reportTransportReceiver verifies native protobuf and status detail propagation
// through a loopback gRPC server and the production report client factory.
type reportTransportReceiver struct {
	protocol.UnimplementedResultReceiverServer
	mu    sync.Mutex
	calls map[int64][]reportTransportCall
}

func (s *reportTransportReceiver) ReportTaskResult(ctx context.Context, request *protocol.ReportTaskResultRequest) (*protocol.ReportTaskResultResponse, error) {
	deadline, _ := ctx.Deadline()
	s.mu.Lock()
	s.calls[request.TaskId] = append(s.calls[request.TaskId], reportTransportCall{
		request: proto.Clone(request).(*protocol.ReportTaskResultRequest), budget: time.Until(deadline),
	})
	attempt := len(s.calls[request.TaskId])
	s.mu.Unlock()
	if request.TaskId == 2 {
		return nil, bizerror.New(subcode.InvalidRequest, "synthetic invalid result")
	}
	if attempt == 1 {
		return nil, status.Error(codes.ResourceExhausted, "synthetic result buffer is full")
	}
	return &protocol.ReportTaskResultResponse{}, nil
}
