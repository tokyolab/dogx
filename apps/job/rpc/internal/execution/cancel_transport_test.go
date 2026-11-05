package execution

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestCancellerTransportReusesDirectConnectionAndProtectsCredentials(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	executor := &cancelTransportExecutor{}
	server := grpc.NewServer()
	protocol.RegisterExecutorServer(server, executor)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	var logs executorClientLogBuffer
	previous := logx.Reset()
	logx.SetWriter(logx.NewWriter(&logs))
	t.Cleanup(func() { logx.SetWriter(previous) })
	groups := cancelGroupFixture()
	component, err := NewCanceller(groups)
	if err != nil {
		t.Fatal(err)
	}
	created, closed := 0, 0
	component.create = func(config zrpc.RpcClientConf, timeout time.Duration) (*cancelConnection, error) {
		created++
		connection, err := createCancelConnection(config, timeout)
		if err != nil {
			return nil, err
		}
		closeConnection := connection.close
		connection.close = func() error { closed++; return closeConnection() }
		return connection, nil
	}
	target := cancelTargetFixture()
	target.Address = listener.Addr().String()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := component.Cancel(ctx, target); err != nil {
		t.Fatalf("direct cancellation failed: %v", err)
	}
	executor.mu.Lock()
	calls := append([]cancelTransportCall(nil), executor.calls...)
	executor.mu.Unlock()
	if created != 1 || closed != 1 || groups.calls != 1 || len(calls) != 2 || calls[0].remote == "" || calls[0].remote != calls[1].remote {
		t.Fatal("communication retry did not reuse and release one direct connection")
	}
	for _, call := range calls {
		if call.request.InstanceId != target.InstanceID || call.request.Reason != protocol.CancelReason_MANUAL_STOP ||
			call.request.Auth.GetGroupName() != groups.group.Name || call.request.Auth.GetToken() != groups.group.Token ||
			call.budget <= 0 || call.budget > time.Second || len(call.metadata.Get("app")) != 0 || len(call.metadata.Get("token")) != 0 {
			t.Fatal("direct transport changed the Instance notification, credentials or one-second deadline")
		}
	}
	if output := logs.String(); strings.Contains(output, groups.group.Token) || strings.Contains(output, groups.group.Name) {
		t.Fatal("cancellation transport logged group credentials or request content")
	}
}

type cancelTransportCall struct {
	request  *protocol.CancelInstanceRequest
	metadata metadata.MD
	budget   time.Duration
	remote   string
}

// cancelTransportExecutor refuses its first call without closing the connection,
// then acknowledges the next signal without claiming a matching business activity.
type cancelTransportExecutor struct {
	protocol.UnimplementedExecutorServer
	mu    sync.Mutex
	calls []cancelTransportCall
}

func (s *cancelTransportExecutor) CancelInstance(ctx context.Context, request *protocol.CancelInstanceRequest) (*protocol.CancelInstanceResponse, error) {
	deadline, _ := ctx.Deadline()
	md, _ := metadata.FromIncomingContext(ctx)
	var address string
	if remote, ok := peer.FromContext(ctx); ok && remote.Addr != nil {
		address = remote.Addr.String()
	}
	s.mu.Lock()
	s.calls = append(s.calls, cancelTransportCall{request: proto.Clone(request).(*protocol.CancelInstanceRequest), metadata: md.Copy(), budget: time.Until(deadline), remote: address})
	count := len(s.calls)
	s.mu.Unlock()
	if count == 1 {
		return nil, status.Error(codes.Unavailable, "synthetic temporary cancellation failure")
	}
	return &protocol.CancelInstanceResponse{}, nil
}
