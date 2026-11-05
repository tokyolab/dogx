package execution

import (
	"bytes"
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
	"google.golang.org/grpc/status"
)

func TestNewExecutorClientRejectsNativeCredentials(t *testing.T) {
	// These values are deliberately synthetic and must not enter diagnostics.
	const credential = "synthetic-native-executor-credential"
	for _, config := range []zrpc.RpcClientConf{
		{App: credential}, {Token: credential}, {App: credential, Token: credential},
	} {
		client, err := NewExecutorClient(config, DefaultDispatchRPCTimeout)
		if err == nil || client != nil || strings.Contains(err.Error(), credential) {
			t.Fatal("native credentials were not rejected without disclosure")
		}
	}
	if client, err := NewExecutorClient(zrpc.RpcClientConf{NonBlock: true}, DefaultDispatchRPCTimeout); err == nil || client != nil {
		t.Fatal("a missing target was accepted")
	}
}

func TestExecutorClientTransportBudgetsAndCredentialLogging(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	const token = "synthetic-executor-body-secret"
	type observedCall struct {
		deadline time.Time
		auth     *protocol.GroupAuth
		metadata metadata.MD
	}
	observed := make(chan observedCall, 1)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		deadline, _ := ctx.Deadline()
		md, _ := metadata.FromIncomingContext(ctx)
		var auth *protocol.GroupAuth
		switch request := req.(type) {
		case *protocol.DispatchTaskRequest:
			auth = request.Auth
		case *protocol.CancelInstanceRequest:
			auth = request.Auth
		}
		observed <- observedCall{deadline, auth, md.Copy()}
		return nil, status.Error(codes.Unavailable, "synthetic executor unavailable")
	}))
	protocol.RegisterExecutorServer(server, &protocol.UnimplementedExecutorServer{})
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()

	var logs executorClientLogBuffer
	previousWriter := logx.Reset()
	logx.SetWriter(logx.NewWriter(&logs))
	t.Cleanup(func() { logx.SetWriter(previousWriter) })
	client, err := NewExecutorClient(zrpc.RpcClientConf{
		Endpoints: []string{listener.Addr().String()}, NonBlock: true,
		Timeout: 2000, Middlewares: zrpc.ClientMiddlewaresConf{Duration: true},
	}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Conn().Close() })
	executor := protocol.NewExecutorClient(client.Conn())
	auth := &protocol.GroupAuth{GroupName: "test-group", Token: token}
	for _, cancelCall := range []bool{false, true} {
		started := time.Now()
		budget := 5 * time.Second
		if cancelCall {
			budget = time.Second
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			_, err = executor.CancelInstance(ctx, &protocol.CancelInstanceRequest{Auth: auth}, grpc.WaitForReady(true))
			cancel()
		} else {
			_, err = executor.DispatchTask(context.Background(), &protocol.DispatchTaskRequest{Auth: auth}, grpc.WaitForReady(true))
		}
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("call error = %v", err)
		}
		call := <-observed
		if actual := call.deadline.Sub(started); actual < budget-250*time.Millisecond || actual > budget+250*time.Millisecond {
			t.Fatalf("RPC deadline budget = %v, want %v", actual, budget)
		}
		if call.auth.GetToken() != token || len(call.metadata.Get("app")) != 0 || len(call.metadata.Get("token")) != 0 {
			t.Fatal("credentials did not remain exclusively in the protocol body")
		}
	}
	output := logs.String()
	if !strings.Contains(output, "DispatchTask") || !strings.Contains(output, "CancelInstance") || strings.Contains(output, token) || strings.Contains(output, "test-group") {
		t.Fatal("failure logging omitted a method or disclosed a request body")
	}
}

type executorClientLogBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *executorClientLogBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(value)
}

func (b *executorClientLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}
