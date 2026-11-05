package jobexecutor

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

func TestNewReportClientRejectsNativeCredentials(t *testing.T) {
	// These values are deliberately synthetic and must not enter diagnostics.
	const credential = "synthetic-native-report-credential"
	for _, config := range []zrpc.RpcClientConf{
		{App: credential}, {Token: credential}, {App: credential, Token: credential},
	} {
		client, err := NewReportClient(config)
		if err == nil || client != nil || strings.Contains(err.Error(), credential) {
			t.Fatal("native credentials were not rejected without disclosure")
		}
	}
	if client, err := NewReportClient(zrpc.RpcClientConf{NonBlock: true}); err == nil || client != nil {
		t.Fatal("a missing target was accepted")
	}
}

func TestReportClientDoesNotWaitForUnavailableScheduler(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	client, err := NewReportClient(zrpc.RpcClientConf{
		Target: "passthrough:///" + address, NonBlock: false,
	})
	if err != nil {
		t.Fatalf("unavailable scheduler prevented report client construction: %v", err)
	}
	t.Cleanup(func() { _ = client.Conn().Close() })
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("report client waited for scheduler connectivity: %v", elapsed)
	}
	// Nonblocking construction does not acknowledge work or invent connectivity.
	// WaitForReady remains bounded by the caller while no scheduler is listening.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	response, err := protocol.NewResultReceiverClient(client.Conn()).ReportTaskResult(ctx,
		&protocol.ReportTaskResultRequest{}, grpc.WaitForReady(true))
	if response != nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("unavailable scheduler produced an unexpected report result: response=%v error=%v", response, err)
	}
}

func TestReportClientTransportBudgetAndCredentialLogging(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	const token = "synthetic-report-body-secret"
	type observedCall struct {
		deadline time.Time
		auth     *protocol.GroupAuth
		metadata metadata.MD
	}
	observed := make(chan observedCall, 1)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		deadline, _ := ctx.Deadline()
		md, _ := metadata.FromIncomingContext(ctx)
		observed <- observedCall{deadline, req.(*protocol.ReportTaskResultRequest).Auth, md.Copy()}
		return nil, status.Error(codes.Unavailable, "synthetic result receiver unavailable")
	}))
	protocol.RegisterResultReceiverServer(server, &protocol.UnimplementedResultReceiverServer{})
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()

	var logs reportClientLogBuffer
	previousWriter := logx.Reset()
	logx.SetWriter(logx.NewWriter(&logs))
	t.Cleanup(func() { logx.SetWriter(previousWriter) })
	client, err := NewReportClient(zrpc.RpcClientConf{
		Target: "passthrough:///" + listener.Addr().String(), NonBlock: true,
		Timeout: 15000, Middlewares: zrpc.ClientMiddlewaresConf{Duration: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Conn().Close() })
	started := time.Now()
	_, err = protocol.NewResultReceiverClient(client.Conn()).ReportTaskResult(context.Background(),
		&protocol.ReportTaskResultRequest{Auth: &protocol.GroupAuth{GroupName: "test-group", Token: token}}, grpc.WaitForReady(true))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("call error = %v", err)
	}
	call := <-observed
	if budget := call.deadline.Sub(started); budget < 1750*time.Millisecond || budget > 2250*time.Millisecond {
		t.Fatalf("report deadline budget = %v, want two seconds", budget)
	}
	if call.auth.GetToken() != token || len(call.metadata.Get("app")) != 0 || len(call.metadata.Get("token")) != 0 {
		t.Fatal("credentials did not remain exclusively in the protocol body")
	}
	output := logs.String()
	if !strings.Contains(output, "ReportTaskResult") || strings.Contains(output, token) || strings.Contains(output, "test-group") {
		t.Fatal("failure logging omitted the method or disclosed the request body")
	}
}

type reportClientLogBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *reportClientLogBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(value)
}

func (b *reportClientLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}
