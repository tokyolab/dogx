package rpchost

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	scenarioEnv = "DOGX_RPCHOST_TEST_SCENARIO"
	workMethod  = "/rpchost.test.Work/Run"
)

func TestServerLifecycle(t *testing.T) {
	// zrpc owns process-wide shutdown listeners. Isolated processes exercise
	// real transports without sharing those listeners with unrelated tests.
	for _, scenario := range []string{
		"two_servers_drain", "timeout_handler", "force_stop_budget",
		"before_start", "during_registration", "startup_panic",
	} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestServerProcess$", "-test.v")
			cmd.Env = append(os.Environ(), scenarioEnv+"="+scenario)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("server scenario failed: %v\n%s", err, output)
			}
		})
	}
}

func TestServerProcess(t *testing.T) {
	switch os.Getenv(scenarioEnv) {
	case "two_servers_drain":
		testTwoServersDrain(t)
	case "timeout_handler":
		testTimeoutHandler(t)
	case "force_stop_budget":
		testForceStopBudget(t)
	case "before_start":
		testShutdownBeforeStart(t)
	case "during_registration":
		testShutdownDuringRegistration(t)
	case "startup_panic":
		testStartupPanic(t)
	default:
		t.Skip("subprocess helper")
	}
}

func testTwoServersDrain(t *testing.T) {
	release := make(chan struct{})
	servers := make([]*Server, 2)
	calls := make([]<-chan error, len(servers))
	shutdowns := make([]<-chan error, len(servers))
	var intercepted atomic.Int32
	for i := range servers {
		entered := make(chan struct{})
		var address string
		servers[i], address = newTestServer(t, 0, func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
		servers[i].AddOptions(grpc.MaxConcurrentStreams(8))
		servers[i].AddUnaryInterceptors(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if info.FullMethod == workMethod {
				intercepted.Add(1)
			}
			return handler(ctx, req)
		})
		client := startTestServer(t, servers[i], address)
		calls[i] = callWork(client)
		waitSignal(t, entered)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i, server := range servers {
		shutdowns[i] = shutdownAsync(ctx, server)
	}
	for _, server := range servers {
		// Shutdown closes admission before waiting for an existing handler.
		waitForStopping(t, server)
		_, err := server.trackUnary(context.Background(), nil, nil, func(context.Context, any) (any, error) {
			t.Fatal("handler entered after shutdown")
			return nil, nil
		})
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("late admission = %v, want Unavailable", err)
		}
	}
	for _, done := range shutdowns {
		select {
		case err := <-done:
			t.Fatalf("shutdown finished with active work: %v", err)
		default:
		}
	}
	close(release)
	for i, done := range shutdowns {
		if err := waitError(t, done); err != nil {
			t.Fatalf("shutdown %d: %v", i, err)
		}
		if err := waitError(t, calls[i]); err != nil {
			t.Fatalf("work %d: %v", i, err)
		}
		if err := servers[i].Shutdown(ctx); err != nil {
			t.Fatalf("repeated shutdown %d: %v", i, err)
		}
	}
	if got := intercepted.Load(); got != 2 {
		t.Fatalf("intercepted work calls = %d, want 2", got)
	}
}

func testTimeoutHandler(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server, address := newTestServer(t, 50, func(context.Context) error {
		close(entered)
		<-release
		return nil
	})
	client := startTestServer(t, server, address)
	call := callWork(client)
	waitSignal(t, entered)
	if err := waitError(t, call); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("zrpc timeout result = %v, want DeadlineExceeded", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := server.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown with an orphaned handler = %v, want DeadlineExceeded", err)
	}
	// The wire call has ended, but dependency cleanup is still unsafe until
	// the actual handler returns from inside zrpc's timeout goroutine.
	close(release)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown after handler return: %v", err)
	}
}

func testForceStopBudget(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	server, address := newTestServer(t, 0, func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return nil
	})
	client := startTestServer(t, server, address)
	call := callWork(client)
	waitSignal(t, entered)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	done := shutdownAsync(ctx, server)
	waitForStopping(t, server)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, canceled)
	if err := waitError(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown ignoring cancellation = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("shutdown exceeded its context budget: %s", elapsed)
	}
	if err := waitError(t, call); err == nil {
		t.Fatal("closed client unexpectedly completed work")
	}
	close(release)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown after releasing ignored cancellation: %v", err)
	}
}

func testShutdownBeforeStart(t *testing.T) {
	var registered atomic.Bool
	server, err := NewServer(testConfig(t, 0), func(*grpc.Server) { registered.Store(true) })
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	server.Start()
	if registered.Load() {
		t.Fatal("registration ran after shutdown completed")
	}
	assertPanic(t, server.Start)
	assertPanic(t, func() { server.AddOptions(grpc.MaxConcurrentStreams(1)) })
	assertPanic(t, func() { server.AddUnaryInterceptors() })
	if _, err := NewServer(testConfig(t, 0), nil); err == nil {
		t.Fatal("nil registration was accepted")
	}
}

func testShutdownDuringRegistration(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	config := testConfig(t, 0)
	server, err := NewServer(config, func(*grpc.Server) {
		close(entered)
		<-release
	})
	if err != nil {
		t.Fatal(err)
	}
	go server.Start()
	waitSignal(t, entered)
	assertPanic(t, func() { server.AddOptions() })
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := server.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown during blocked registration = %v, want DeadlineExceeded", err)
	}
	close(release)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown after registration: %v", err)
	}
	client, err := zrpc.NewClient(zrpc.RpcClientConf{Endpoints: []string{config.ListenOn}, NonBlock: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Conn().Close() }()
	healthCtx, healthCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer healthCancel()
	if _, err := grpc_health_v1.NewHealthClient(client.Conn()).Check(healthCtx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true)); err == nil {
		t.Fatal("late registration reopened the stopped server")
	}
}

func testStartupPanic(t *testing.T) {
	sentinel := errors.New("registration failed")
	server, err := NewServer(testConfig(t, 0), func(*grpc.Server) { panic(sentinel) })
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recovered, ok := recover().(error); !ok || !errors.Is(recovered, sentinel) {
				t.Fatalf("startup panic = %v, want original error", recovered)
			}
		}()
		server.Start()
		t.Fatal("startup panic was swallowed")
	}()
}

func testConfig(t *testing.T, timeoutMillis int64) zrpc.RpcServerConf {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return zrpc.RpcServerConf{
		ServiceConf: service.ServiceConf{
			Name: "rpchost-test",
			Mode: service.TestMode,
			Log:  logx.LogConf{Mode: "console", Level: "error", Encoding: "plain"},
		},
		ListenOn: address,
		Health:   true,
		Timeout:  timeoutMillis,
	}
}

func newTestServer(t *testing.T, timeoutMillis int64, run func(context.Context) error) (*Server, string) {
	t.Helper()
	config := testConfig(t, timeoutMillis)
	server, err := NewServer(config, func(server *grpc.Server) {
		server.RegisterService(&workServiceDesc, &workService{run: run})
	})
	if err != nil {
		t.Fatal(err)
	}
	return server, config.ListenOn
}

func startTestServer(t *testing.T, server *Server, address string) *grpc.ClientConn {
	t.Helper()
	go server.Start()
	client, err := zrpc.NewClient(zrpc.RpcClientConf{Endpoints: []string{address}, NonBlock: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Conn().Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := grpc_health_v1.NewHealthClient(client.Conn()).Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("health status = %s, want SERVING", response.GetStatus())
	}
	return client.Conn()
}

func callWork(client *grpc.ClientConn) <-chan error {
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		done <- client.Invoke(ctx, workMethod, &emptypb.Empty{}, &emptypb.Empty{})
	}()
	return done
}

func shutdownAsync(ctx context.Context, server *Server) <-chan error {
	done := make(chan error, 1)
	go func() { done <- server.Shutdown(ctx) }()
	return done
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server progress")
	}
}

func waitError(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server result")
		return nil
	}
}

func waitForStopping(t *testing.T, server *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		server.mu.Lock()
		stopping := server.stopping
		server.mu.Unlock()
		if stopping {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("shutdown did not close admission")
		case <-time.After(time.Millisecond):
		}
	}
}

func assertPanic(t *testing.T, action func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("operation did not panic")
		}
	}()
	action()
}

type workRunner interface {
	Run(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
}

type workService struct {
	run func(context.Context) error
}

func (s *workService) Run(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.run(ctx)
}

var workServiceDesc = grpc.ServiceDesc{
	ServiceName: "rpchost.test.Work",
	HandlerType: (*workRunner)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Run",
		Handler: func(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			request := &emptypb.Empty{}
			if err := decode(request); err != nil {
				return nil, err
			}
			handler := func(ctx context.Context, request any) (any, error) {
				return srv.(workRunner).Run(ctx, request.(*emptypb.Empty))
			}
			if interceptor == nil {
				return handler(ctx, request)
			}
			return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: srv, FullMethod: workMethod}, handler)
		},
	}},
}
