package jobexecutor

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/rpchost"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestExecutorServerDefersAdapterUntilHostInitialization(t *testing.T) {
	if executorProviderSubprocess(t, "DOGX_EXECUTOR_PROVIDER_TEST", "TestExecutorServerDefersAdapterUntilHostInitialization") {
		return
	}
	config, shutdown := executorProviderConfig(t)
	var initialized atomic.Pointer[RPCAdapter]
	var calls atomic.Int32
	server, err := NewServer(config, func() *RPCAdapter {
		calls.Add(1)
		return initialized.Load()
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("construction evaluated the adapter before dependencies existed")
	}
	initializing, release := make(chan struct{}), make(chan struct{})
	finishInitialization := sync.OnceFunc(func() { close(release) })
	reports := make(chan *protocol.ReportTaskResultRequest, 1)
	var runtime *Runtime
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() {
		finished <- rpchost.Run(ctx, shutdown, rpchost.Hooks{
			Initialize: func(ctx context.Context) error {
				close(initializing)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				registry := NewRegistry()
				if err := registry.Register("provider.test", runtimeProcessorFunc(func(context.Context, Input) (Result, error) {
					return Result{Summary: "initialized processor ran"}, nil
				})); err != nil {
					return err
				}
				client := &runtimeReportClientStub{report: func(_ context.Context, report *protocol.ReportTaskResultRequest) (*protocol.ReportTaskResultResponse, error) {
					reports <- report
					return &protocol.ReportTaskResultResponse{}, nil
				}}
				var err error
				runtime, err = NewRuntime(registry, client, adapterTestGroup, adapterTestToken, adapterTestID,
					RuntimeConfig{Concurrency: 1, QueueCapacity: 1, ReportCapacity: 1, ReportConcurrency: 1})
				if err != nil {
					return err
				}
				auth, err := NewAuthenticator(adapterTestGroup, adapterTestToken)
				if err != nil {
					runtime.Stop()
					return err
				}
				adapter, err := NewRPCAdapter(auth, registry, adapterTestID, runtime)
				if err != nil {
					runtime.Stop()
					return err
				}
				if err := runtime.Start(); err != nil {
					runtime.Stop()
					return err
				}
				initialized.Store(adapter)
				return nil
			},
			Stop:  func() { runtime.Stop() },
			Drain: func(ctx context.Context) error { return runtime.Drain(ctx) },
		}, server)
	}()
	t.Cleanup(func() {
		cancel()
		finishInitialization()
		select {
		case err := <-finished:
			if err != nil {
				t.Errorf("stop executor provider host: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("executor provider host did not finish shutdown")
		}
	})
	select {
	case <-initializing:
	case <-time.After(5 * time.Second):
		t.Fatal("host never entered dependency initialization")
	}
	if calls.Load() != 0 {
		t.Fatal("server registration evaluated the adapter during initialization")
	}
	finishInitialization()
	conn, err := grpc.NewClient("passthrough:///"+config.ListenOn, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rpcCancel()
	if _, err := grpc_health_v1.NewHealthClient(conn).Check(rpcCtx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("health after dependency initialization: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("adapter provider calls after startup: %d", calls.Load())
	}
	request := validAdapterDispatch()
	request.ProcessorName = "provider.test"
	ack, err := protocol.NewExecutorClient(conn).DispatchTask(rpcCtx, request)
	if err != nil || ack.GetStatus() != protocol.DispatchStatus_ACCEPTED || ack.GetExecutorInstanceId() != adapterTestID {
		t.Fatalf("dispatch through initialized adapter: status=%v error=%v", ack.GetStatus(), err)
	}
	select {
	case report := <-reports:
		if report.TaskId != request.TaskId || report.Outcome != protocol.TaskOutcome_SUCCESS || report.Summary != "initialized processor ran" {
			t.Fatal("registered adapter did not use the initialized execution runtime")
		}
	case <-rpcCtx.Done():
		t.Fatal("dispatch ACK did not reach the initialized processor and reporter")
	}
	if calls.Load() != 1 {
		t.Fatal("serving requests evaluated the adapter provider again")
	}
}

func TestExecutorServerNilProviderResultFailsHostStartup(t *testing.T) {
	if executorProviderSubprocess(t, "DOGX_EXECUTOR_NIL_PROVIDER_TEST", "TestExecutorServerNilProviderResultFailsHostStartup") {
		return
	}
	config, shutdown := executorProviderConfig(t)
	var calls atomic.Int32
	server, err := NewServer(config, func() *RPCAdapter {
		calls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("construction evaluated the adapter provider")
	}
	var closed atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = rpchost.Run(ctx, shutdown, rpchost.Hooks{
		Initialize: func(context.Context) error { return nil },
		Close: func() error {
			closed.Store(true)
			return nil
		},
	}, server)
	if err == nil || !strings.Contains(err.Error(), "RPC adapter provider returned nil after initialization") {
		t.Fatalf("nil adapter did not fail host startup explicitly: %v", err)
	}
	if calls.Load() != 1 || !closed.Load() {
		t.Fatal("startup failure did not evaluate the provider once and close initialized resources")
	}
}

// executorProviderSubprocess isolates host-owned logging and go-zero's process
// shutdown hooks. The child reuses this package's stable test executable path.
func executorProviderSubprocess(t *testing.T, environment, name string) bool {
	t.Helper()
	if os.Getenv(environment) == "1" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+name+"$", "-test.v")
	command.Env = append(os.Environ(), environment+"=1")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("executor provider subprocess: %v\n%s", err, output)
	}
	return true
}

func executorProviderConfig(t *testing.T) (zrpc.RpcServerConf, rpchost.ShutdownConfig) {
	t.Helper()
	var config zrpc.RpcServerConf
	if err := conf.FillDefault(&config); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config.ListenOn = listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	config.Name, config.Mode = "executor-provider-test", service.TestMode
	config.DevServer.Enabled = false
	config.CpuThreshold = 0
	shutdown := rpchost.ShutdownConfig{WrapUpTime: time.Millisecond, WaitTime: 3 * time.Second}
	if err := shutdown.Normalize(&config.ServiceConf); err != nil {
		t.Fatal(err)
	}
	return config, shutdown
}
