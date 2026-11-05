package rpchost

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const hostScenarioEnv = "DOGX_RPC_HOST_TEST_SCENARIO"

func TestHostLifecycle(t *testing.T) {
	// Each child owns go-zero's process-wide signal handlers, logging, and
	// shutdown listeners. A timed-out host is tested by exiting that child.
	for _, scenario := range []string{
		"graceful_context", "graceful_signal", "ignored_handler", "stalled_drain",
		"stalled_close", "stalled_logging", "startup_failure", "startup_panic", "signal_before_run",
		"signal_during_init", "stalled_init",
	} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHostProcess$", "-test.v")
			cmd.Env = append(os.Environ(), hostScenarioEnv+"="+scenario)
			if scenario == "graceful_signal" || scenario == "signal_during_init" {
				configureHostSignalProcess(cmd)
			}
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("host scenario failed: %v\n%s", err, output)
			}
		})
	}
}

func TestHostProcess(t *testing.T) {
	switch scenario := os.Getenv(hostScenarioEnv); scenario {
	case "graceful_context", "graceful_signal":
		testHostGraceful(t, scenario == "graceful_signal")
	case "ignored_handler":
		testHostIgnoredHandler(t)
	case "stalled_drain", "stalled_close", "stalled_logging":
		testHostStalledHook(t, scenario)
	case "startup_failure", "startup_panic":
		testHostStartupFailure(t, scenario == "startup_panic")
	case "signal_before_run":
		testHostSignalBeforeRun(t)
	case "signal_during_init", "stalled_init":
		testHostStoppedInitialization(t, scenario == "stalled_init")
	default:
		t.Skip("subprocess helper")
	}
}

func testHostGraceful(t *testing.T, signalStop bool) {
	policy := ShutdownConfig{WrapUpTime: 30 * time.Millisecond, WaitTime: 3 * time.Second}
	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	var returned atomic.Int32
	servers := make([]*Server, 2)
	addresses := make([]string, 2)
	for i := range servers {
		servers[i], addresses[i] = newHostTestServer(t, policy, 0, func(context.Context) error {
			entered <- struct{}{}
			<-release
			returned.Add(1)
			return nil
		})
	}
	var stopCalls, drainCalls, closeCalls, logCalls atomic.Int32
	var backgroundDone, dependenciesClosed atomic.Bool
	stopDone, drainStarted := make(chan struct{}), make(chan struct{})
	releaseBackground := make(chan struct{})
	logx.SetWriter(&hostLogWriter{Writer: logx.NewWriter(io.Discard), close: func() error {
		logCalls.Add(1)
		if !dependenciesClosed.Load() {
			return errors.New("logging closed before dependencies")
		}
		return nil
	}})
	hooks := Hooks{
		Stop: func() {
			stopCalls.Add(1)
			close(stopDone)
		},
		Drain: func(ctx context.Context) error {
			drainCalls.Add(1)
			select {
			case <-stopDone:
			default:
				return errors.New("background drain started before admission stopped")
			}
			// Both listeners must reject admission before application draining.
			for _, server := range servers {
				_, err := server.trackUnary(ctx, nil, nil, func(context.Context, any) (any, error) {
					return nil, errors.New("late handler was admitted")
				})
				if status.Code(err) != codes.Unavailable {
					return errors.New("RPC admission remained open during application drain")
				}
			}
			close(drainStarted)
			select {
			case <-releaseBackground:
				backgroundDone.Store(true)
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		Close: func() error {
			closeCalls.Add(1)
			if returned.Load() != 2 || !backgroundDone.Load() {
				return errors.New("dependencies closed while application work remained")
			}
			dependenciesClosed.Store(true)
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runHostAsync(ctx, policy, hooks, servers...)
	calls := make([]<-chan error, len(servers))
	for i, address := range addresses {
		calls[i] = callWork(hostTestClient(t, address))
		waitSignal(t, entered)
	}
	if signalStop {
		if err := sendHostTestSignal(); err != nil {
			t.Fatal(err)
		}
	} else {
		cancel()
	}
	waitSignal(t, drainStarted)
	select {
	case err := <-done:
		t.Fatalf("host returned while both RPC and background work were blocked: %v", err)
	default:
	}
	close(release)
	for _, call := range calls {
		if err := waitError(t, call); err != nil {
			t.Fatalf("accepted work did not complete: %v", err)
		}
	}
	if dependenciesClosed.Load() {
		t.Fatal("dependencies closed before background work finished")
	}
	close(releaseBackground)
	if err := waitError(t, done); err != nil {
		t.Fatalf("graceful shutdown: %v", err)
	}
	if stopCalls.Load() != 1 || drainCalls.Load() != 1 || closeCalls.Load() != 1 || logCalls.Load() != 1 {
		t.Fatalf("lifecycle calls stop/drain/close/log = %d/%d/%d/%d, want 1/1/1/1",
			stopCalls.Load(), drainCalls.Load(), closeCalls.Load(), logCalls.Load())
	}
}

func testHostIgnoredHandler(t *testing.T) {
	policy := ShutdownConfig{WrapUpTime: 20 * time.Millisecond, WaitTime: 300 * time.Millisecond}
	entered, release := make(chan struct{}), make(chan struct{})
	server, address := newHostTestServer(t, policy, 40, func(context.Context) error {
		close(entered)
		<-release
		return nil
	})
	var dependenciesClosed atomic.Bool
	logAttempted := make(chan struct{})
	logx.SetWriter(&hostLogWriter{Writer: logx.NewWriter(io.Discard), close: func() error {
		close(logAttempted)
		return nil
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runHostAsync(ctx, policy, Hooks{Close: func() error {
		dependenciesClosed.Store(true)
		return nil
	}}, server)
	call := callWork(hostTestClient(t, address))
	waitSignal(t, entered)
	if err := waitError(t, call); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("zrpc did not return its wire timeout: %v", err)
	}
	started := time.Now()
	cancel()
	assertHostDeadline(t, done, started, policy.WaitTime)
	if dependenciesClosed.Load() {
		t.Fatal("dependencies closed while a timed-out RPC handler still used them")
	}
	waitSignal(t, logAttempted)
	// Leave the handler blocked: the process must exit without waiting for it.
}

func testHostStalledHook(t *testing.T, scenario string) {
	policy := ShutdownConfig{WrapUpTime: 20 * time.Millisecond, WaitTime: 300 * time.Millisecond}
	if scenario == "stalled_logging" {
		policy.WaitTime = 900 * time.Millisecond
	}
	server, address := newHostTestServer(t, policy, 0, func(context.Context) error { return nil })
	blocked := make(chan struct{})
	phaseEntered, logAttempted := make(chan struct{}), make(chan struct{})
	var dependenciesClosed atomic.Bool
	var phaseOnce sync.Once
	hooks := Hooks{
		Drain: func(ctx context.Context) error {
			if scenario == "stalled_drain" {
				phaseOnce.Do(func() { close(phaseEntered) })
				<-blocked
			} else if scenario == "stalled_logging" {
				// Logging receives only the remainder of the original budget.
				timer := time.NewTimer(600 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
		Close: func() error {
			dependenciesClosed.Store(true)
			if scenario == "stalled_close" {
				phaseOnce.Do(func() { close(phaseEntered) })
				<-blocked
			}
			return nil
		},
	}
	logx.SetWriter(&hostLogWriter{Writer: logx.NewWriter(io.Discard), close: func() error {
		close(logAttempted)
		if scenario == "stalled_logging" {
			phaseOnce.Do(func() { close(phaseEntered) })
			<-blocked
		}
		return nil
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runHostAsync(ctx, policy, hooks, server)
	hostTestClient(t, address)
	started := time.Now()
	cancel()
	waitSignal(t, phaseEntered)
	assertHostDeadline(t, done, started, policy.WaitTime)
	if scenario == "stalled_drain" && dependenciesClosed.Load() {
		t.Fatal("dependencies closed despite an unfinished background drain")
	}
	if scenario != "stalled_drain" && !dependenciesClosed.Load() {
		t.Fatal("dependency cleanup was not reached after work completed")
	}
	waitSignal(t, logAttempted)
}

func testHostStartupFailure(t *testing.T, panicInRegistration bool) {
	policy := ShutdownConfig{WrapUpTime: 20 * time.Millisecond, WaitTime: time.Second}
	config := testConfig(t, 0)
	if err := policy.Normalize(&config.ServiceConf); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("synthetic registration failure")
	register := func(*grpc.Server) {}
	if panicInRegistration {
		register = func(*grpc.Server) { panic(sentinel) }
	} else {
		listener, err := net.Listen("tcp", config.ListenOn)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = listener.Close() }()
	}
	server, err := NewServer(config, register)
	if err != nil {
		t.Fatal(err)
	}
	other, _ := newHostTestServer(t, policy, 0, func(context.Context) error { return nil })
	var stopCalled, drained, closed, logsClosed atomic.Bool
	logx.SetWriter(&hostLogWriter{Writer: logx.NewWriter(io.Discard), close: func() error {
		logsClosed.Store(true)
		if !closed.Load() {
			return errors.New("logging closed before startup cleanup")
		}
		return nil
	}})
	err = Run(context.Background(), policy, Hooks{
		Stop: func() { stopCalled.Store(true) },
		Drain: func(context.Context) error {
			drained.Store(true)
			if !stopCalled.Load() {
				return errors.New("startup cleanup drained before stopping")
			}
			return nil
		},
		Close: func() error {
			closed.Store(true)
			if !drained.Load() {
				return errors.New("startup cleanup closed before draining")
			}
			return nil
		},
	}, server, other)
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("startup failure was not promptly reported: %v", err)
	}
	if panicInRegistration && !errors.Is(err, sentinel) {
		t.Fatalf("startup panic cause was lost: %v", err)
	}
	if !panicInRegistration {
		var networkError *net.OpError
		if !errors.As(err, &networkError) {
			t.Fatalf("listen failure cause was lost: %v", err)
		}
	}
	if !stopCalled.Load() || !drained.Load() || !closed.Load() || !logsClosed.Load() {
		t.Fatal("startup failure skipped orderly cleanup")
	}
}

func testHostStoppedInitialization(t *testing.T, stalled bool) {
	policy := ShutdownConfig{WrapUpTime: 30 * time.Millisecond, WaitTime: 3 * time.Second}
	if stalled {
		policy.WaitTime = 300 * time.Millisecond
	}
	config := testConfig(t, 0)
	if err := policy.Normalize(&config.ServiceConf); err != nil {
		t.Fatal(err)
	}
	var registered, stopped, drained, closed atomic.Bool
	server, err := NewServer(config, func(*grpc.Server) { registered.Store(true) })
	if err != nil {
		t.Fatal(err)
	}
	initEntered, initCanceled, releaseInit := make(chan struct{}), make(chan struct{}), make(chan struct{})
	logAttempted := make(chan struct{})
	logx.SetWriter(&hostLogWriter{Writer: logx.NewWriter(io.Discard), close: func() error {
		close(logAttempted)
		if !stalled && !closed.Load() {
			return errors.New("logging closed before initialized dependencies")
		}
		return nil
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runHostAsync(ctx, policy, Hooks{
		Initialize: func(ctx context.Context) error {
			close(initEntered)
			<-ctx.Done()
			close(initCanceled)
			<-releaseInit
			return nil
		},
		Stop: func() { stopped.Store(true) },
		Drain: func(context.Context) error {
			drained.Store(true)
			if !stopped.Load() {
				return errors.New("initialized work drained before stopping")
			}
			return nil
		},
		Close: func() error {
			closed.Store(true)
			if !drained.Load() {
				return errors.New("initialized dependencies closed before draining")
			}
			return nil
		},
	}, server)
	waitSignal(t, initEntered)
	started := time.Now()
	if stalled {
		cancel()
	} else if err := sendHostTestSignal(); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, initCanceled)
	if registered.Load() || stopped.Load() || drained.Load() || closed.Load() {
		t.Fatal("host served or cleaned up before initialization actually completed")
	}
	if stalled {
		assertHostDeadline(t, done, started, policy.WaitTime)
		if registered.Load() || stopped.Load() || drained.Load() || closed.Load() {
			t.Fatal("host touched incomplete initialization after its deadline")
		}
	} else {
		close(releaseInit)
		if err := waitError(t, done); err != nil {
			t.Fatalf("shutdown during initialization: %v", err)
		}
		if registered.Load() || !stopped.Load() || !drained.Load() || !closed.Load() {
			t.Fatal("completed initialization was served or did not receive orderly cleanup")
		}
	}
	waitSignal(t, logAttempted)
}

func newHostTestServer(t *testing.T, policy ShutdownConfig, timeoutMillis int64, run func(context.Context) error) (*Server, string) {
	t.Helper()
	config := testConfig(t, timeoutMillis)
	if err := policy.Normalize(&config.ServiceConf); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(config, func(server *grpc.Server) {
		server.RegisterService(&workServiceDesc, &workService{run: run})
	})
	if err != nil {
		t.Fatal(err)
	}
	return server, config.ListenOn
}

func hostTestClient(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	client, err := zrpc.NewClient(zrpc.RpcClientConf{Endpoints: []string{address}, NonBlock: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Conn().Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := grpc_health_v1.NewHealthClient(client.Conn()).Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil || response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("host health response = %v, error = %v", response, err)
	}
	return client.Conn()
}

func runHostAsync(ctx context.Context, policy ShutdownConfig, hooks Hooks, servers ...*Server) <-chan error {
	done := make(chan error, 1)
	go func() { done <- Run(ctx, policy, hooks, servers...) }()
	return done
}

func assertHostDeadline(t *testing.T, done <-chan error, started time.Time, budget time.Duration) {
	t.Helper()
	remaining := time.Until(started.Add(budget + 300*time.Millisecond))
	if remaining < 0 {
		t.Fatal("shutdown phase exceeded the shared budget before returning")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("unfinished shutdown error = %v, want DeadlineExceeded", err)
		}
		if elapsed := time.Since(started); elapsed < budget/2 {
			t.Fatalf("shutdown abandoned work before its deadline: %s", elapsed)
		}
	case <-time.After(remaining):
		t.Fatalf("host did not return within its single %s shutdown budget", budget)
	}
}

type hostLogWriter struct {
	logx.Writer
	close func() error
}

func (w *hostLogWriter) Close() error {
	return w.close()
}
