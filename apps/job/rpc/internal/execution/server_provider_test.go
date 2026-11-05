package execution

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/rpchost"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestResultServerDefersReceiverUntilHostInitialization(t *testing.T) {
	if queueReceiverSubprocess(t, "DOGX_RESULT_PROVIDER_TEST", "TestResultServerDefersReceiverUntilHostInitialization") {
		return
	}
	config := transportServerConfig(t)
	shutdown := rpchost.ShutdownConfig{WrapUpTime: time.Millisecond, WaitTime: 3 * time.Second}
	if err := shutdown.Normalize(&config.ServiceConf); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	var initialized atomic.Pointer[Receiver]
	server, err := NewServer(config, func() *Receiver {
		calls.Add(1)
		return initialized.Load()
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("construction evaluated the receiver before dependencies existed")
	}
	results := &transportResults{reports: make(chan taskresult.Report, 1)}
	initializing := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	finishInitialization := func() { releaseOnce.Do(func() { close(release) }) }
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
				receiver, err := NewReceiver(&transportGroupAuth{}, results)
				if err != nil {
					return err
				}
				initialized.Store(receiver)
				return nil
			},
		}, server)
	}()
	t.Cleanup(func() {
		cancel()
		finishInitialization()
		select {
		case err := <-finished:
			if err != nil {
				t.Errorf("stop result server host: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("result server host did not finish shutdown")
		}
	})
	select {
	case <-initializing:
	case <-time.After(5 * time.Second):
		t.Fatal("host never entered dependency initialization")
	}
	if calls.Load() != 0 {
		t.Fatal("server registration evaluated the receiver during initialization")
	}
	finishInitialization()
	conn := transportClient(t, config.ListenOn, true)
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rpcCancel()
	if _, err := grpc_health_v1.NewHealthClient(conn).Check(rpcCtx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("health after dependency initialization: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("receiver provider calls after startup: %d", calls.Load())
	}
	if _, err := protocol.NewResultReceiverClient(conn).ReportTaskResult(rpcCtx, validReceiverRequest()); err != nil {
		t.Fatalf("report through initialized receiver: %v", err)
	}
	select {
	case report := <-results.reports:
		if report.GroupID != 41 || report.TaskID != 33 {
			t.Fatal("registered receiver lost the initialized dependencies")
		}
	case <-rpcCtx.Done():
		t.Fatal("initialized result admission did not retain the report")
	}
	if calls.Load() != 1 {
		t.Fatal("serving a request evaluated the receiver provider again")
	}
}

func TestResultServerNilProviderResultFailsHostStartup(t *testing.T) {
	if queueReceiverSubprocess(t, "DOGX_RESULT_NIL_PROVIDER_TEST", "TestResultServerNilProviderResultFailsHostStartup") {
		return
	}
	config := transportServerConfig(t)
	shutdown := rpchost.ShutdownConfig{WrapUpTime: time.Millisecond, WaitTime: 3 * time.Second}
	if err := shutdown.Normalize(&config.ServiceConf); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	server, err := NewServer(config, func() *Receiver {
		calls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("construction evaluated the receiver provider")
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
	if err == nil || !strings.Contains(err.Error(), "result receiver provider returned nil after initialization") {
		t.Fatalf("nil receiver did not fail host startup explicitly: %v", err)
	}
	if calls.Load() != 1 || !closed.Load() {
		t.Fatal("startup failure did not evaluate the provider once and close initialized resources")
	}
}
