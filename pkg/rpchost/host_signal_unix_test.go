//go:build linux || darwin || freebsd

package rpchost

import (
	"context"
	"io"
	"os"
	"os/exec"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"google.golang.org/grpc"
)

func configureHostSignalProcess(_ *exec.Cmd) {}

func sendHostTestSignal() error {
	return syscall.Kill(os.Getpid(), syscall.SIGTERM)
}

func testHostSignalBeforeRun(t *testing.T) {
	policy := ShutdownConfig{WrapUpTime: 20 * time.Millisecond, WaitTime: 2 * time.Second}
	config := testConfig(t, 0)
	if err := policy.Normalize(&config.ServiceConf); err != nil {
		t.Fatal(err)
	}
	var registered, closed atomic.Bool
	server, err := NewServer(config, func(*grpc.Server) { registered.Store(true) })
	if err != nil {
		t.Fatal(err)
	}
	logx.SetWriter(logx.NewWriter(io.Discard))
	// Simulate a termination signal during dependency initialization. Waiting
	// here proves the original native listener notification already completed.
	waitNative := proc.AddShutdownListener(func() {})
	if err := sendHostTestSignal(); err != nil {
		t.Fatal(err)
	}
	nativeDone := make(chan struct{})
	go func() {
		waitNative()
		close(nativeDone)
	}()
	waitSignal(t, nativeDone)
	err = Run(context.Background(), policy, Hooks{Close: func() error {
		closed.Store(true)
		return nil
	}}, server)
	if err != nil {
		t.Fatalf("shutdown received before Run: %v", err)
	}
	if registered.Load() || !closed.Load() {
		t.Fatalf("pre-signalled host registered=%t, dependencies closed=%t; want false/true", registered.Load(), closed.Load())
	}
}
