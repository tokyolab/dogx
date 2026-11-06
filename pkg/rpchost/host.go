package rpchost

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
)

// Hooks describes application-owned work and resources in one RPC process.
// Functions are called at most once. They must not close process-wide logging.
// Run bounds its waits, but cannot forcibly interrupt a hook that ignores its
// Context; returning from Run therefore requires the host process to exit.
type Hooks struct {
	// Initialize prepares application dependencies before any server starts.
	// Its Context is cancelled when shutdown begins. An error must roll back
	// partially acquired resources; Close is called only after successful
	// initialization. If initialization ignores cancellation, Run waits only
	// within the shared shutdown budget and the host must then exit.
	Initialize func(context.Context) error
	// Stop closes work admission and signals producers to stop. It must return
	// promptly and coordinate with concurrent submissions, including submissions
	// from RPCs that began before shutdown. It must not release shared resources.
	Stop func()
	// Drain waits for actual completion of accepted work after Stop returns.
	// It must include submissions admitted before Stop that are still being
	// handed off. Nil means that no work assigned to Drain remains, not merely
	// that cancellation was sent. RPC handlers drain concurrently with it;
	// private maintenance workers may instead be joined inside Close.
	Drain func(context.Context) error
	// Close releases owned dependencies after RPCs and Drain have finished. It
	// may first stop and join private maintenance workers, provided their shared
	// resources remain open until they actually exit. It is never started after
	// failed draining or budget expiry. A slow Close may outlive Run; the host
	// must exit rather than reuse resources whose cleanup is still in progress.
	Close func() error
}

// Run owns one process's RPC serving and shutdown sequence. It starts servers
// through a go-zero ServiceGroup and returns after a termination signal, ctx
// cancellation, or startup failure has triggered shutdown. An already cancelled
// ctx skips startup and still performs cleanup. Config must also be normalized
// into every server's shared ServiceConf before constructing the servers, which
// applies the native process shutdown policy. Run does not reapply that policy.
//
// The first stop request fixes one WaitTime deadline for wrap-up, RPC draining,
// background draining, dependency cleanup and logging. A timeout does not prove
// that unfinished work has stopped; dependencies still in use are not closed.
// Startup and cleanup errors are returned, while a completed requested shutdown
// returns nil. Run does not call os.Exit: main must exit when it returns and write
// any final error to stderr, since global logging has already been closed or its
// closing attempted. Run is process-scoped, not a hot-restart or library API.
func Run(ctx context.Context, config ShutdownConfig, hooks Hooks, servers ...*Server) error {
	if ctx == nil {
		return errors.New("RPC host context must not be nil")
	}
	if err := config.validate(); err != nil {
		return err
	}
	if len(servers) == 0 {
		return errors.New("RPC host requires at least one server")
	}
	seen := make(map[*Server]struct{}, len(servers))
	for _, server := range servers {
		if server == nil {
			return errors.New("RPC host server must not be nil")
		}
		if _, duplicate := seen[server]; duplicate {
			return errors.New("RPC host server is registered more than once")
		}
		seen[server] = struct{}{}
	}
	signal, waitNative, releaseSignals := terminationSignal()
	defer releaseSignals()
	var stopping atomic.Bool
	startErrors := make(chan error, len(servers))
	group := service.NewServiceGroup()
	for _, server := range servers {
		group.Add(service.WithStart(func() {
			err := invokeHook("start RPC server", func() error {
				server.Start()
				return nil
			})
			if err != nil {
				startErrors <- err
			} else if !stopping.Load() {
				startErrors <- errors.New("RPC server exited before shutdown")
			}
		}))
	}
	var runErr error
	var signalled bool
	var stopped bool
	var signalBeforeRun bool
	select {
	case <-signal:
		stopped, signalled, signalBeforeRun = true, true, true
	default:
		stopped = ctx.Err() != nil
	}
	initialized := hooks.Initialize == nil
	initializeCtx, cancelInitialize := context.WithCancel(context.Background())
	defer cancelInitialize()
	var initializeDone <-chan error
	if !stopped && hooks.Initialize != nil {
		initializeDone = beginHook("initialize application dependencies", func() error {
			return hooks.Initialize(initializeCtx)
		})
		select {
		case runErr = <-initializeDone:
			initializeDone = nil
			initialized = runErr == nil
			stopped = runErr != nil
		case <-signal:
			stopped, signalled = true, true
		case <-ctx.Done():
			stopped = true
		}
	}
	// Initialization and a stop request may become ready together. Recheck
	// before publishing listeners instead of briefly serving a stopping process.
	if !stopped {
		select {
		case <-signal:
			stopped, signalled = true, true
		default:
			stopped = ctx.Err() != nil
		}
	}
	if !stopped {
		go group.Start()
		select {
		case <-signal:
			signalled = true
		case <-ctx.Done():
		case runErr = <-startErrors:
		}
	}
	stopping.Store(true)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), config.WaitTime)
	defer cancel()
	cancelInitialize()
	for _, server := range servers {
		server.stopAdmission()
	}
	if initializeDone != nil {
		// No listener was started while initialization was pending. Do not run
		// Stop or Close concurrently with an initializer that still owns setup.
		initializeErr := awaitHook(shutdownCtx, initializeDone)
		initialized = initializeErr == nil
		runErr = errors.Join(runErr, initializeErr)
	}
	workDone := beginHook("drain application work", func() error {
		if !initialized {
			return nil
		}
		if hooks.Stop != nil {
			hooks.Stop()
		}
		if err := shutdownCtx.Err(); err != nil {
			return err
		}
		if hooks.Drain != nil {
			return hooks.Drain(shutdownCtx)
		}
		return nil
	})
	if signalled && !signalBeforeRun {
		// Unix discovery publishers receive native wrap-up during this interval.
		// Windows has no native callbacks; it still uses the same configured phase
		// and deadline. Registry leases may remain until process exit and expiry.
		timer := time.NewTimer(config.WrapUpTime)
		select {
		case <-timer.C:
		case <-shutdownCtx.Done():
		}
		timer.Stop()
	}
	serverResults := make(chan error, len(servers))
	for _, server := range servers {
		go func() { serverResults <- server.Shutdown(shutdownCtx) }()
	}
	var drainErr error
	for range servers {
		drainErr = errors.Join(drainErr, awaitHook(shutdownCtx, serverResults))
	}
	drainErr = errors.Join(drainErr, awaitHook(shutdownCtx, workDone))
	runErr = errors.Join(runErr, drainErr)
	if initialized && drainErr == nil && shutdownCtx.Err() == nil && hooks.Close != nil {
		// Only the coordinating goroutine advances phases. A late Drain return
		// after our deadline cannot start closing dependencies in the background.
		runErr = errors.Join(runErr, awaitHook(shutdownCtx, beginHook("close application dependencies", hooks.Close)))
	}
	if signalled && !signalBeforeRun && waitNative != nil && shutdownCtx.Err() == nil {
		// This wait is outside every native callback. Waiting for zrpc Start or
		// the global callback group from inside a callback would deadlock.
		runErr = errors.Join(runErr, awaitHook(shutdownCtx, beginHook("finish native shutdown", func() error {
			waitNative()
			return nil
		})))
	}
	// Logging is attempted last, even on deadline expiry; a blocked writer gets
	// no fresh budget. Never call zrpc.Stop, which closes this global writer early.
	runErr = errors.Join(runErr, awaitHook(shutdownCtx, beginHook("close process logging", logx.Close)))
	return errors.Join(runErr, shutdownCtx.Err())
}

func beginHook(name string, hook func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- invokeHook(name, hook) }()
	return done
}

func invokeHook(name string, hook func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if cause, ok := recovered.(error); ok {
				err = fmt.Errorf("%s panicked: %w", name, cause)
			} else {
				err = fmt.Errorf("%s panicked (%T)", name, recovered)
			}
		}
	}()
	if err := hook(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func awaitHook(ctx context.Context, done <-chan error) error {
	select {
	case err := <-done:
		return err
	default:
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
