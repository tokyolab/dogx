package loginprotection

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
)

func lazyTestRuntime(t *testing.T, load func(context.Context) (*system.LoginSecurityConfig, error)) *Runtime {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Runtime{load: load, lazy: true, ctx: ctx, cancel: cancel}
}

func TestLazyCurrentCoalescesAndCachesSnapshots(t *testing.T) {
	var calls atomic.Int32
	source := validConfig()
	r := lazyTestRuntime(t, func(context.Context) (*system.LoginSecurityConfig, error) {
		calls.Add(1)
		return source, nil
	})
	var callers sync.WaitGroup
	for i := 0; i < 30; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			cfg, err := r.Current(context.Background())
			if err != nil || cfg.FailureThreshold != 5 {
				t.Errorf("unexpected config: %v %v", cfg, err)
				return
			}
			cfg.FailureThreshold = 99
		}()
	}
	callers.Wait()
	source.FailureThreshold = 77
	cfg, err := r.Current(context.Background())
	if err != nil || cfg.FailureThreshold != 5 || calls.Load() != 1 {
		t.Fatalf("cache/ownership mismatch: %v %v calls=%d", cfg, err, calls.Load())
	}
}

func TestLazyFailureBackoffAndRecovery(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "RPC failure", true: "invalid config"}[invalid], func(t *testing.T) {
			var calls atomic.Int32
			var recovered atomic.Bool
			r := lazyTestRuntime(t, func(context.Context) (*system.LoginSecurityConfig, error) {
				calls.Add(1)
				if recovered.Load() {
					return validConfig(), nil
				}
				if invalid {
					return &system.LoginSecurityConfig{}, nil
				}
				return nil, errors.New("RPC unavailable")
			})
			for i := 0; i < 10; i++ {
				if cfg, err := r.Current(context.Background()); err == nil || cfg != nil {
					t.Fatalf("failure must not disable protection: %v %v", cfg, err)
				}
			}
			if calls.Load() != 1 {
				t.Fatalf("backoff did not suppress retries: %d", calls.Load())
			}
			recovered.Store(true)
			// Advance just the retry deadline, without making this test sleep.
			r.mu.Lock()
			r.retryAt = time.Now().Add(-time.Second)
			r.mu.Unlock()
			if cfg, err := r.Current(context.Background()); err != nil || cfg == nil || calls.Load() != 2 {
				t.Fatalf("retry did not recover: %v %v calls=%d", cfg, err, calls.Load())
			}
		})
	}
}

func TestLazyCancellationDoesNotPoisonSharedLoad(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	r := lazyTestRuntime(t, func(ctx context.Context) (*system.LoginSecurityConfig, error) {
		calls.Add(1)
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 5*time.Second {
			t.Error("shared load has no bounded timeout")
		}
		close(started)
		select {
		case <-release:
			return validConfig(), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := r.Current(ctx); first <- err }()
	<-started
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled caller: %v", err)
	}
	close(release)
	if cfg, err := r.Current(context.Background()); err != nil || cfg == nil || calls.Load() != 1 {
		t.Fatalf("one caller cancelled shared work: %v %v calls=%d", cfg, err, calls.Load())
	}
}

func TestLazyShutdownCancelsLoad(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	r := lazyTestRuntime(t, func(ctx context.Context) (*system.LoginSecurityConfig, error) {
		close(started)
		<-ctx.Done()
		close(finished)
		return nil, ctx.Err()
	})
	result := make(chan error, 1)
	go func() { _, err := r.Current(context.Background()); result <- err }()
	<-started
	r.cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown result: %v", err)
	}
	<-finished
	if _, err := r.Current(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("closed runtime accepted a request: %v", err)
	}
}

func TestCancelledCurrentDoesNotLoad(t *testing.T) {
	r := lazyTestRuntime(t, func(context.Context) (*system.LoginSecurityConfig, error) {
		t.Error("cancelled request started a load")
		return validConfig(), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Current(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request: %v", err)
	}
}

func TestExplicitReloadBypassesBackoffAndInvalidatesFailedSnapshot(t *testing.T) {
	var unavailable atomic.Bool
	unavailable.Store(true)
	r := lazyTestRuntime(t, func(context.Context) (*system.LoginSecurityConfig, error) {
		if unavailable.Load() {
			return nil, errors.New("offline")
		}
		return validConfig(), nil
	})
	ctx := context.Background()
	if _, err := r.Current(ctx); err == nil {
		t.Fatal("unavailable source accepted")
	}
	unavailable.Store(false)
	if err := r.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Current(ctx); err != nil {
		t.Fatalf("explicit refresh did not bypass backoff: %v", err)
	}
	unavailable.Store(true)
	if err := r.Reload(ctx); err == nil {
		t.Fatal("reload failure lost")
	}
	if _, err := r.Current(ctx); err == nil {
		t.Fatal("stale snapshot remained usable")
	}
}

func TestExplicitReloadFollowsInflightDemandLoad(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	r := lazyTestRuntime(t, func(ctx context.Context) (*system.LoginSecurityConfig, error) {
		cfg := validConfig()
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		} else {
			cfg.FailureThreshold = 10
		}
		return cfg, nil
	})
	loaded := make(chan error, 1)
	go func() { _, err := r.Current(context.Background()); loaded <- err }()
	<-started
	reloaded := make(chan error, 1)
	go func() { reloaded <- r.Reload(context.Background()) }()
	close(release)
	if err := <-loaded; err != nil {
		t.Fatal(err)
	}
	if err := <-reloaded; err != nil {
		t.Fatal(err)
	}
	if cfg, err := r.Current(context.Background()); err != nil || cfg.FailureThreshold != 10 || calls.Load() != 2 {
		t.Fatalf("notification was lost behind old load: %v %v calls=%d", cfg, err, calls.Load())
	}
}
