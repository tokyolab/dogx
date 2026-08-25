package authorization

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type policyLoaderStub struct {
	active    atomic.Int32
	maxActive atomic.Int32
	calls     atomic.Int32
	err       error
	delay     time.Duration
}

func (s *policyLoaderStub) LoadPolicy() error {
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		maximum := s.maxActive.Load()
		if active <= maximum || s.maxActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	s.calls.Add(1)
	time.Sleep(s.delay)
	return s.err
}

func TestPolicyReloaderSerializesEveryReloadSource(t *testing.T) {
	loader := &policyLoaderStub{delay: 5 * time.Millisecond}
	reloader, err := NewPolicyReloader(loader)
	if err != nil {
		t.Fatalf("create policy reloader: %v", err)
	}

	var wait sync.WaitGroup
	for range 12 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := reloader.Reload(); err != nil {
				t.Errorf("reload: %v", err)
			}
		}()
	}
	wait.Wait()
	if loader.calls.Load() != 12 || loader.maxActive.Load() != 1 {
		t.Fatalf("reloads were not serialized: calls=%d maxActive=%d", loader.calls.Load(), loader.maxActive.Load())
	}
	if !reloader.Ready() {
		t.Fatal("successful reload did not mark policy ready")
	}
}

func TestPolicyReloaderKeepsLastSuccessfulSnapshotReadyAfterFailure(t *testing.T) {
	loader := &policyLoaderStub{}
	reloader, err := NewPolicyReloader(loader)
	if err != nil {
		t.Fatalf("create policy reloader: %v", err)
	}
	if err := reloader.Reload(); err != nil {
		t.Fatalf("initial reload: %v", err)
	}
	loader.err = errors.New("postgres unavailable")
	if err := reloader.Reload(); err == nil {
		t.Fatal("expected reload error")
	}
	if !reloader.Ready() {
		t.Fatal("failed refresh discarded readiness of last successful snapshot")
	}
}

func TestPolicyReloaderPeriodicFallbackRunsAndStops(t *testing.T) {
	loader := &policyLoaderStub{}
	reloader, err := NewPolicyReloader(loader)
	if err != nil {
		t.Fatalf("create policy reloader: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := reloader.Start(ctx, 5*time.Millisecond, nil); err != nil {
		t.Fatalf("start periodic reload: %v", err)
	}
	time.Sleep(18 * time.Millisecond)
	cancel()
	reloader.Wait()
	settledCalls := loader.calls.Load()
	if settledCalls == 0 {
		t.Fatal("periodic fallback did not reload policy")
	}
	time.Sleep(8 * time.Millisecond)
	if loader.calls.Load() != settledCalls {
		t.Fatal("periodic fallback continued after cancellation")
	}
}

func TestPolicyReloaderValidatesDependencies(t *testing.T) {
	if _, err := NewPolicyReloader(nil); err == nil {
		t.Fatal("expected nil loader to be rejected")
	}
	reloader, _ := NewPolicyReloader(&policyLoaderStub{})
	if err := reloader.Start(nil, time.Second, nil); err == nil {
		t.Fatal("expected nil context to be rejected")
	}
	if err := reloader.Start(context.Background(), 0, nil); err == nil {
		t.Fatal("expected invalid interval to be rejected")
	}
}
