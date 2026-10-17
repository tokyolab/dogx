package authorization

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// PolicyLoader loads a local authorization policy from persistent storage.
// It does not provide cross-process notifications.
type PolicyLoader interface {
	// LoadPolicy returns after the load completes. It has no Context parameter,
	// so cancellation by a waiting caller does not interrupt the load. The
	// implementation must coordinate snapshot updates with concurrent authorization.
	LoadPolicy() error
}

// PolicyReloader serializes notification-driven and periodic reloads within this
// process and records whether any load has succeeded. It must be constructed by
// NewPolicyReloader and does not own the loader's database connection.
type PolicyReloader struct {
	loader PolicyLoader
	mu     sync.Mutex
	wait   sync.WaitGroup
	ready  atomic.Bool
}

func NewPolicyReloader(loader PolicyLoader) (*PolicyReloader, error) {
	if loader == nil {
		return nil, errors.New("policy loader is nil")
	}
	return &PolicyReloader{loader: loader}, nil
}

// Reload waits for earlier reloads and then loads the policy synchronously,
// preserving the cause on failure. Concurrent calls are supported but are not
// coalesced. Once a load succeeds, later failures do not clear the Ready flag.
func (r *PolicyReloader) Reload() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.loader.LoadPolicy(); err != nil {
		return fmt.Errorf("load Casbin policy: %w", err)
	}
	r.ready.Store(true)
	return nil
}

// Ready reports whether a policy load has ever succeeded, not whether the latest
// load succeeded or dependencies are currently healthy. A nil receiver returns false.
func (r *PolicyReloader) Ready() bool {
	return r != nil && r.ready.Load()
}

// Start starts periodic reloads and returns immediately without an initial load.
// Callers needing an initial policy must call Reload first. The lifecycle owner
// should call Start once, then cancel ctx and call Wait to await shutdown;
// repeated calls start additional loops. onError runs synchronously in the
// background loop and should return promptly; nil discards error notifications.
// ctx stops future iterations but does not interrupt an ongoing LoadPolicy call.
func (r *PolicyReloader) Start(
	ctx context.Context,
	interval time.Duration,
	onError func(error),
) error {
	if ctx == nil {
		return errors.New("policy reload context is nil")
	}
	if interval <= 0 {
		return errors.New("policy reload interval must be positive")
	}
	if onError == nil {
		onError = func(error) {}
	}

	r.wait.Add(1)
	go func() {
		defer r.wait.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.Reload(); err != nil {
					onError(err)
				}
			}
		}
	}()
	return nil
}

// Wait waits for background loops to exit and for the reload currently holding
// the mutex to complete. A nil receiver is safe. It neither cancels loops nor
// prevents new reloads. Shutdown must cancel Start's Context and stop external
// notification sources before Wait, then close the loader's dependencies.
// Wait must not run concurrently with new Start calls.
func (r *PolicyReloader) Wait() {
	if r == nil {
		return
	}
	r.wait.Wait()
	// Wait for any in-flight reload triggered outside the periodic goroutine,
	// such as a Redis watcher callback, before the database is closed.
	r.mu.Lock()
	r.mu.Unlock()
}
