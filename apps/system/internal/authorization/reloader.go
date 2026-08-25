package authorization

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type PolicyLoader interface {
	LoadPolicy() error
}

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

func (r *PolicyReloader) Reload() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.loader.LoadPolicy(); err != nil {
		return fmt.Errorf("load Casbin policy: %w", err)
	}
	r.ready.Store(true)
	return nil
}

func (r *PolicyReloader) Ready() bool {
	return r != nil && r.ready.Load()
}

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

func (r *PolicyReloader) Wait() {
	if r == nil {
		return
	}
	r.wait.Wait()
	r.mu.Lock()
	r.mu.Unlock()
}
