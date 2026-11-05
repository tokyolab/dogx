package svc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/execution"
)

func TestStoppingHostSharesDrainBudgetAndWaitsForActualExit(t *testing.T) {
	for _, boundary := range []string{"address query", "cancel RPC"} {
		t.Run(boundary, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				stopper := &hostStoppingProcessor{entered: make(chan context.Context, 1), release: make(chan struct{})}
				notification := &hostStoppingNotification{boundary: boundary, entered: make(chan context.Context, 1), release: make(chan struct{})}
				queue, notifier := newHostStoppingComponents(t, stopper, notification)
				db, connection := newLifecycleTestDatabase(t, nil)
				host := &ServiceContext{Stops: queue, CancelNotifications: notifier, sqlDB: db}
				var stopOnce, notificationOnce sync.Once
				releaseStop := func() { stopOnce.Do(func() { close(stopper.release) }) }
				releaseNotification := func() { notificationOnce.Do(func() { close(notification.release) }) }
				defer func() { releaseStop(); releaseNotification(); _ = host.Close() }()
				if err := queue.Start(); err != nil {
					t.Fatal(err)
				}
				if err := notifier.Start(); err != nil {
					t.Fatal(err)
				}
				decision := execution.Notification{InstanceID: 1, ExecutorGroupID: 2, Reason: model.EndReasonManualStop}
				if !queue.Hint(1) || !notifier.Hint(decision) {
					t.Fatal("host stopping work was not admitted")
				}
				stopCtx, notificationCtx := <-stopper.entered, <-notification.entered
				waitCtx, cancelWait := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancelWait()
				if err := host.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("active stopping wait ignored the caller deadline: %v", err)
				}
				if stopCtx.Err() != nil || notificationCtx.Err() != nil || connection.closed.Load() != 0 {
					t.Fatal("Wait canceled stopping work or released its dependency")
				}
				host.Stop()
				host.Stop()
				if queue.Hint(1) || notifier.Hint(decision) || stopCtx.Err() != nil || notificationCtx.Err() != nil {
					t.Fatal("Stop kept admission open or canceled admitted stopping work")
				}
				shutdown, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
				defer cancelShutdown()
				drained := make(chan error, 1)
				go func() { drained <- host.Drain(shutdown) }()
				synctest.Wait()
				time.Sleep(time.Second)
				if err := <-drained; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("blocked stopping drain lost the shared deadline: %v", err)
				}
				synctest.Wait()
				if stopCtx.Err() == nil || notificationCtx.Err() == nil || connection.closed.Load() != 0 {
					t.Fatal("shared deadline missed one component or closed active dependencies")
				}
				if err := host.Wait(shutdown); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("cancellation was mistaken for actual stopping completion: %v", err)
				}
				closed := make(chan error, 1)
				go func() { closed <- host.Close() }()
				synctest.Wait()
				assertStoppingHostStillOpen(t, closed, connection.closed.Load())
				// Releasing database cancellation alone cannot release dependencies
				// still used by the independent address query or RPC cleanup.
				releaseStop()
				synctest.Wait()
				assertStoppingHostStillOpen(t, closed, connection.closed.Load())
				releaseNotification()
				if err := <-closed; err != nil {
					t.Fatal(err)
				}
				if err := host.Wait(shutdown); err != nil {
					t.Fatalf("actual completion did not precede the expired wait context: %v", err)
				}
				if err := host.Close(); err != nil {
					t.Fatal(err)
				}
				if connection.closed.Load() != 1 || stopper.calls.Load() != 1 || notification.blocked.Load() != 1 {
					t.Fatal("cleanup repeated or stopping work restarted after shutdown")
				}
			})
		})
	}
}

func TestStoppingHostCleansUpBeforeComponentsStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stopper := &hostStoppingProcessor{entered: make(chan context.Context, 1), release: make(chan struct{})}
		notification := &hostStoppingNotification{boundary: "cancel RPC", entered: make(chan context.Context, 1), release: make(chan struct{})}
		queue, notifier := newHostStoppingComponents(t, stopper, notification)
		db, connection := newLifecycleTestDatabase(t, nil)
		host := &ServiceContext{Stops: queue, CancelNotifications: notifier, sqlDB: db}
		defer func() { close(stopper.release); close(notification.release); _ = host.Close() }()
		// Initialization can fail after ownership assignment but before either
		// component starts. Cleanup must complete without starting its workers.
		host.Stop()
		if queue.Start() == nil || notifier.Start() == nil {
			t.Fatal("initializer cleanup permitted a stopped component to start")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := host.Drain(ctx); err != nil {
			t.Fatalf("unused components needed a fresh shutdown budget: %v", err)
		}
		if err := host.Wait(ctx); err != nil {
			t.Fatalf("unused components did not finish on Stop: %v", err)
		}
		if connection.closed.Load() != 0 {
			t.Fatal("Stop or Drain closed dependencies before Close")
		}
		if err := host.Close(); err != nil {
			t.Fatal(err)
		}
		if connection.closed.Load() != 1 || stopper.calls.Load() != 0 || notification.queries.Load() != 0 || notification.blocked.Load() != 0 {
			t.Fatal("initializer cleanup started work or failed to close its dependency")
		}
	})
}

func assertStoppingHostStillOpen(t *testing.T, closed <-chan error, dependencyCloses int32) {
	t.Helper()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before actual stopping dependency exit: %v", err)
	default:
	}
	if dependencyCloses != 0 {
		t.Fatal("Close released a dependency still used by stopping work")
	}
}

// hostStoppingProcessor models transaction cleanup that has not actually
// returned after cancellation. It never claims a successful committed batch.
type hostStoppingProcessor struct {
	entered chan context.Context
	release chan struct{}
	calls   atomic.Int32
}

func (s *hostStoppingProcessor) ProcessBatch(ctx context.Context, _ int64) (instancestop.BatchResult, error) {
	s.calls.Add(1)
	s.entered <- ctx
	<-s.release
	return instancestop.BatchResult{}, ctx.Err()
}

// hostStoppingNotification holds one selected external call through actual
// return, independently of database cancellation's lifetime.
type hostStoppingNotification struct {
	boundary string
	entered  chan context.Context
	release  chan struct{}
	queries  atomic.Int32
	blocked  atomic.Int32
}

func (s *hostStoppingNotification) ListCancelTargets(ctx context.Context, _ int64, after string, _ int) ([]string, error) {
	s.queries.Add(1)
	if s.boundary == "address query" {
		s.blocked.Add(1)
		s.entered <- ctx
		<-s.release
		return nil, ctx.Err()
	}
	if after != "" {
		return nil, nil
	}
	return []string{"127.0.0.1:19001"}, nil
}

func (s *hostStoppingNotification) Cancel(ctx context.Context, _ execution.CancelTarget) error {
	s.blocked.Add(1)
	s.entered <- ctx
	<-s.release
	return ctx.Err()
}

func newHostStoppingComponents(t *testing.T, stopper *hostStoppingProcessor, notification *hostStoppingNotification) (*instancestop.Queue, *execution.Notifier) {
	t.Helper()
	queue, err := instancestop.NewQueue(stopper, instancestop.Config{Capacity: 1, Concurrency: 1, RunTimeout: 2 * time.Second, MaxBatches: 4})
	if err != nil {
		t.Fatal(err)
	}
	notifier, err := execution.NewNotifier(notification, notification, execution.NotificationConfig{
		InstanceCapacity: 1, PageSize: 1, TargetCapacity: 1, Concurrency: 1,
		QueryTimeout: time.Minute, InstanceTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return queue, notifier
}
