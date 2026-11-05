package svc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/taskrecovery"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
)

func TestUnboundHostWaitsForActualDatabaseExit(t *testing.T) {
	for _, step := range []string{"clock", "candidates", "settlement"} {
		t.Run(step, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				source := &hostUnboundSource{step: step, entered: make(chan context.Context, 1), release: release}
				scanner := newHostUnboundRuntime(t, source)
				saver := &hostUnboundSaver{entered: make(chan context.Context, 1), release: release}
				results, err := taskresult.NewQueue(saver, taskresult.QueueConfig{Capacity: 1, Concurrency: 1})
				if err != nil {
					t.Fatal(err)
				}
				db, connection := newLifecycleTestDatabase(t, nil)
				host := &ServiceContext{UnboundRecovery: scanner, Results: results, sqlDB: db}
				var releaseOnce sync.Once
				finish := func() { releaseOnce.Do(func() { close(release) }) }
				defer func() { finish(); _ = host.Close() }()
				if err := scanner.Start(); err != nil {
					t.Fatal(err)
				}
				if err := results.Start(); err != nil {
					t.Fatal(err)
				}
				if accepted, err := results.Accept(context.Background(), taskresult.Report{TaskID: 2}); !accepted || err != nil {
					t.Fatalf("result admission: accepted=%v err=%v", accepted, err)
				}
				scanCtx, saveCtx := <-source.entered, <-saver.entered
				waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
				defer cancelWait()
				if err := host.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("active scanner wait did not use the caller deadline: %v", err)
				}
				if scanCtx.Err() != nil || saveCtx.Err() != nil || connection.closed.Load() != 0 {
					t.Fatal("Wait canceled work or closed an active dependency")
				}
				stopped := make(chan struct{})
				go func() { host.Stop(); host.Stop(); close(stopped) }()
				synctest.Wait()
				select {
				case <-stopped:
				default:
					t.Fatal("Stop waited for the scanner's database call")
				}
				if !errors.Is(scanCtx.Err(), context.Canceled) || saveCtx.Err() != nil {
					t.Fatal("Stop did not cancel discovery independently of accepted result persistence")
				}
				shutdown, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
				defer cancelShutdown()
				drained := make(chan error, 1)
				go func() { drained <- host.Drain(shutdown) }()
				synctest.Wait()
				time.Sleep(time.Second)
				if err := <-drained; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("blocked scanner drain did not preserve the shared deadline: %v", err)
				}
				if saveCtx.Err() == nil || connection.closed.Load() != 0 {
					t.Fatal("scanner cleanup blocked result cancellation or released the database early")
				}
				closed := make(chan error, 1)
				go func() { closed <- host.Close() }()
				synctest.Wait()
				select {
				case err := <-closed:
					t.Fatalf("Close returned before the database users actually exited: %v", err)
				default:
				}
				if connection.closed.Load() != 0 || source.calls.Load() != 1 {
					t.Fatal("unfinished scan was replaced or its dependency was closed")
				}
				finish()
				if err := <-closed; err != nil {
					t.Fatal(err)
				}
				if err := host.Wait(shutdown); err != nil {
					t.Fatalf("actual completion did not precede the expired wait context: %v", err)
				}
				if err := host.Close(); err != nil {
					t.Fatal(err)
				}
				if connection.closed.Load() != 1 || source.calls.Load() != 1 {
					t.Fatal("dependency cleanup or scanner execution repeated after shutdown")
				}
			})
		})
	}
}

func TestUnboundHostCleansUpBeforeScannerStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &hostUnboundSource{step: "clock", entered: make(chan context.Context, 1), release: make(chan struct{})}
		scanner := newHostUnboundRuntime(t, source)
		db, connection := newLifecycleTestDatabase(t, nil)
		host := &ServiceContext{UnboundRecovery: scanner, sqlDB: db}
		defer func() { close(source.release); _ = host.Close() }()
		// An initializer may fail after assigning the component but before Start.
		// Ownership cleanup must still terminate its lifecycle without spawning it.
		host.Stop()
		if err := scanner.Start(); err == nil {
			t.Fatal("stopped scanner started during initialization cleanup")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := host.Drain(ctx); err != nil {
			t.Fatalf("unstarted scanner required a new shutdown budget: %v", err)
		}
		if err := host.Wait(ctx); err != nil {
			t.Fatalf("unstarted scanner did not finish on Stop: %v", err)
		}
		if connection.closed.Load() != 0 {
			t.Fatal("Stop or Drain closed dependencies before Close")
		}
		if err := host.Close(); err != nil {
			t.Fatal(err)
		}
		if connection.closed.Load() != 1 || source.calls.Load() != 0 {
			t.Fatal("initializer cleanup started scanning or failed to release its dependency")
		}
	})
}

// hostUnboundSource keeps one selected database boundary active after cancellation
// so host tests can distinguish the stop request from the actual resource release.
type hostUnboundSource struct {
	step    string
	entered chan context.Context
	release chan struct{}
	calls   atomic.Int32
}

func (s *hostUnboundSource) block(ctx context.Context, step string) error {
	if step != s.step {
		return nil
	}
	s.calls.Add(1)
	s.entered <- ctx
	<-s.release
	return ctx.Err()
}

func (s *hostUnboundSource) DatabaseTime(ctx context.Context) (time.Time, error) {
	return time.Now(), s.block(ctx, "clock")
}

func (s *hostUnboundSource) ListUnboundCandidates(ctx context.Context, _, cutoff time.Time, _ *repository.UnboundCursor, _ int) ([]repository.UnboundCandidate, error) {
	if err := s.block(ctx, "candidates"); err != nil {
		return nil, err
	}
	return []repository.UnboundCandidate{{TaskID: 1, InstanceID: 1, AttemptNo: 1, AttemptRegisteredAt: cutoff}}, nil
}

func (s *hostUnboundSource) SettleBatch(ctx context.Context, candidates []repository.UnboundCandidate) (taskrecovery.BatchResult, error) {
	if err := s.block(ctx, "settlement"); err != nil {
		return taskrecovery.BatchResult{}, err
	}
	return taskrecovery.BatchResult{Processed: len(candidates)}, nil
}

// hostUnboundSaver verifies that waiting for scanner cleanup cannot delay the
// shared shutdown deadline reaching accepted result persistence.
type hostUnboundSaver struct {
	entered chan context.Context
	release <-chan struct{}
}

func (s *hostUnboundSaver) Save(ctx context.Context, _ taskresult.Report) (taskresult.SaveResult, error) {
	s.entered <- ctx
	<-s.release
	return taskresult.SaveResult{}, ctx.Err()
}

func newHostUnboundRuntime(t *testing.T, source *hostUnboundSource) *taskrecovery.Runtime {
	t.Helper()
	scanner, err := taskrecovery.NewRuntime(source, source, source, taskrecovery.Config{
		ScanInterval: time.Minute, PageSize: 1, RunTimeout: time.Minute,
		OperationTimeout: time.Minute, ObservationPeriod: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return scanner
}
