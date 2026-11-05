package svc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestStopAndWaitPreserveDependenciesUntilRuntimeReturns(t *testing.T) {
	runCtx, cancel := context.WithCancel(context.Background())
	runtimeDone := make(chan struct{})
	db, connection := newLifecycleTestDatabase(t, nil)
	etcd := clientv3.NewCtxClient(context.Background())
	s := &ServiceContext{cancel: cancel, done: runtimeDone, sqlDB: db, etcd: etcd}
	var releaseOnce sync.Once
	releaseRuntime := func() { releaseOnce.Do(func() { close(runtimeDone) }) }
	t.Cleanup(func() {
		releaseRuntime()
		_ = s.Close()
	})

	// Observing a caller deadline must not itself stop runtime work.
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer waitCancel()
	started := time.Now()
	if err := s.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait error = %v, want caller deadline", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("Wait exceeded the caller's deadline")
	}
	if runCtx.Err() != nil {
		t.Fatal("Wait cancelled the runtime")
	}
	if etcd.Ctx().Err() != nil || connection.closed.Load() != 0 {
		t.Fatal("Wait deadline closed shared dependencies")
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("database remained unusable after Wait: %v", err)
	}

	stopped := make(chan struct{})
	go func() {
		s.Stop()
		s.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop waited for runtime completion")
	}
	if !errors.Is(runCtx.Err(), context.Canceled) {
		t.Fatal("Stop did not request scheduler cancellation")
	}
	if etcd.Ctx().Err() != nil || connection.closed.Load() != 0 {
		t.Fatal("Stop closed dependencies while runtime work was active")
	}
	cancelledCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	if err := s.Wait(cancelledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait cancellation error = %v", err)
	}
	releaseRuntime()
	if err := s.Wait(cancelledCtx); err != nil {
		t.Fatalf("already completed runtime did not take precedence: %v", err)
	}
	if etcd.Ctx().Err() != nil || connection.closed.Load() != 0 {
		t.Fatal("observing runtime completion closed dependencies before the host drain barrier")
	}
	// NewCtxClient intentionally has no network connection, so its Close returns
	// context.Canceled. It still exposes whether the dependency was closed early.
	if err := s.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("close error = %v, want the context-only etcd client's close result", err)
	}
	if etcd.Ctx().Err() == nil || connection.closed.Load() != 1 {
		t.Fatal("Close did not release dependencies after runtime completion")
	}
}

func TestConcurrentStopWaitAndCloseShareOneCleanup(t *testing.T) {
	runCtx, cancel := context.WithCancel(context.Background())
	runtimeDone := make(chan struct{})
	closeFailure := errors.New("synthetic database close failure")
	db, connection := newLifecycleTestDatabase(t, closeFailure)
	var cancellations atomic.Int32
	s := &ServiceContext{
		cancel: func() { cancellations.Add(1); cancel() },
		done:   runtimeDone, sqlDB: db,
	}
	var releaseOnce sync.Once
	releaseRuntime := func() { releaseOnce.Do(func() { close(runtimeDone) }) }
	t.Cleanup(func() {
		releaseRuntime()
		_ = s.Close()
	})
	const callers = 8
	closed := make(chan error, callers)
	waited := make(chan error, callers)
	stopped := make(chan struct{}, callers)
	for range callers {
		go func() { closed <- s.Close() }()
		go func() { waited <- s.Wait(context.Background()) }()
		go func() { s.Stop(); stopped <- struct{}{} }()
	}
	select {
	case <-runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("concurrent shutdown did not request cancellation")
	}
	for range callers {
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("Stop blocked behind Close waiting for the runtime")
		}
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before runtime completion: %v", err)
	case err := <-waited:
		t.Fatalf("Wait returned before runtime completion: %v", err)
	default:
	}
	if connection.closed.Load() != 0 {
		t.Fatal("concurrent Close released an active dependency")
	}
	releaseRuntime()
	for range callers {
		select {
		case err := <-waited:
			if err != nil {
				t.Fatalf("wait after runtime completion: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Wait did not observe runtime completion")
		}
		select {
		case err := <-closed:
			if !errors.Is(err, closeFailure) {
				t.Fatalf("Close lost the shared cleanup error: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent Close did not finish")
		}
	}
	if cancellations.Load() != 1 || connection.closed.Load() != 1 {
		t.Fatalf("cleanup repeated: cancellations=%d, database closes=%d", cancellations.Load(), connection.closed.Load())
	}
	if err := s.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("repeated Close lost its original error: %v", err)
	}
}

func TestCloseWaitsForRuntimeCleanupAndIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runtimeDone := make(chan struct{})
	s := &ServiceContext{cancel: cancel, done: runtimeDone}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("close did not cancel runtime")
	}
	select {
	case err := <-closed:
		t.Fatalf("close returned before runtime stopped: %v", err)
	default:
	}
	close(runtimeDone)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not finish")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceClock(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	if got := (&ServiceContext{Clock: func() time.Time { return now }}).Now(); !got.Equal(now) {
		t.Fatalf("clock=%s", got)
	}
	before := time.Now()
	got := (&ServiceContext{}).Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("fallback clock=%s", got)
	}
	if err := (&ServiceContext{}).Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (&ServiceContext{}).Wait(ctx); err != nil {
		t.Fatalf("absent runtime should need no wait: %v", err)
	}
}

// lifecycleTestConnector implements the standard SQL test boundary without
// connecting to any database; the pool still performs real ownership cleanup.
type lifecycleTestConnector struct{ connection *lifecycleTestConnection }

func (c *lifecycleTestConnector) Connect(context.Context) (driver.Conn, error) {
	return c.connection, nil
}

func (c *lifecycleTestConnector) Driver() driver.Driver { return c }

func (c *lifecycleTestConnector) Open(string) (driver.Conn, error) { return c.connection, nil }

type lifecycleTestConnection struct {
	closed   atomic.Int32
	closeErr error
}

func (c *lifecycleTestConnection) Close() error {
	c.closed.Add(1)
	return c.closeErr
}

func (c *lifecycleTestConnection) Ping(ctx context.Context) error { return ctx.Err() }

func (*lifecycleTestConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected SQL statement in lifecycle test")
}

func (*lifecycleTestConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected SQL transaction in lifecycle test")
}

func newLifecycleTestDatabase(t *testing.T, closeErr error) (*sql.DB, *lifecycleTestConnection) {
	t.Helper()
	connection := &lifecycleTestConnection{closeErr: closeErr}
	db := sql.OpenDB(&lifecycleTestConnector{connection: connection})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db, connection
}
