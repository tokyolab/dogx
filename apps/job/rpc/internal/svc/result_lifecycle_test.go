package svc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestServiceStopPreservesAcceptedResultsAndCloseWaitsForSaver(t *testing.T) {
	runCtx, cancel := context.WithCancel(context.Background())
	runtimeDone := make(chan struct{})
	saveStarted := make(chan context.Context, 1)
	releaseSave := make(chan struct{})
	queue := newResultLifecycleQueue(t, &resultLifecycleSaver{save: func(ctx context.Context, _ taskresult.Report) (taskresult.SaveResult, error) {
		saveStarted <- ctx
		<-releaseSave
		return taskresult.SaveResult{Applied: true}, nil
	}})
	db, connection := newLifecycleTestDatabase(t, nil)
	etcd := clientv3.NewCtxClient(context.Background())
	admissionAtCancel := make(chan error, 1)
	s := &ServiceContext{Results: queue, cancel: func() {
		_, err := queue.Accept(context.Background(), taskresult.Report{TaskID: 2})
		admissionAtCancel <- err
		cancel()
	}, done: runtimeDone, sqlDB: db, etcd: etcd}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSave); close(runtimeDone) }) }
	t.Cleanup(func() {
		release()
		_ = s.Close()
	})
	if accepted, err := queue.Accept(context.Background(), taskresult.Report{TaskID: 1}); !accepted || err != nil {
		t.Fatalf("accept result: accepted=%v error=%v", accepted, err)
	}
	var saveCtx context.Context
	select {
	case saveCtx = <-saveStarted:
	case <-time.After(time.Second):
		t.Fatal("result saver did not start")
	}

	// Wait only observes completion; its timeout must not stop either lifecycle.
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer waitCancel()
	if err := s.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("active work wait error=%v", err)
	}
	if runCtx.Err() != nil || saveCtx.Err() != nil {
		t.Fatal("Wait cancelled the runtime or accepted result")
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
		t.Fatal("Stop waited for accepted result completion")
	}
	select {
	case err := <-admissionAtCancel:
		if !errors.Is(err, taskresult.ErrNotReady) {
			t.Fatalf("runtime cancellation preceded result admission closure: %v", err)
		}
	default:
		t.Fatal("Stop did not invoke runtime cancellation")
	}
	if !errors.Is(runCtx.Err(), context.Canceled) {
		t.Fatal("Stop did not cancel runtime producers")
	}
	if saveCtx.Err() != nil {
		t.Fatal("Stop cancelled an accepted result before the drain deadline")
	}
	if accepted, err := queue.Accept(context.Background(), taskresult.Report{TaskID: 2}); accepted || !errors.Is(err, taskresult.ErrNotReady) {
		t.Fatalf("stopped admission: accepted=%v error=%v", accepted, err)
	}

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned while accepted work was active: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if etcd.Ctx().Err() != nil || connection.closed.Load() != 0 {
		t.Fatal("Close released dependencies before accepted result completion")
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("database became unusable during result saving: %v", err)
	}
	if saveCtx.Err() != nil {
		t.Fatal("Close supplied an unapproved result cancellation deadline")
	}
	release()
	select {
	case err := <-closed:
		// The context-only etcd client has no connection and reports cancellation.
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("close error=%v, want context-only etcd close result", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after all dependency users returned")
	}
	if etcd.Ctx().Err() == nil || connection.closed.Load() != 1 {
		t.Fatal("Close did not release dependencies exactly once")
	}
}

func TestServiceDrainDeadlineReachesResultsWhileRuntimeIsBlocked(t *testing.T) {
	runtimeDone := make(chan struct{})
	runCtx, cancel := context.WithCancel(context.Background())
	saveStarted := make(chan context.Context, 1)
	releaseSave := make(chan struct{})
	queue := newResultLifecycleQueue(t, &resultLifecycleSaver{save: func(ctx context.Context, _ taskresult.Report) (taskresult.SaveResult, error) {
		saveStarted <- ctx
		// Model a driver operation that has not returned despite cancellation.
		<-releaseSave
		return taskresult.SaveResult{}, ctx.Err()
	}})
	db, connection := newLifecycleTestDatabase(t, nil)
	etcd := clientv3.NewCtxClient(context.Background())
	s := &ServiceContext{Results: queue, cancel: cancel, done: runtimeDone, sqlDB: db, etcd: etcd}
	var runtimeOnce, saverOnce sync.Once
	finishRuntime := func() { runtimeOnce.Do(func() { close(runtimeDone) }) }
	finishSave := func() { saverOnce.Do(func() { close(releaseSave) }) }
	t.Cleanup(func() {
		finishRuntime()
		finishSave()
		_ = s.Close()
	})
	if accepted, err := queue.Accept(context.Background(), taskresult.Report{TaskID: 1}); !accepted || err != nil {
		t.Fatalf("accept result: accepted=%v error=%v", accepted, err)
	}
	var saveCtx context.Context
	select {
	case saveCtx = <-saveStarted:
	case <-time.After(time.Second):
		t.Fatal("result saver did not start")
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer drainCancel()
	drained := make(chan error, 1)
	go func() { drained <- s.Drain(drainCtx) }()
	select {
	case err := <-drained:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("blocked drain error=%v, want shared deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Drain ignored the shared deadline")
	}
	if runCtx.Err() == nil {
		t.Fatal("Drain did not stop runtime producers")
	}
	select {
	case <-saveCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("blocked runtime prevented the shared deadline from cancelling result saving")
	}
	if etcd.Ctx().Err() != nil || connection.closed.Load() != 0 {
		t.Fatal("Drain timeout released dependencies still in use")
	}

	// Once the runtime returns, the ignored saver cancellation remains a real
	// dependency user. Waiting must include it without granting a new grace period.
	finishRuntime()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer waitCancel()
	if err := s.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait treated cancellation as saver completion: %v", err)
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before ignored cancellation completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if etcd.Ctx().Err() != nil || connection.closed.Load() != 0 {
		t.Fatal("Close released the dependencies of an unfinished saver")
	}
	finishSave()
	select {
	case err := <-closed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("close error=%v, want context-only etcd close result", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not observe actual saver completion")
	}
	if err := s.Wait(waitCtx); err != nil {
		t.Fatalf("actual completion did not take precedence over expired caller: %v", err)
	}
	if etcd.Ctx().Err() == nil || connection.closed.Load() != 1 {
		t.Fatal("dependencies were not released after actual saver completion")
	}
}

// resultLifecycleSaver controls actual Save completion independently of Context
// cancellation so lifecycle tests can detect premature dependency cleanup.
type resultLifecycleSaver struct {
	save func(context.Context, taskresult.Report) (taskresult.SaveResult, error)
}

func (s *resultLifecycleSaver) Save(ctx context.Context, report taskresult.Report) (taskresult.SaveResult, error) {
	return s.save(ctx, report)
}

func newResultLifecycleQueue(t *testing.T, saver *resultLifecycleSaver) *taskresult.Queue {
	t.Helper()
	queue, err := taskresult.NewQueue(saver, taskresult.QueueConfig{Capacity: 1, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(); err != nil {
		t.Fatal(err)
	}
	return queue
}
