package svc

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/instancecheck"
)

func TestHostWaitsForActualInstanceCheckExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := &hostCompletionStub{entered: make(chan context.Context, 1), release: make(chan struct{})}
		queue, err := instancecheck.NewQueue(check, instancecheck.Config{Capacity: 1, Concurrency: 1, Timeout: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		if err := queue.Start(); err != nil {
			t.Fatal(err)
		}
		db, connection := newLifecycleTestDatabase(t, nil)
		host := &ServiceContext{InstanceChecks: queue, sqlDB: db}
		if !queue.Hint(1) {
			t.Fatal("completion hint was not admitted")
		}
		checkCtx := <-check.entered
		shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		drained := make(chan error, 1)
		go func() { drained <- host.Drain(shutdown) }()
		synctest.Wait()
		if queue.Hint(2) {
			t.Fatal("host shutdown left hint admission open")
		}
		time.Sleep(time.Second)
		if err := <-drained; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("drain did not preserve the host deadline: %v", err)
		}
		if checkCtx.Err() == nil || connection.closed.Load() != 0 {
			t.Fatal("shutdown either missed check cancellation or closed its database early")
		}
		closed := make(chan error, 1)
		go func() { closed <- host.Close() }()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("Close returned before the checker actually exited: %v", err)
		default:
		}
		close(check.release)
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		if connection.closed.Load() != 1 {
			t.Fatal("database was not released after check completion")
		}
	})
}

type hostCompletionStub struct {
	entered chan context.Context
	release chan struct{}
}

func (s *hostCompletionStub) Check(ctx context.Context, _ int64) (instancecheck.Outcome, error) {
	s.entered <- ctx
	<-s.release
	return instancecheck.OutcomeNotReady, ctx.Err()
}
