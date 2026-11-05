package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestInstanceRecoveryOptionValidatesAndDoesNotStartWork(t *testing.T) {
	config := Config{Prefix: "/test/instance-recovery-option"}
	for _, option := range []Option{nil, WithInstanceRecovery(nil)} {
		if _, err := New(&clientv3.Client{}, triggerServiceStub{}, triggerServiceStub{}, config, option); err == nil {
			t.Fatal("invalid recovery option accepted")
		}
	}
	calls := 0
	r, err := New(&clientv3.Client{}, triggerServiceStub{}, triggerServiceStub{}, config,
		WithInstanceRecovery(func(context.Context) error { calls++; return nil }))
	if err != nil || r == nil || r.recoverInstances == nil || calls != 0 {
		t.Fatalf("construction started or omitted instance discovery: calls=%d error=%v", calls, err)
	}
}

func TestInstanceRecoveryLosingTermWaitsForActualExitBeforeReelection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := unitRuntime(triggerServiceStub{})
		r.newCron = func() cronEngine { return &cronStub{} }
		r.timing.backoffMin, r.timing.backoffMax = time.Millisecond, time.Millisecond
		ended, entered, canceled, release, nextEntered := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		releaseWork := func() { releaseOnce.Do(func() { close(release) }) }
		defer releaseWork()
		var opens, closes, recoveryCalls atomic.Int32
		r.coordination = coordinationStub{openFn: func(context.Context, int, string) (electionSession, error) {
			stop := make(chan struct{})
			if opens.Add(1) == 1 {
				stop = ended
			}
			return electionSessionStub{ended: stop, campaignFn: func(context.Context) error { return nil },
				closeFn: func(context.Context) error { closes.Add(1); return nil }}, nil
		}}
		if err := WithInstanceRecovery(func(ctx context.Context) error {
			if recoveryCalls.Add(1) == 1 {
				close(entered)
				<-ctx.Done()
				close(canceled)
				// Cancellation is a request; a driver may still be releasing its
				// connection. Reelection must wait for this actual return.
				<-release
			} else {
				close(nextEntered)
				<-ctx.Done()
			}
			return ctx.Err()
		})(r); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()
		<-entered
		close(ended)
		<-canceled
		// The cleanup diagnostic threshold cannot release a still-running term.
		time.Sleep(r.timing.request + time.Second)
		synctest.Wait()
		if opens.Load() != 1 || closes.Load() != 0 || recoveryCalls.Load() != 1 || r.Stats().Terms != 1 {
			t.Fatalf("old discovery overlapped reelection: opens=%d closes=%d calls=%d terms=%d", opens.Load(), closes.Load(), recoveryCalls.Load(), r.Stats().Terms)
		}
		select {
		case <-done:
			t.Fatal("runtime exited before canceled discovery returned")
		default:
		}
		releaseWork()
		<-nextEntered
		if opens.Load() != 2 || closes.Load() != 1 || recoveryCalls.Load() != 2 || r.Stats().Terms != 2 {
			t.Fatal("new elected term failed to rebuild instance discovery")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) || closes.Load() != 2 {
			t.Fatalf("instance discovery shutdown=%v closes=%d", err, closes.Load())
		}
	})
}

func TestInstanceRecoveryDoesNotStartForUnsuccessfulCampaign(t *testing.T) {
	r := unitRuntime(triggerServiceStub{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	if err := WithInstanceRecovery(func(context.Context) error { calls++; return nil })(r); err != nil {
		t.Fatal(err)
	}
	r.coordination = coordinationStub{openFn: func(context.Context, int, string) (electionSession, error) {
		return electionSessionStub{ended: make(chan struct{}), campaignFn: func(context.Context) error {
			cancel()
			return errors.New("synthetic unsuccessful campaign")
		}}, nil
	}}
	if err := r.Run(ctx); !errors.Is(err, context.Canceled) || calls != 0 || r.Stats().Terms != 0 {
		t.Fatalf("non-leader started discovery: calls=%d terms=%d error=%v", calls, r.Stats().Terms, err)
	}
}
