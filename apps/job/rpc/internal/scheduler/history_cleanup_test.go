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

func TestHistoryCleanupOptionValidatesAndDoesNotStartWork(t *testing.T) {
	cfg := Config{Prefix: "/test/history-cleanup-option"}
	if _, err := New(&clientv3.Client{}, triggerServiceStub{}, triggerServiceStub{}, cfg, WithHistoryCleanup(nil)); err == nil {
		t.Fatal("nil history cleanup option accepted")
	}
	calls := 0
	r, err := New(&clientv3.Client{}, triggerServiceStub{}, triggerServiceStub{}, cfg,
		WithHistoryCleanup(func(context.Context) error { calls++; return nil }))
	if err != nil || r == nil || r.cleanupHistory == nil || calls != 0 {
		t.Fatalf("construction started or omitted history cleanup: calls=%d error=%v", calls, err)
	}
}

func TestHistoryCleanupLosingTermWaitsForActualExitBeforeReelection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := unitRuntime(triggerServiceStub{})
		r.newCron = func() cronEngine { return &cronStub{} }
		r.timing.backoffMin, r.timing.backoffMax = time.Millisecond, time.Millisecond
		ended, entered, canceled, release, nextEntered := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		releaseWork := func() { releaseOnce.Do(func() { close(release) }) }
		defer releaseWork()
		var opens, closes, cleanupCalls atomic.Int32
		r.coordination = coordinationStub{openFn: func(context.Context, int, string) (electionSession, error) {
			stop := make(chan struct{})
			if opens.Add(1) == 1 {
				stop = ended
			}
			return electionSessionStub{ended: stop, campaignFn: func(context.Context) error { return nil },
				closeFn: func(context.Context) error { closes.Add(1); return nil }}, nil
		}}
		if err := WithHistoryCleanup(func(ctx context.Context) error {
			if cleanupCalls.Add(1) == 1 {
				close(entered)
				<-ctx.Done()
				close(canceled)
				// A canceled database operation can still own a connection. The
				// next local term must not start until that operation returns.
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
		time.Sleep(r.timing.request + time.Second)
		synctest.Wait()
		if opens.Load() != 1 || closes.Load() != 0 || cleanupCalls.Load() != 1 || r.Stats().Terms != 1 {
			t.Fatalf("old cleanup overlapped reelection: opens=%d closes=%d calls=%d terms=%d", opens.Load(), closes.Load(), cleanupCalls.Load(), r.Stats().Terms)
		}
		select {
		case <-done:
			t.Fatal("runtime returned before canceled cleanup actually exited")
		default:
		}
		releaseWork()
		<-nextEntered
		if opens.Load() != 2 || closes.Load() != 1 || cleanupCalls.Load() != 2 || r.Stats().Terms != 2 {
			t.Fatal("new elected term did not restart history cleanup")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) || closes.Load() != 2 {
			t.Fatalf("history cleanup shutdown=%v closes=%d", err, closes.Load())
		}
	})
}

func TestHistoryCleanupDoesNotRunOnStandbyOrFailedCampaign(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, campaignFails := range []bool{false, true} {
			r := unitRuntime(triggerServiceStub{})
			var calls atomic.Int32
			if err := WithHistoryCleanup(func(context.Context) error { calls.Add(1); return nil })(r); err != nil {
				t.Fatal(err)
			}
			campaigning := make(chan struct{})
			r.coordination = coordinationStub{openFn: func(context.Context, int, string) (electionSession, error) {
				return electionSessionStub{ended: make(chan struct{}), campaignFn: func(ctx context.Context) error {
					close(campaigning)
					<-ctx.Done()
					if campaignFails {
						return errors.New("synthetic failed campaign")
					}
					return nil
				}}, nil
			}}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- r.Run(ctx) }()
			<-campaigning
			time.Sleep(time.Hour)
			synctest.Wait()
			if calls.Load() != 0 || r.Stats().Terms != 0 {
				t.Fatal("standby started history cleanup before election success")
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) || calls.Load() != 0 || r.Stats().Terms != 0 {
				t.Fatalf("unelected cleanup: calls=%d terms=%d error=%v", calls.Load(), r.Stats().Terms, err)
			}
		}
	})
}

func TestHistoryCleanupIsIndependentOfInstanceDiscovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := unitRuntime(triggerServiceStub{})
		r.newCron = func() cronEngine { return &cronStub{} }
		discoveryStarted, cleanupStarted := make(chan struct{}), make(chan struct{})
		if err := WithInstanceRecovery(func(ctx context.Context) error {
			close(discoveryStarted)
			<-ctx.Done()
			return ctx.Err()
		})(r); err != nil {
			t.Fatal(err)
		}
		if err := WithHistoryCleanup(func(ctx context.Context) error {
			close(cleanupStarted)
			<-ctx.Done()
			return ctx.Err()
		})(r); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() { r.runTerm(ctx); close(done) }()
		synctest.Wait()
		select {
		case <-discoveryStarted:
		default:
			t.Fatal("instance discovery was not started")
		}
		select {
		case <-cleanupStarted:
		default:
			t.Fatal("history cleanup waited for instance discovery to finish")
		}
		cancel()
		<-done
	})
}
