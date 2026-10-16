package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestConfigDefaultsAndNamespaceIsolation(t *testing.T) {
	config, err := (Config{Prefix: "/dogx/test"}).defaults()
	if err != nil || config.QueueCapacity != 1024 || config.CreationConcurrency != 32 || config.LeaseTTLSeconds != 15 || config.RecoveryLimit != 1024 || config.QueryTimeout != 5*time.Second {
		t.Fatalf("defaults=%+v error=%v", config, err)
	}
	for _, invalid := range []Config{
		{}, {Prefix: "/"}, {Prefix: "relative"}, {Prefix: "/dogx/../test"}, {Prefix: "/dogx/test/"},
		{Prefix: "/dogx\\test"}, {Prefix: "/dogx/\n"}, {Prefix: "/dogx/test", QueueCapacity: -1},
		{Prefix: "/dogx/test", CreationConcurrency: -1}, {Prefix: "/dogx/test", LeaseTTLSeconds: -1},
		{Prefix: "/dogx/test", RecoveryLimit: -1}, {Prefix: "/dogx/test", QueryTimeout: -time.Second},
	} {
		if _, err := invalid.defaults(); err == nil {
			t.Errorf("accepted invalid configuration: %+v", invalid)
		}
	}
	k := makeKeys(config.Prefix)
	if k.election != "/dogx/test/scheduler/election" || k.changed != "/dogx/test/scheduler/job-config-changed" {
		t.Fatalf("unexpected namespace: %+v", k)
	}
	if _, err := New(nil, triggerServiceStub{}, config); err == nil {
		t.Fatal("accepted nil etcd client")
	}
	if _, err := New(&clientv3.Client{}, nil, config); err == nil {
		t.Fatal("accepted nil service")
	}
	if _, err := New(&clientv3.Client{}, triggerServiceStub{}, Config{}); err == nil {
		t.Fatal("accepted invalid runtime namespace")
	}
}

func TestNotificationIsOneCallAndHonorsUpstreamBudget(t *testing.T) {
	r := unitRuntime(triggerServiceStub{})
	var calls int
	r.coordination = coordinationStub{putFn: func(ctx context.Context, key, value string) error {
		calls++
		deadline, exists := ctx.Deadline()
		if !exists || time.Until(deadline) > 50*time.Millisecond || key != r.keys.changed || value != "reload" {
			t.Errorf("wrong notification request or deadline: key=%s value=%s deadline=%s", key, value, deadline)
		}
		<-ctx.Done()
		return ctx.Err()
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := r.Notify(ctx); !errors.Is(err, context.DeadlineExceeded) || calls != 1 || r.Stats().NotificationErrors != 1 {
		t.Fatalf("notify calls=%d stats=%+v error=%v", calls, r.Stats(), err)
	}
	if err := r.Notify(ctx); !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatal("already canceled notification contacted etcd")
	}
}

func TestRuntimeRejectsConcurrentRunAndCanceledStartup(t *testing.T) {
	r := unitRuntime(triggerServiceStub{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opened := make(chan struct{})
	r.coordination = coordinationStub{openFn: func(ctx context.Context, ttl int, prefix string) (electionSession, error) {
		close(opened)
		if ttl != 15 || prefix != r.keys.election {
			t.Errorf("wrong election configuration: ttl=%d prefix=%s", ttl, prefix)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	awaitSignal(t, opened)
	if err := r.Run(ctx); err == nil {
		t.Fatal("concurrent runtime accepted")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled session startup did not exit")
	}
	if err := r.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled restart returned %v", err)
	}
}

func TestCampaignSuccessCannotStartAnExpiredSession(t *testing.T) {
	r := unitRuntime(triggerServiceStub{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ended := make(chan struct{})
	var closed atomic.Int32
	r.coordination = coordinationStub{openFn: func(context.Context, int, string) (electionSession, error) {
		return electionSessionStub{
			ended:      ended,
			campaignFn: func(context.Context) error { close(ended); return nil },
			closeFn: func(ctx context.Context) error {
				if _, ok := ctx.Deadline(); !ok || ctx.Err() != nil {
					t.Error("session cleanup lacks an independent bounded context")
				}
				closed.Add(1)
				cancel()
				return errors.New("lease already revoked")
			},
		}, nil
	}}
	if err := r.Run(ctx); !errors.Is(err, context.Canceled) || r.Stats().Terms != 0 || closed.Load() != 1 {
		t.Fatalf("expired campaign installed runtime or skipped cleanup: stats=%+v closes=%d error=%v", r.Stats(), closed.Load(), err)
	}
}

func TestBackoffStopsOnCancellationAndCapsDelay(t *testing.T) {
	r := unitRuntime(triggerServiceStub{})
	r.timing.backoffMin, r.timing.backoffMax = time.Millisecond, 2*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r.backoff(ctx, 10) {
		t.Fatal("canceled backoff continued")
	}
	started := time.Now()
	if !r.backoff(context.Background(), 1000000) || time.Since(started) < time.Millisecond {
		t.Fatal("bounded jitter lost the minimum backoff")
	}
}
