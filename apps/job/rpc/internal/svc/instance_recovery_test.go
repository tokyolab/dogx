package svc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestInstanceRecoveryBranchesDoNotShareBackpressureOrCatchUpTicks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		release := make(chan struct{})
		var runningCalls, stoppingCalls atomic.Int32
		running := func(ctx context.Context, ticks <-chan struct{}) error {
			runningCalls.Add(1)
			<-release
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticks:
					runningCalls.Add(1)
				}
			}
		}
		stopping := func(ctx context.Context, ticks <-chan struct{}) error {
			stoppingCalls.Add(1)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticks:
					stoppingCalls.Add(1)
				}
			}
		}
		done := make(chan error, 1)
		go func() { done <- runInstanceRecovery(ctx, time.Minute, running, stopping) }()
		synctest.Wait()
		if runningCalls.Load() != 1 || stoppingCalls.Load() != 1 {
			t.Fatal("both branches must perform their immediate pass")
		}
		for range 3 {
			time.Sleep(time.Minute)
			synctest.Wait()
		}
		if runningCalls.Load() != 1 || stoppingCalls.Load() != 4 {
			t.Fatal("one blocked branch prevented the other from receiving discovery ticks")
		}
		close(release)
		synctest.Wait()
		if runningCalls.Load() != 1 {
			t.Fatal("busy discovery accumulated catch-up passes")
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if runningCalls.Load() != 2 || stoppingCalls.Load() != 5 {
			t.Fatal("released branch failed to rejoin the common clock")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("recovery exit: %v", err)
		}
	})
}

func TestInstanceRecoveryWaitsForBothActualReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		release := make(chan struct{})
		var canceled atomic.Int32
		blocked := func(ctx context.Context, _ <-chan struct{}) error {
			<-ctx.Done()
			canceled.Add(1)
			<-release
			return ctx.Err()
		}
		done := make(chan error, 1)
		go func() { done <- runInstanceRecovery(ctx, time.Minute, blocked, blocked) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		if canceled.Load() != 2 {
			t.Fatal("term cancellation failed to reach both branches")
		}
		select {
		case <-done:
			t.Fatal("cancellation was mistaken for actual dependency release")
		default:
		}
		close(release)
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("recovery exit: %v", err)
		}
	})
}

func TestInstanceRecoveryUnexpectedExitCancelsSibling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cause := errors.New("synthetic branch failure")
		for _, failure := range []error{nil, cause} {
			returned := false
			err := runInstanceRecovery(context.Background(), time.Minute,
				func(context.Context, <-chan struct{}) error { return failure },
				func(ctx context.Context, _ <-chan struct{}) error {
					<-ctx.Done()
					returned = true
					return ctx.Err()
				})
			if err == nil || !returned || failure != nil && !errors.Is(err, failure) {
				t.Fatal("unexpected exit lost its error or left sibling discovery running")
			}
		}
	})
}
