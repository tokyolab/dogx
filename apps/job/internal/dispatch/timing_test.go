package dispatch

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestDispatcherDeploymentTimingReachesWorkAndBusyWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		registrar := &dispatcherRegistrarStub{call: func(ctx context.Context, _ int64) (*RegisteredAttempt, error) {
			deadline, _ := ctx.Deadline()
			if deadline.Sub(started) != 7*time.Second {
				t.Fatal("configured total budget did not reach registration")
			}
			time.Sleep(time.Second)
			attempt := dispatcherAttempt()
			return &attempt, nil
		}}
		sender := &dispatcherSenderStub{call: func(ctx context.Context, _ RegisteredAttempt) (Response, error) {
			deadline, _ := ctx.Deadline()
			if time.Until(deadline) != 6*time.Second {
				t.Fatal("sender received a renewed total budget")
			}
			return Response{Outcome: OutcomeBusy}, nil
		}}
		store := &dispatcherStoreStub{}
		d := testDispatcher(t, registrar, sender, store, WithDispatchTiming(7*time.Second, 10*time.Second))
		result, err := d.DispatchOrdinary(context.Background(), 11)
		if err != nil || !result.Applied {
			t.Fatalf("dispatch result=%+v err=%v", result, err)
		}
		calls := store.snapshot()
		if len(calls) != 1 || calls[0].busy.Delay < 8*time.Second || calls[0].busy.Delay > 12*time.Second {
			t.Fatal("BUSY settlement did not use configured jittered delay")
		}
	})
}
