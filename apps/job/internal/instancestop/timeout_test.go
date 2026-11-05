package instancestop

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestTimeoutDecisionUsesFreshLockedTimeAndDoesNotReadChildren(t *testing.T) {
	for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
		f := newStopFixture(t)
		deadline := f.now
		f.instance.DeadlineAt = &deadline
		f.now = deadline.Add(-time.Hour)
		f.onSQL = func(name string, _ context.Context) error {
			if name == "lock" {
				f.now = deadline.Add(offset)
			}
			return nil
		}
		decision, err := f.controller.RequestTimeout(context.Background(), 11)
		if err != nil || decision.InstanceID != 11 || decision.ExecutorGroupID != 17 || f.nowait {
			t.Fatalf("timeout decision=%+v err=%v", decision, err)
		}
		if offset < 0 {
			if decision.New || decision.Status != model.InstanceRunning || !reflect.DeepEqual(f.calls, []string{"lock", "clock"}) {
				t.Fatal("deadline was inferred before the locked database time")
			}
			continue
		}
		if !decision.New || decision.Status != model.InstanceStopping || decision.EndReason == nil || *decision.EndReason != model.EndReasonInstanceTimeout ||
			!reflect.DeepEqual(f.calls, []string{"lock", "clock", "record"}) || f.record.expected != model.InstanceRunning ||
			f.record.reason != model.EndReasonInstanceTimeout || f.record.summary != timeoutSummary || !f.record.at.Equal(f.now) {
			t.Fatal("timeout did not freeze its reason after the fresh locked clock or read Tasks inside the parent transaction")
		}
	}
}

func TestTimeoutPreservesWaitingAndExistingStopOrTerminalDecisions(t *testing.T) {
	for _, state := range []model.InstanceStatus{model.InstanceWaiting, model.InstanceStopping, model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled} {
		f := newStopFixture(t)
		f.instance.Status = state
		reason := model.EndReasonManualStop
		f.instance.EndReason = &reason
		f.instance.StartedAt, f.instance.DeadlineAt = nil, nil
		f.now = time.Time{}
		decision, err := f.controller.RequestTimeout(context.Background(), 11)
		if err != nil || decision.New || decision.Status != state || decision.EndReason == nil || *decision.EndReason != reason ||
			!reflect.DeepEqual(f.calls, []string{"lock"}) {
			t.Fatalf("observed state %d became a timeout: %+v %v", state, decision, err)
		}
		*decision.EndReason = "changed caller copy"
		if *f.instance.EndReason != model.EndReasonManualStop {
			t.Fatal("timeout observation exposed mutable repository facts")
		}
	}
}

func TestTimeoutErrorsNeverPublishNewDecision(t *testing.T) {
	failure := errors.New("synthetic timeout persistence failure")
	for _, scenario := range []struct {
		name   string
		change func(*stopFixture)
		want   error
	}{
		{"missing", func(f *stopFixture) { f.instance = nil }, repository.ErrInstanceNotFound},
		{"lock", func(f *stopFixture) { f.failures["lock"] = failure }, failure},
		{"clock", func(f *stopFixture) { f.failures["clock"] = failure }, failure},
		{"write", func(f *stopFixture) { f.failures["record"] = failure }, failure},
		{"commit unknown", func(f *stopFixture) { f.commitErr = failure }, failure},
		{"zero update", func(f *stopFixture) { f.recordChanged = false }, ErrInvalidState},
		{"zero clock", func(f *stopFixture) { f.now = time.Time{} }, ErrInvalidState},
		{"missing deadline", func(f *stopFixture) { f.instance.DeadlineAt = nil }, ErrInvalidState},
		{"missing start", func(f *stopFixture) { f.instance.StartedAt = nil }, ErrInvalidState},
		{"invalid deadline", func(f *stopFixture) { f.instance.DeadlineAt = f.instance.StartedAt }, ErrInvalidState},
		{"already finished", func(f *stopFixture) { f.instance.FinishedAt = &f.now }, ErrInvalidState},
		{"invalid group", func(f *stopFixture) { f.instance.ExecutorGroupID = 0 }, ErrInvalidState},
		{"wrong ID", func(f *stopFixture) { f.instance.ID++ }, ErrInvalidState},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newStopFixture(t)
			deadline := f.now
			f.instance.DeadlineAt = &deadline
			scenario.change(f)
			decision, err := f.controller.RequestTimeout(context.Background(), 11)
			if decision != (Decision{}) || !errors.Is(err, scenario.want) {
				t.Fatalf("failed timeout published facts: %+v %v", decision, err)
			}
		})
	}
	f := newStopFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, id := range []int64{-1, 0, 11} {
		if decision, err := f.controller.RequestTimeout(ctx, id); err == nil || decision != (Decision{}) {
			t.Fatal("invalid timeout request was accepted")
		}
	}
	if f.runs != 0 {
		t.Fatal("canceled timeout began a transaction")
	}
}
