package instancecheck

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/model"
)

func TestCheckTimeoutOnlyAfterExplicitIncompleteOrdinaryFacts(t *testing.T) {
	for _, state := range []model.TaskStatus{model.TaskWaiting, model.TaskRunning, model.TaskRetryWaiting, model.TaskCanceled, 0} {
		store := timeoutCheckStore(state)
		if state == 0 {
			store.facts.TaskID, store.facts.TaskStatus = nil, nil
		}
		requester := &checkTimeoutStub{decision: newTimeoutDecision()}
		var published []instancestop.Decision
		var resumed []int64
		checker, err := NewChecker(store, WithStopping(requester,
			func(d instancestop.Decision) { published = append(published, d) },
			func(id int64) { resumed = append(resumed, id) }))
		if err != nil {
			t.Fatal(err)
		}
		got, err := checker.Check(context.Background(), 1)
		if err != nil || got != OutcomeStopping || store.reads != 1 || store.clocks != 1 || store.writes != 0 ||
			requester.calls != 1 || len(published) != 1 || !published[0].New || len(resumed) != 0 {
			t.Fatalf("incomplete state %d: outcome=%d err=%v", state, got, err)
		}
	}
	store := timeoutCheckStore(model.TaskRunning)
	store.now = store.facts.DeadlineAt.Add(-time.Nanosecond)
	requester := &checkTimeoutStub{}
	checker := newTimeoutChecker(t, store, requester)
	if got, err := checker.Check(context.Background(), 1); err != nil || got != OutcomeNotReady || store.clocks != 1 || requester.calls != 0 {
		t.Fatal("future database deadline requested a timeout")
	}
}

func TestCheckNeverFallsThroughNormalCompletionOrErrorsToTimeout(t *testing.T) {
	failure := errors.New("synthetic completion failure")
	for _, scenario := range []struct {
		name    string
		change  func(*completionStoreStub)
		want    Outcome
		wantErr error
	}{
		{"late success", func(s *completionStoreStub) { s.facts.TaskStatus = checkPointer(model.TaskSucceeded); s.changed = true }, OutcomeTerminal, nil},
		{"late failure", func(s *completionStoreStub) { s.facts.TaskStatus = checkPointer(model.TaskFailed); s.changed = true }, OutcomeTerminal, nil},
		{"zero completion update", func(s *completionStoreStub) { s.facts.TaskStatus = checkPointer(model.TaskSucceeded) }, OutcomeNotApplied, nil},
		{"completion write failure", func(s *completionStoreStub) {
			s.facts.TaskStatus = checkPointer(model.TaskSucceeded)
			s.writeErr = failure
		}, OutcomeSkipped, failure},
		{"read failure", func(s *completionStoreStub) { s.readErr = failure }, OutcomeSkipped, failure},
		{"invalid configuration", func(s *completionStoreStub) { s.facts.ExecutionConfig = []byte(`{}`) }, OutcomeSkipped, ErrInvalidConfig},
		{"unknown task", func(s *completionStoreStub) { s.facts.TaskStatus = checkPointer(model.TaskStatus(99)) }, OutcomeSkipped, ErrInvalidFacts},
		{"sharded", func(s *completionStoreStub) {
			s.facts.ExecutionConfig = []byte(`{"schema_version":1,"execution_mode":2}`)
		}, OutcomeSkipped, ErrInvalidConfig},
		{"waiting", func(s *completionStoreStub) { s.facts.Status = model.InstanceWaiting }, OutcomeSkipped, nil},
		{"terminal", func(s *completionStoreStub) { s.facts.Status = model.InstanceSucceeded }, OutcomeTerminal, nil},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store := timeoutCheckStore(model.TaskRunning)
			scenario.change(store)
			requester := &checkTimeoutStub{}
			checker := newTimeoutChecker(t, store, requester)
			got, err := checker.Check(context.Background(), 1)
			if got != scenario.want || !errors.Is(err, scenario.wantErr) || store.clocks != 0 || requester.calls != 0 {
				t.Fatalf("completion outcome incorrectly entered timeout: %d %v", got, err)
			}
		})
	}
}

func TestCheckTimeoutClockAndRequestErrorsDoNotHandoff(t *testing.T) {
	failure := errors.New("synthetic timeout failure")
	for _, scenario := range []struct {
		name   string
		change func(*completionStoreStub, *checkTimeoutStub)
		want   error
	}{
		{"missing deadline", func(s *completionStoreStub, _ *checkTimeoutStub) { s.facts.DeadlineAt = nil }, ErrInvalidFacts},
		{"empty deadline", func(s *completionStoreStub, _ *checkTimeoutStub) { s.facts.DeadlineAt = new(time.Time) }, ErrInvalidFacts},
		{"clock failed", func(s *completionStoreStub, _ *checkTimeoutStub) { s.clockErr = failure }, failure},
		{"clock empty", func(s *completionStoreStub, _ *checkTimeoutStub) { s.now = time.Time{} }, ErrInvalidFacts},
		{"commit unconfirmed", func(_ *completionStoreStub, r *checkTimeoutStub) { r.err = failure }, failure},
		{"wrong instance", func(_ *completionStoreStub, r *checkTimeoutStub) { r.decision.InstanceID++ }, ErrInvalidFacts},
		{"invalid new state", func(_ *completionStoreStub, r *checkTimeoutStub) { r.decision.Status = model.InstanceSucceeded }, ErrInvalidFacts},
		{"invalid new reason", func(_ *completionStoreStub, r *checkTimeoutStub) {
			r.decision.EndReason = checkPointer(model.EndReasonManualStop)
		}, ErrInvalidFacts},
		{"missing new group", func(_ *completionStoreStub, r *checkTimeoutStub) { r.decision.ExecutorGroupID = 0 }, ErrInvalidFacts},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store, requester := timeoutCheckStore(model.TaskRunning), &checkTimeoutStub{decision: newTimeoutDecision()}
			scenario.change(store, requester)
			checker := newTimeoutChecker(t, store, requester)
			got, err := checker.Check(context.Background(), 1)
			if got != OutcomeSkipped || !errors.Is(err, scenario.want) {
				t.Fatalf("failed timeout became a stop decision: %d %v", got, err)
			}
		})
	}
}

func TestCheckTimeoutRespectsLockedStateAndOnlyResumesExistingStops(t *testing.T) {
	for _, scenario := range []struct {
		state model.InstanceStatus
		want  Outcome
	}{
		{model.InstanceWaiting, OutcomeSkipped}, {model.InstanceRunning, OutcomeNotReady},
		{model.InstanceStopping, OutcomeStopping}, {model.InstanceSucceeded, OutcomeTerminal},
		{model.InstanceFailed, OutcomeTerminal}, {model.InstanceCanceled, OutcomeTerminal},
	} {
		store := timeoutCheckStore(model.TaskRunning)
		requester := &checkTimeoutStub{decision: instancestop.Decision{InstanceID: 1, Status: scenario.state, EndReason: checkPointer(model.EndReasonManualStop)}}
		var published, resumed int
		checker, _ := NewChecker(store, WithStopping(requester, func(instancestop.Decision) { published++ }, func(id int64) {
			if id != 1 {
				t.Error("resumed wrong Instance")
			}
			resumed++
		}))
		got, err := checker.Check(context.Background(), 1)
		wantResumed := 0
		if scenario.state == model.InstanceStopping {
			wantResumed = 1
		}
		if err != nil || got != scenario.want || published != 0 || resumed != wantResumed {
			t.Fatalf("locked state %d changed its existing decision: outcome=%d err=%v", scenario.state, got, err)
		}
	}
	store := timeoutCheckStore(model.TaskRunning)
	store.facts.Status, store.facts.ExecutionConfig = model.InstanceStopping, []byte(`invalid`)
	requester := &checkTimeoutStub{}
	resumed := 0
	checker, _ := NewChecker(store, WithStopping(requester, func(instancestop.Decision) { t.Fatal("existing stop sent another notification") }, func(int64) { resumed++ }))
	if got, err := checker.Check(context.Background(), 1); err != nil || got != OutcomeStopping || resumed != 1 || store.clocks != 0 || requester.calls != 0 {
		t.Fatal("observed STOPPING did not go directly to database convergence")
	}
}

func TestCheckStoppingValidatesDependenciesAndKeepsCommittedOutcomeOnHintPanic(t *testing.T) {
	for _, option := range []Option{nil, WithStopping(nil, func(instancestop.Decision) {}, func(int64) {}),
		WithStopping((*checkTimeoutStub)(nil), func(instancestop.Decision) {}, func(int64) {}),
		WithStopping(&checkTimeoutStub{}, nil, func(int64) {}), WithStopping(&checkTimeoutStub{}, func(instancestop.Decision) {}, nil)} {
		if _, err := NewChecker(&completionStoreStub{}, option); err == nil {
			t.Fatal("invalid stopping dependencies accepted")
		}
	}
	store, requester := timeoutCheckStore(model.TaskRunning), &checkTimeoutStub{decision: newTimeoutDecision()}
	checker, _ := NewChecker(store, WithStopping(requester, func(instancestop.Decision) { panic("synthetic private handoff failure") }, func(int64) {}))
	if got, err := checker.Check(context.Background(), 1); err != nil || got != OutcomeStopping {
		t.Fatal("hint panic changed a confirmed stopping decision into failure")
	}
}

func TestCheckHandsOffCommittedTimeoutAfterContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := timeoutCheckStore(model.TaskRunning)
	requester := &checkTimeoutStub{decision: newTimeoutDecision(), beforeReturn: cancel}
	var published []instancestop.Decision
	checker, err := NewChecker(store, WithStopping(requester,
		func(decision instancestop.Decision) { published = append(published, decision) },
		func(int64) { t.Fatal("new timeout resumed an existing stop") }))
	if err != nil {
		t.Fatal(err)
	}
	got, err := checker.Check(ctx, 1)
	if err != nil || got != OutcomeStopping || ctx.Err() != context.Canceled ||
		requester.calls != 1 || len(published) != 1 || !published[0].New {
		t.Fatalf("canceled caller lost a committed timeout handoff: outcome=%d err=%v published=%d", got, err, len(published))
	}
}

func timeoutCheckStore(state model.TaskStatus) *completionStoreStub {
	now := time.Date(2026, 11, 5, 1, 2, 3, 0, time.UTC)
	store := &completionStoreStub{facts: checkFacts(state), now: now}
	store.facts.DeadlineAt = &now
	return store
}

func newTimeoutDecision() instancestop.Decision {
	return instancestop.Decision{InstanceID: 1, ExecutorGroupID: 17, Status: model.InstanceStopping, New: true, EndReason: checkPointer(model.EndReasonInstanceTimeout)}
}

func newTimeoutChecker(t *testing.T, store *completionStoreStub, requester *checkTimeoutStub) *Checker {
	t.Helper()
	checker, err := NewChecker(store, WithStopping(requester, func(instancestop.Decision) { t.Error("unexpected new stop handoff") }, func(int64) { t.Error("unexpected resume handoff") }))
	if err != nil {
		t.Fatal(err)
	}
	return checker
}

type checkTimeoutStub struct {
	decision     instancestop.Decision
	err          error
	calls        int
	beforeReturn func()
}

func (s *checkTimeoutStub) RequestTimeout(context.Context, int64) (instancestop.Decision, error) {
	s.calls++
	if s.beforeReturn != nil {
		s.beforeReturn()
	}
	return s.decision, s.err
}
