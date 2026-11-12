package instancecheck

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestCheckParentBeforeUnusedFacts(t *testing.T) {
	for _, tc := range []struct {
		status model.InstanceStatus
		want   Outcome
	}{
		{model.InstanceWaiting, OutcomeSkipped}, {model.InstanceStopping, OutcomeStopping},
		{model.InstanceSucceeded, OutcomeTerminal}, {model.InstanceFailed, OutcomeTerminal},
		{model.InstanceCanceled, OutcomeTerminal},
	} {
		store := &completionStoreStub{facts: &repository.InstanceCompletion{ID: 1, Status: tc.status, ExecutionConfig: []byte(`invalid`)}}
		checker, _ := NewChecker(store)
		if got, err := checker.Check(context.Background(), 1); err != nil || got != tc.want || store.writes != 0 {
			t.Fatalf("parent status %d: outcome=%d err=%v writes=%d", tc.status, got, err, store.writes)
		}
	}
	store := &completionStoreStub{}
	checker, _ := NewChecker(store)
	if got, err := checker.Check(context.Background(), 1); err != nil || got != OutcomeSkipped {
		t.Fatalf("missing parent: %d %v", got, err)
	}
}

func TestCheckOrdinaryFinalResultsAndUnappliedWrite(t *testing.T) {
	for _, status := range []model.TaskStatus{model.TaskSucceeded, model.TaskFailed} {
		for _, summary := range []*string{nil, checkPointer(""), checkPointer("final summary")} {
			store := &completionStoreStub{facts: checkFacts(status), changed: true}
			store.facts.ResultSummary = summary
			past := time.Now().Add(-time.Hour)
			store.facts.DeadlineAt = &past
			checker, _ := NewChecker(store)
			got, err := checker.Check(context.Background(), 1)
			want := model.InstanceSucceeded
			if status == model.TaskFailed {
				want = model.InstanceFailed
			}
			if err != nil || got != OutcomeTerminal || store.writes != 1 || store.target != want || store.summary != summary {
				t.Fatalf("final status %d: outcome=%d err=%v writes=%d target=%d", status, got, err, store.writes, store.target)
			}
		}
	}
	store := &completionStoreStub{facts: checkFacts(model.TaskSucceeded)}
	checker, _ := NewChecker(store)
	if got, err := checker.Check(context.Background(), 1); err != nil || got != OutcomeNotApplied || store.reads != 1 || store.writes != 1 {
		t.Fatalf("zero-row write must not reread or infer completion: %d %v", got, err)
	}
}

func TestCheckIncompleteAndAnomalousOrdinaryFacts(t *testing.T) {
	for _, status := range []model.TaskStatus{model.TaskWaiting, model.TaskRunning, model.TaskRetryWaiting, model.TaskCanceled} {
		store := &completionStoreStub{facts: checkFacts(status)}
		checker, _ := NewChecker(store)
		if got, err := checker.Check(context.Background(), 1); err != nil || got != OutcomeNotReady || store.writes != 0 {
			t.Fatalf("task %d: outcome=%d err=%v", status, got, err)
		}
	}
	store := &completionStoreStub{facts: checkFacts(model.TaskSucceeded)}
	store.facts.TaskID, store.facts.TaskStatus = nil, nil
	checker, _ := NewChecker(store)
	if got, err := checker.Check(context.Background(), 1); err != nil || got != OutcomeNotReady || store.writes != 0 {
		t.Fatalf("missing ordinary Task: %d %v", got, err)
	}
	store.facts.ExecutionConfig = []byte(`{"schema_version":1,"execution_mode":2,"post_enabled":true}`)
	if got, err := checker.Check(context.Background(), 1); err == nil || got != OutcomeSkipped {
		t.Fatalf("missing sharding assembly was silently ignored: %d %v", got, err)
	}
}

func TestCheckRejectsUnusableRequiredFacts(t *testing.T) {
	for _, input := range []string{`invalid`, `null`, `{}`, `{"schema_version":2,"execution_mode":1}`, `{"schema_version":1,"execution_mode":9}`, `{"schema_version":1,"execution_mode":1}`, `{"schema_version":1,"execution_mode":1,"post_enabled":true}`} {
		store := &completionStoreStub{facts: checkFacts(model.TaskSucceeded)}
		store.facts.ExecutionConfig = []byte(input)
		checker, _ := NewChecker(store)
		if got, err := checker.Check(context.Background(), 1); !errors.Is(err, ErrInvalidConfig) || got == OutcomeNotReady || store.writes != 0 {
			t.Fatalf("unusable required config became incomplete results: %d %v", got, err)
		}
	}
	for _, mutate := range []func(*repository.InstanceCompletion){
		func(f *repository.InstanceCompletion) { f.Status = 99 },
		func(f *repository.InstanceCompletion) { f.TaskStatus = checkPointer(model.TaskStatus(99)) },
		func(f *repository.InstanceCompletion) { f.TaskStatus = nil },
		func(f *repository.InstanceCompletion) { f.ID = 2 },
	} {
		store := &completionStoreStub{facts: checkFacts(model.TaskSucceeded)}
		mutate(store.facts)
		checker, _ := NewChecker(store)
		if got, err := checker.Check(context.Background(), 1); !errors.Is(err, ErrInvalidFacts) || got == OutcomeNotReady || store.writes != 0 {
			t.Fatalf("invalid persisted facts became incomplete results: %d %v", got, err)
		}
	}
}

func TestCheckPreservesStorageFailuresAndCancellation(t *testing.T) {
	cause := errors.New("synthetic uncertain storage outcome")
	for _, writing := range []bool{false, true} {
		store := &completionStoreStub{facts: checkFacts(model.TaskSucceeded), changed: true}
		if writing {
			store.writeErr = cause
		} else {
			store.readErr = cause
		}
		checker, _ := NewChecker(store)
		if got, err := checker.Check(context.Background(), 1); !errors.Is(err, cause) || got == OutcomeTerminal || got == OutcomeNotReady || store.reads != 1 || store.writes > 1 {
			t.Fatalf("storage failure was reclassified or retried: %d %v", got, err)
		}
	}
	store := &completionStoreStub{}
	checker, _ := NewChecker(store)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := checker.Check(ctx, 1); !errors.Is(err, context.Canceled) || store.reads != 0 {
		t.Fatal("canceled check started storage work")
	}
	if _, err := checker.Check(context.Background(), 0); err == nil || store.reads != 0 {
		t.Fatal("invalid ID reached storage")
	}
	if _, err := NewChecker(nil); err == nil {
		t.Fatal("missing dependency was accepted")
	}
	if _, err := NewChecker((*completionStoreStub)(nil)); err == nil {
		t.Fatal("typed nil dependency was accepted")
	}
}

func checkFacts(status model.TaskStatus) *repository.InstanceCompletion {
	return &repository.InstanceCompletion{ID: 1, Status: model.InstanceRunning,
		ExecutionConfig: []byte(`{"schema_version":1,"execution_mode":1,"post_enabled":false}`),
		TaskID:          checkPointer(int64(2)), TaskStatus: &status}
}

func checkPointer[T any](value T) *T { return &value }

type completionStoreStub struct {
	facts    *repository.InstanceCompletion
	readErr  error
	writeErr error
	clockErr error
	now      time.Time
	changed  bool
	reads    int
	writes   int
	clocks   int
	target   model.InstanceStatus
	summary  *string
}

func (s *completionStoreStub) FindCompletion(context.Context, int64) (*repository.InstanceCompletion, error) {
	s.reads++
	return s.facts, s.readErr
}

func (s *completionStoreStub) Complete(_ context.Context, _ int64, target model.InstanceStatus, summary *string) (bool, error) {
	s.writes++
	s.target, s.summary = target, summary
	return s.changed, s.writeErr
}

func (s *completionStoreStub) DatabaseTime(context.Context) (time.Time, error) {
	s.clocks++
	return s.now, s.clockErr
}
