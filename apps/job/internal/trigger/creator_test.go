package trigger

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"github.com/zeromicro/go-zero/core/logx/logtest"
)

var testNow = time.Date(2026, 10, 16, 12, 0, 5, 0, time.Local)

func validDefinition() *model.JobDefinition {
	due := testNow.Add(-5 * time.Second)
	return &model.JobDefinition{Base: basemodel.Base{ID: 7}, Name: "job", ExecutorGroupID: 2, ProcessorName: "processor", ExecutionMode: model.ExecutionModeNormal,
		Params: "current", CronExpr: "0 * * * * *", Status: basemodel.RecordStatusEnabled, MisfirePolicy: model.MisfireIgnore, BlockPolicy: model.BlockDiscard,
		MaxRetries: 3, RetryIntervalSeconds: 10, TimeoutSeconds: 900, NextTriggerAt: &due}
}

func testCreator(t *testing.T) (*Creator, *repositoryStub) {
	t.Helper()
	r := &repositoryStub{job: validDefinition(), available: true, created: true, failures: map[string]error{}}
	s, err := New(func(ctx context.Context, fn func(repository.Repositories) error) error {
		return fn(repository.Repositories{Jobs: r, Groups: r, Instances: r})
	}, WithClock(func() time.Time { return testNow }))
	if err != nil {
		t.Fatal(err)
	}
	return s, r
}

func TestAutomaticDecisionAndAtomicResult(t *testing.T) {
	for _, tc := range []struct {
		name      string
		lag       time.Duration
		misfire   model.MisfirePolicy
		active    bool
		duplicate bool
		reason    string
		trigger   model.TriggerType
		created   bool
	}{
		{"exact tolerance", 5 * time.Second, model.MisfireIgnore, false, false, "created", model.TriggerScheduled, true},
		{"outside tolerance ignored", 5*time.Second + time.Microsecond, model.MisfireIgnore, true, false, "misfire_ignored", 0, false},
		{"many missed coalesced", 72 * time.Hour, model.MisfireOnce, false, false, "created", model.TriggerMisfire, true},
		{"blocked", time.Second, model.MisfireOnce, true, false, "blocked", 0, false},
		{"duplicate", time.Second, model.MisfireOnce, false, true, "duplicate", model.TriggerScheduled, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r := testCreator(t)
			due := testNow.Add(-tc.lag)
			r.job.NextTriggerAt, r.job.MisfirePolicy, r.active, r.created = &due, tc.misfire, tc.active, !tc.duplicate
			hints := 0
			s.hint = func(int64) { hints++ }
			result, err := s.TriggerAutomatic(context.Background(), 7)
			if err != nil || result.Reason != tc.reason || result.Created != tc.created {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if r.advanced == nil || !r.advanced.After(testNow) {
				t.Fatalf("missing future progress: %v", r.advanced)
			}
			if tc.created && hints != 1 || !tc.created && hints != 0 {
				t.Fatalf("hints=%d", hints)
			}
			if r.inserted != nil && (r.inserted.TriggerType != tc.trigger || !r.inserted.ScheduledAt.Equal(due)) {
				t.Fatalf("snapshot=%+v", r.inserted)
			}
			if tc.reason == "misfire_ignored" && strings.Contains(strings.Join(r.calls, ","), "active") {
				t.Fatal("ignored misfire queried active instances")
			}
		})
	}
}

func TestAutomaticFailureDoesNotPublish(t *testing.T) {
	for _, step := range []string{"lock", "active", "insert", "advance", "commit"} {
		t.Run(step, func(t *testing.T) {
			s, r := testCreator(t)
			failure := errors.New("database transport failure")
			if step == "commit" {
				s.transact = func(ctx context.Context, fn func(repository.Repositories) error) error {
					if err := fn(repository.Repositories{Jobs: r, Groups: r, Instances: r}); err != nil {
						return err
					}
					return failure
				}
			} else {
				r.failures[step] = failure
			}
			s.hint = func(int64) { t.Error("unconfirmed transaction published a hint") }
			result, err := s.TriggerAutomatic(context.Background(), 7)
			if !errors.Is(err, failure) || result != (AutomaticResult{}) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestAutomaticNoopAndInvalidSchedule(t *testing.T) {
	for _, kind := range []string{"deleted", "disabled", "not_due", "no_progress", "invalid_cron"} {
		t.Run(kind, func(t *testing.T) {
			s, r := testCreator(t)
			switch kind {
			case "deleted":
				r.failures["lock"] = repository.ErrJobNotFound
			case "disabled":
				r.job.Status = basemodel.RecordStatusDisabled
			case "not_due":
				future := testNow.Add(time.Second)
				r.job.NextTriggerAt = &future
			case "no_progress":
				r.job.NextTriggerAt = nil
			case "invalid_cron":
				r.job.CronExpr = "broken"
			}
			_, err := s.TriggerAutomatic(context.Background(), 7)
			if (err != nil) != (kind == "invalid_cron") {
				t.Fatalf("err=%v", err)
			}
			if r.advanced != nil || r.inserted != nil {
				t.Fatal("no-op changed persistence")
			}
		})
	}
}

func TestManualReplayAndConflicts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		prior    *repository.ManualIdentity
		override bool
		params   string
		conflict bool
		fallback bool
	}{
		{"inherit ignores edited params", &repository.ManualIdentity{ID: 44}, false, "", false, false},
		{"same normalized override", &repository.ManualIdentity{ID: 44, ParamsOverridden: true, OverrideParams: stringPtr("x")}, true, " x ", false, false},
		{"different flag", &repository.ManualIdentity{ID: 44}, true, "", true, false},
		{"different content", &repository.ManualIdentity{ID: 44, ParamsOverridden: true, OverrideParams: stringPtr("x")}, true, "y", true, false},
		{"insert conflict fresh read", &repository.ManualIdentity{ID: 44}, false, "", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r := testCreator(t)
			r.identities = []*repository.ManualIdentity{tc.prior}
			if tc.fallback {
				r.identities = []*repository.ManualIdentity{nil, tc.prior}
				r.created = false
			} else {
				r.available = false
				r.active = true
			}
			id, err := s.TriggerManual(context.Background(), ManualTrigger{JobID: 7, RequestID: uuid.New(), OperatorID: 8, ParamsOverridden: tc.override, Params: tc.params})
			if tc.conflict {
				if !errors.Is(err, ErrRequestConflict) {
					t.Fatalf("err=%v", err)
				}
				return
			}
			if err != nil || id != 44 {
				t.Fatalf("id=%d err=%v", id, err)
			}
			if !tc.fallback && strings.Contains(strings.Join(r.calls, ","), "available") {
				t.Fatal("replay checked current group")
			}
		})
	}
}

func TestManualSnapshotAndRejections(t *testing.T) {
	s, r := testCreator(t)
	r.job.Status = basemodel.RecordStatusDisabled
	request := ManualTrigger{JobID: 7, RequestID: uuid.Nil, OperatorID: 8, ParamsOverridden: true, Params: "   "}
	id, err := s.TriggerManual(context.Background(), request)
	if err != nil || id != 99 {
		t.Fatalf("id=%d err=%v", id, err)
	}
	if r.inserted.ExecutionConfig.Params != "" || r.inserted.TriggeredBy == nil || *r.inserted.TriggeredBy != 8 || r.inserted.Status != model.InstanceWaiting || r.inserted.ScheduledAt != nil {
		t.Fatalf("snapshot=%+v", r.inserted)
	}
	if r.advanced != nil {
		t.Fatal("manual creation changed automatic progress")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*repositoryStub)
		want   error
	}{
		{"deleted", func(r *repositoryStub) { r.failures["lock"] = repository.ErrJobNotFound }, repository.ErrJobNotFound},
		{"unavailable group", func(r *repositoryStub) { r.available = false }, repository.ErrExecutorGroupUnavailable},
		{"blocked", func(r *repositoryStub) { r.active = true }, ErrBlocked},
		{"unresolved conflict", func(r *repositoryStub) { r.created = false }, ErrIndeterminate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r := testCreator(t)
			tc.mutate(r)
			if _, err := s.TriggerManual(context.Background(), request); !errors.Is(err, tc.want) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestManualFailuresNeverPublishUnconfirmedInstance(t *testing.T) {
	for _, step := range []string{"lock", "find", "available", "active", "insert", "commit"} {
		t.Run(step, func(t *testing.T) {
			s, r := testCreator(t)
			failure := errors.New("manual transaction failed")
			if step == "commit" {
				s.transact = func(ctx context.Context, fn func(repository.Repositories) error) error {
					if err := fn(repository.Repositories{Jobs: r, Groups: r, Instances: r}); err != nil {
						return err
					}
					return failure
				}
			} else {
				r.failures[step] = failure
			}
			s.hint = func(int64) { t.Error("failed manual transaction published a hint") }
			id, err := s.TriggerManual(context.Background(), ManualTrigger{JobID: 7, OperatorID: 8})
			if id != 0 || !errors.Is(err, failure) {
				t.Fatalf("id=%d err=%v", id, err)
			}
		})
	}
}

func TestCreationCapacitySharedAndHeldUntilUnwind(t *testing.T) {
	s, _ := testCreator(t)
	s.slots = make(chan struct{}, 1)
	entered, allowReturn, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	s.transact = func(ctx context.Context, fn func(repository.Repositories) error) error {
		close(entered)
		<-allowReturn
		return ctx.Err()
	}
	go func() { _, err := s.TriggerAutomatic(ctx, 7); done <- err }()
	<-entered
	cancel()
	_, err := s.TriggerManual(context.Background(), ManualTrigger{JobID: 7, RequestID: uuid.New(), OperatorID: 8})
	if !errors.Is(err, ErrCapacityBusy) {
		t.Fatalf("manual exceeded shared capacity: %v", err)
	}
	close(allowReturn)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if len(s.slots) != 0 {
		t.Fatal("creation slot leaked")
	}
}

func TestAutomaticCapacityWaitHonorsProcessingBudget(t *testing.T) {
	s, r := testCreator(t)
	s.slots = make(chan struct{}, 1)
	s.slots <- struct{}{}
	s.budget = 20 * time.Millisecond
	_, err := s.TriggerAutomatic(context.Background(), 7)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("automatic quota wait err=%v", err)
	}
	if len(r.calls) != 0 || len(s.slots) != 1 {
		t.Fatalf("waiting worker touched persistence or released another slot: calls=%v slots=%d", r.calls, len(s.slots))
	}
	<-s.slots
	if result, err := s.TriggerAutomatic(context.Background(), 7); err != nil || !result.Created {
		t.Fatalf("capacity did not recover: result=%+v err=%v", result, err)
	}
}

func TestCreationCancellationDoesNotEnterTransaction(t *testing.T) {
	s, r := testCreator(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.TriggerAutomatic(ctx, 7); !errors.Is(err, context.Canceled) {
		t.Fatalf("automatic cancellation=%v", err)
	}
	if _, err := s.TriggerManual(ctx, ManualTrigger{JobID: 7, OperatorID: 8}); !errors.Is(err, context.Canceled) {
		t.Fatalf("manual cancellation=%v", err)
	}
	if len(r.calls) != 0 || len(s.slots) != 0 {
		t.Fatalf("canceled creation calls=%v slots=%d", r.calls, len(s.slots))
	}
}

func TestAutomaticSkipLogsOnlyConfirmedCommitWithDecisionFacts(t *testing.T) {
	for _, commitFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "committed", true: "unknown commit"}[commitFails], func(t *testing.T) {
			collector := logtest.NewCollector(t)
			s, r := testCreator(t)
			r.active = true
			if commitFails {
				s.transact = func(ctx context.Context, fn func(repository.Repositories) error) error {
					if err := fn(repository.Repositories{Jobs: r, Groups: r, Instances: r}); err != nil {
						return err
					}
					return errors.New("commit response lost")
				}
			}
			_, _ = s.TriggerAutomatic(context.Background(), 7)
			logs := collector.String()
			if commitFails {
				if strings.Contains(logs, "job automatic trigger skipped") {
					t.Fatalf("unconfirmed commit logged successful skip: %s", logs)
				}
				return
			}
			for _, field := range []string{"job_id", "scheduled_at", "decision_at", "next_trigger_at", "BLOCK_DISCARDED"} {
				if !strings.Contains(logs, field) {
					t.Errorf("skip log missing %s: %s", field, logs)
				}
			}
			if strings.Count(logs, "job automatic trigger skipped") != 1 {
				t.Fatalf("skip events=%s", logs)
			}
		})
	}
}

func TestConstructorRejectsInvalidDependenciesAndLimits(t *testing.T) {
	runner := func(context.Context, func(repository.Repositories) error) error { return nil }
	for _, tc := range []struct {
		name    string
		runner  repository.TransactionRunner
		options []Option
	}{
		{"nil runner", nil, nil},
		{"nil clock", runner, []Option{WithClock(nil)}},
		{"zero capacity", runner, []Option{WithCreationLimits(0, time.Second)}},
		{"zero budget", runner, []Option{WithCreationLimits(1, 0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.runner, tc.options...); err == nil {
				t.Fatal("accepted invalid trigger dependencies")
			}
		})
	}
	hints := 0
	s, err := New(runner, WithCreationLimits(2, time.Second), WithInstanceHint(func(int64) { hints++ }))
	if err != nil || cap(s.slots) != 2 || s.budget != time.Second {
		t.Fatalf("creator=%+v err=%v", s, err)
	}
	s.hint(1)
	if hints != 1 {
		t.Fatal("configured hint was not retained")
	}
}

func TestInvalidTriggerInputDoesNotEnterTransaction(t *testing.T) {
	for _, request := range []ManualTrigger{
		{JobID: 0, OperatorID: 8},
		{JobID: 7, OperatorID: 0},
		{JobID: 7, OperatorID: 8, Params: "not overridden"},
		{JobID: 7, OperatorID: 8, ParamsOverridden: true, Params: "\x00"},
		{JobID: 7, OperatorID: 8, ParamsOverridden: true, Params: string([]byte{0xff})},
	} {
		s, r := testCreator(t)
		if _, err := s.TriggerManual(context.Background(), request); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("manual input=%+v err=%v", request, err)
		}
		if len(r.calls) != 0 {
			t.Fatalf("invalid input reached persistence: %v", r.calls)
		}
	}
	s, r := testCreator(t)
	if _, err := s.TriggerAutomatic(context.Background(), 0); !errors.Is(err, ErrInvalidInput) || len(r.calls) != 0 {
		t.Fatalf("automatic invalid ID err=%v calls=%v", err, r.calls)
	}
}

func TestNilLockedDefinitionIsTechnicalFailure(t *testing.T) {
	for _, manual := range []bool{false, true} {
		t.Run(fmt.Sprint("manual=", manual), func(t *testing.T) {
			s, r := testCreator(t)
			r.job = nil
			s.hint = func(int64) { t.Error("invalid repository result published hint") }
			var err error
			if manual {
				var id int64
				id, err = s.TriggerManual(context.Background(), ManualTrigger{JobID: 7, OperatorID: 8})
				if id != 0 {
					t.Fatalf("invalid result returned id=%d", id)
				}
			} else {
				var result AutomaticResult
				result, err = s.TriggerAutomatic(context.Background(), 7)
				if result != (AutomaticResult{}) {
					t.Fatalf("invalid result=%+v", result)
				}
			}
			if err == nil || errors.Is(err, repository.ErrJobNotFound) || errors.Is(err, ErrInvalidInput) {
				t.Fatalf("nil definition lost technical-failure semantics: %v", err)
			}
			if strings.Join(r.calls, ",") != "lock" {
				t.Fatalf("continued after invalid lock result: %v", r.calls)
			}
		})
	}
}

func TestTriggerPreservesTypedPersistenceError(t *testing.T) {
	cause := &testPersistenceError{}
	for _, manual := range []bool{false, true} {
		t.Run(fmt.Sprint("manual=", manual), func(t *testing.T) {
			s, r := testCreator(t)
			r.failures["lock"] = fmt.Errorf("repository lock: %w", cause)
			var err error
			if manual {
				_, err = s.TriggerManual(context.Background(), ManualTrigger{JobID: 7, OperatorID: 8})
			} else {
				_, err = s.TriggerAutomatic(context.Background(), 7)
			}
			var target *testPersistenceError
			if !errors.Is(err, cause) || !errors.As(err, &target) || target != cause {
				t.Fatalf("persistence error chain lost: %v", err)
			}
		})
	}
}

type testPersistenceError struct{}

func (*testPersistenceError) Error() string { return "driver failed" }

func stringPtr(s string) *string { return &s }
