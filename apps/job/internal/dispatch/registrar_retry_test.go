package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestRetryRegistrarUsesOriginalCandidateAndFrozenOpportunityBudget(t *testing.T) {
	f, candidate := newRetryRegistrationFixture(t)
	start, deadline := *f.instance.StartedAt, *f.instance.DeadlineAt
	got, err := f.registrar.RetryOrdinary(context.Background(), candidate)
	if err != nil || got == nil || got.TaskID != candidate.TaskID || got.InstanceID != candidate.InstanceID ||
		got.AttemptNo != candidate.AttemptNo+1 || got.ExecutorGroupID != 41 || got.JobID != 21 ||
		!got.RegisteredAt.Equal(f.now) || !got.DeadlineAt.Equal(deadline) || got.ExecutionConfig.MaxRetries != 0 {
		t.Fatalf("retry registration=%+v err=%v", got, err)
	}
	want := repository.RetryRegistration{TaskID: candidate.TaskID, InstanceID: candidate.InstanceID,
		ExpectedAttemptNo: candidate.AttemptNo, MaxAttempts: 1, RegisteredAt: f.now}
	if f.retryWrite != want || f.retryCalls != 1 || f.started || f.inserted ||
		!f.instance.StartedAt.Equal(start) || !f.instance.DeadlineAt.Equal(deadline) {
		t.Fatal("retry replaced the candidate, used attempt numbering as budget, or reset first-start facts")
	}
}

func TestRetryRegistrarWidensMaximumRetryBudgetBeforeAddingFirstAttempt(t *testing.T) {
	f, candidate := newRetryRegistrationFixture(t)
	var config model.ExecutionConfig
	if err := json.Unmarshal(f.instance.ExecutionConfig, &config); err != nil {
		t.Fatal(err)
	}
	config.MaxRetries = math.MaxInt32
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	f.instance.ExecutionConfig = data
	if got, err := f.registrar.RetryOrdinary(context.Background(), candidate); err != nil || got == nil || f.retryWrite.MaxAttempts != int64(math.MaxInt32)+1 {
		t.Fatalf("maximum budget registration=%+v err=%v write=%+v", got, err, f.retryWrite)
	}
}

func TestRetryRegistrarNeverPublishesUnconfirmedOrMissingUpdate(t *testing.T) {
	failure := errors.New("synthetic retry persistence failure")
	for _, test := range []struct {
		name      string
		configure func(*registrationFixture)
		want      error
	}{
		{"old candidate", func(f *registrationFixture) { f.retryTask = nil }, nil},
		{"update failed", func(f *registrationFixture) { f.retryErr = failure }, failure},
		{"commit unknown", func(f *registrationFixture) { f.commitErr = failure }, failure},
		{"different task", func(f *registrationFixture) { f.retryTask.TaskID++ }, ErrInvalidInstanceState},
		{"different parent", func(f *registrationFixture) { f.retryTask.InstanceID++ }, ErrInvalidInstanceState},
		{"old attempt", func(f *registrationFixture) { f.retryTask.AttemptNo-- }, ErrInvalidInstanceState},
		{"skipped attempt", func(f *registrationFixture) { f.retryTask.AttemptNo++ }, ErrInvalidInstanceState},
		{"wrapped attempt", func(f *registrationFixture) { f.retryTask.AttemptNo = math.MinInt64 }, ErrInvalidInstanceState},
		{"unoccupied opportunity", func(f *registrationFixture) { f.retryTask.UsedAttempts = 0 }, ErrInvalidInstanceState},
		{"exceeded opportunity budget", func(f *registrationFixture) { f.retryTask.UsedAttempts = 2 }, ErrInvalidInstanceState},
		{"wrong registration time", func(f *registrationFixture) { f.retryTask.RegisteredAt = f.now.Add(-time.Second) }, ErrInvalidInstanceState},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, candidate := newRetryRegistrationFixture(t)
			test.configure(f)
			got, err := f.registrar.RetryOrdinary(context.Background(), candidate)
			if got != nil || !errors.Is(err, test.want) || f.retryCalls != 1 {
				t.Fatalf("unconfirmed retry=%+v err=%v want=%v", got, err, test.want)
			}
		})
	}
}

func TestRetryRegistrarHintsStoppingAndExpiredParentsOnlyAfterTransaction(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		for _, commitFailure := range []bool{false, true} {
			f, candidate := newRetryRegistrationFixture(t)
			if stopping {
				f.instance.Status = model.InstanceStopping
			} else {
				f.instance.DeadlineAt = &f.now
			}
			// Neither path requires a snapshot to decide that registration is forbidden.
			f.instance.ExecutionConfig = json.RawMessage(`{"invalid`)
			if commitFailure {
				f.commitErr = errors.New("synthetic commit acknowledgement lost")
			}
			hints := 0
			WithRegistrationHint(func(id int64) {
				if !f.transactionReturned || id != candidate.InstanceID {
					t.Fatal("parent hint ran inside the transaction or changed the parent identity")
				}
				hints++
			})(f.registrar)
			got, err := f.registrar.RetryOrdinary(context.Background(), candidate)
			wantHints := 1
			if commitFailure {
				wantHints = 0
			}
			if got != nil || !errors.Is(err, f.commitErr) || hints != wantHints || f.retryCalls != 0 || f.started || f.inserted {
				t.Fatalf("stopping=%v commitFailure=%v retry=%+v err=%v hints=%d", stopping, commitFailure, got, err, hints)
			}
		}
	}
}

func TestRetryRegistrarSkipsInactiveParentsWithoutSnapshotOrTaskReads(t *testing.T) {
	for _, state := range []model.InstanceStatus{model.InstanceWaiting, model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled} {
		f, candidate := newRetryRegistrationFixture(t)
		f.instance.Status = state
		f.instance.ExecutionConfig = json.RawMessage(`{"invalid`)
		if got, err := f.registrar.RetryOrdinary(context.Background(), candidate); got != nil || err != nil || f.clockRead || f.retryCalls != 0 {
			t.Fatalf("inactive retry=%+v err=%v state=%v", got, err, state)
		}
	}
	f, candidate := newRetryRegistrationFixture(t)
	f.instance = nil
	if got, err := f.registrar.RetryOrdinary(context.Background(), candidate); got != nil || err != nil || f.clockRead || f.retryCalls != 0 {
		t.Fatalf("missing parent retry=%+v err=%v", got, err)
	}
	f, candidate = newRetryRegistrationFixture(t)
	f.instance.ExecutionConfig = json.RawMessage(`{"schema_version":1,"execution_mode":2}`)
	if got, err := f.registrar.RetryOrdinary(context.Background(), candidate); got != nil || err != nil || f.retryCalls != 0 {
		t.Fatalf("sharded retry=%+v err=%v", got, err)
	}
}

func TestRetryRegistrarRejectsBrokenParentFactsBeforeUpdatingTask(t *testing.T) {
	broken := errors.New("synthetic database failure")
	for _, test := range []struct {
		name      string
		configure func(*registrationFixture)
		want      error
	}{
		{"lock conflict", func(f *registrationFixture) { f.lockErr = repository.ErrInstanceLocked }, repository.ErrInstanceLocked},
		{"lock failure", func(f *registrationFixture) { f.lockErr = broken }, broken},
		{"clock failure", func(f *registrationFixture) { f.clockErr = broken }, broken},
		{"empty clock", func(f *registrationFixture) { f.now = time.Time{} }, ErrInvalidInstanceState},
		{"wrong parent", func(f *registrationFixture) { f.instance.ID++ }, ErrInvalidInstanceState},
		{"missing job", func(f *registrationFixture) { f.instance.JobID = 0 }, ErrInvalidInstanceState},
		{"missing group", func(f *registrationFixture) { f.instance.ExecutorGroupID = 0 }, ErrInvalidInstanceState},
		{"missing start", func(f *registrationFixture) { f.instance.StartedAt = nil }, ErrInvalidInstanceState},
		{"empty start", func(f *registrationFixture) { f.instance.StartedAt = new(time.Time) }, ErrInvalidInstanceState},
		{"missing deadline", func(f *registrationFixture) { f.instance.DeadlineAt = nil }, ErrInvalidInstanceState},
		{"deadline before start", func(f *registrationFixture) {
			value := f.instance.StartedAt.Add(-time.Second)
			f.instance.DeadlineAt = &value
		}, ErrInvalidInstanceState},
		{"already finished", func(f *registrationFixture) { f.instance.FinishedAt = &f.now }, ErrInvalidInstanceState},
		{"broken snapshot", func(f *registrationFixture) { f.instance.ExecutionConfig = json.RawMessage(`{`) }, ErrInvalidExecutionConfig},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, candidate := newRetryRegistrationFixture(t)
			test.configure(f)
			if got, err := f.registrar.RetryOrdinary(context.Background(), candidate); got != nil || !errors.Is(err, test.want) ||
				!errors.Is(f.callbackErr, test.want) || f.retryCalls != 0 {
				t.Fatalf("invalid retry=%+v err=%v callback=%v", got, err, f.callbackErr)
			}
		})
	}
}

func TestRetryRegistrarHonorsEarlierDeadlineAndRejectsCanceledOrInvalidCalls(t *testing.T) {
	f, candidate := newRetryRegistrationFixture(t, WithRegistrationHint(nil))
	deadline := time.Now().Add(time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if got, err := f.registrar.RetryOrdinary(ctx, candidate); got == nil || err != nil || !f.deadline.Equal(deadline) {
		t.Fatalf("bounded retry=%+v err=%v deadline=%v", got, err, f.deadline)
	}
	f, candidate = newRetryRegistrationFixture(t)
	ctx, stop := context.WithCancel(context.Background())
	stop()
	if got, err := f.registrar.RetryOrdinary(ctx, candidate); got != nil || !errors.Is(err, context.Canceled) || f.called {
		t.Fatalf("canceled retry=%+v err=%v", got, err)
	}
	for _, invalid := range []repository.RetryCandidate{
		{TaskID: 0, InstanceID: 11, AttemptNo: 7}, {TaskID: 31, InstanceID: -1, AttemptNo: 7}, {TaskID: 31, InstanceID: 11, AttemptNo: 0},
	} {
		if got, err := f.registrar.RetryOrdinary(context.Background(), invalid); got != nil || err == nil || f.called {
			t.Fatalf("invalid candidate retry=%+v err=%v", got, err)
		}
	}
}

func newRetryRegistrationFixture(t *testing.T, options ...RegistrarOption) (*registrationFixture, repository.RetryCandidate) {
	t.Helper()
	f := newRegistrationFixture(t, options...)
	startedAt, deadlineAt := f.now.Add(-time.Minute), f.now.Add(14*time.Minute)
	f.instance.Status = model.InstanceRunning
	f.instance.StartedAt, f.instance.DeadlineAt = &startedAt, &deadlineAt
	candidate := repository.RetryCandidate{TaskID: f.taskID, InstanceID: f.instance.ID, AttemptNo: 7, NextDispatchAt: f.now.Add(-time.Second)}
	// Refunded BUSY attempts may advance numbering beyond the single opportunity
	// allowed by max_retries=0. Registration must use UsedAttempts for the budget.
	f.retryTask = &repository.RegisteredTask{TaskID: candidate.TaskID, InstanceID: candidate.InstanceID,
		AttemptNo: candidate.AttemptNo + 1, UsedAttempts: 1, RegisteredAt: f.now}
	return f, candidate
}
