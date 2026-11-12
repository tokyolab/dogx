package dispatch

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestDispatcherRetryPreservesCandidateAndUsesSharedResponseSaving(t *testing.T) {
	for _, outcome := range []Outcome{OutcomeAccepted, OutcomeBusy, OutcomeRejected} {
		t.Run(dispatcherOutcomeName(outcome), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				attempt := dispatcherAttempt()
				attempt.AttemptNo = 8
				candidate := repository.DispatchCandidate{TaskType: model.TaskTypeOrdinary, Status: model.TaskRetryWaiting, TaskID: attempt.TaskID, InstanceID: attempt.InstanceID,
					AttemptNo: 7, NextDispatchAt: attempt.RegisteredAt.Add(-time.Second)}
				registrar := &dispatcherRegistrarStub{retryCall: func(ctx context.Context, got repository.DispatchCandidate) (*RegisteredAttempt, error) {
					if got != candidate {
						t.Fatal("dispatcher replaced the original retry candidate")
					}
					if deadline, ok := ctx.Deadline(); !ok || deadline.Sub(time.Now()) != DefaultTotalTimeout {
						t.Fatal("retry registration did not receive the total dispatch budget")
					}
					return &attempt, nil
				}}
				sender := &dispatcherSenderStub{call: func(_ context.Context, got RegisteredAttempt) (Response, error) {
					if !reflect.DeepEqual(got, attempt) {
						t.Fatal("retry send did not use the newly committed attempt")
					}
					return dispatcherResponse(outcome), nil
				}}
				store := &dispatcherStoreStub{write: func(_ context.Context, _ dispatcherWriteCall, number int) (bool, error) {
					if number == 1 {
						return false, errors.New("synthetic response save failure")
					}
					return true, nil
				}}
				hints := 0
				d := testDispatcher(t, registrar, sender, store, WithInstanceHint(func(id int64) {
					if id != candidate.InstanceID {
						t.Fatal("retry response hinted the wrong parent")
					}
					hints++
				}))
				got, err := d.DispatchTask(context.Background(), candidate)
				if err != nil || got != (DispatchResult{Registered: true, Outcome: outcome, Applied: true}) {
					t.Fatalf("retry dispatch=%+v err=%v", got, err)
				}
				writes := store.snapshot()
				if registrar.calls.Load() != 0 || registrar.retryCalls.Load() != 1 || sender.calls.Load() != 1 || len(writes) != 2 {
					t.Fatal("retry used first-start registration, resent business or skipped response backoff")
				}
				for _, write := range writes {
					assertDispatcherWrite(t, write, outcome, attempt)
					if write != writes[0] {
						t.Fatal("retry response saving changed original evidence")
					}
				}
				wantHints := 1
				if outcome == OutcomeAccepted {
					wantHints = 0
				}
				if hints != wantHints {
					t.Fatalf("retry hints=%d want=%d", hints, wantHints)
				}
			})
		})
	}
}

func TestDispatcherRetryNeverSendsWithoutCommitConfirmation(t *testing.T) {
	attempt := dispatcherAttempt()
	candidate := repository.DispatchCandidate{TaskType: model.TaskTypeOrdinary, Status: model.TaskRetryWaiting, TaskID: attempt.TaskID, InstanceID: attempt.InstanceID, AttemptNo: 1}
	commitUnknown := errors.New("synthetic retry commit acknowledgement lost")
	for _, test := range []struct {
		name    string
		attempt *RegisteredAttempt
		err     error
	}{
		{"old candidate", nil, nil},
		{"parent conflict", nil, repository.ErrInstanceLocked},
		{"unknown commit", &attempt, commitUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			registrar := &dispatcherRegistrarStub{retryCall: func(context.Context, repository.DispatchCandidate) (*RegisteredAttempt, error) {
				return test.attempt, test.err
			}}
			sender, store := &dispatcherSenderStub{}, &dispatcherStoreStub{}
			d := testDispatcher(t, registrar, sender, store)
			if got, err := d.DispatchTask(context.Background(), candidate); got != (DispatchResult{}) || !errors.Is(err, test.err) {
				t.Fatalf("unconfirmed retry dispatch=%+v err=%v", got, err)
			}
			if sender.calls.Load() != 0 || len(store.snapshot()) != 0 {
				t.Fatal("unconfirmed retry sent business work or saved dispatch evidence")
			}
		})
	}
}

func TestDispatcherRetryRejectsInvalidAndCanceledCandidatesBeforeRegistration(t *testing.T) {
	registrar, sender, store := &dispatcherRegistrarStub{}, &dispatcherSenderStub{}, &dispatcherStoreStub{}
	d := testDispatcher(t, registrar, sender, store)
	for _, candidate := range []repository.DispatchCandidate{
		{TaskID: 0, InstanceID: 22, AttemptNo: 1}, {TaskID: 44, InstanceID: -1, AttemptNo: 1}, {TaskID: 44, InstanceID: 22, AttemptNo: 0},
	} {
		if got, err := d.DispatchTask(context.Background(), candidate); got != (DispatchResult{}) || err == nil {
			t.Fatalf("invalid retry dispatch=%+v err=%v", got, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := d.DispatchTask(ctx, repository.DispatchCandidate{TaskType: model.TaskTypeOrdinary, Status: model.TaskRetryWaiting, TaskID: 44, InstanceID: 22, AttemptNo: 1}); got != (DispatchResult{}) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled retry dispatch=%+v err=%v", got, err)
	}
	if registrar.calls.Load() != 0 || registrar.retryCalls.Load() != 0 || sender.calls.Load() != 0 || len(store.snapshot()) != 0 {
		t.Fatal("invalid or canceled retry started work")
	}
}

func TestDispatcherRetryDoesNotSendAfterCommittedRegistrationExhaustsBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempt := dispatcherAttempt()
		candidate := repository.DispatchCandidate{TaskType: model.TaskTypeOrdinary, Status: model.TaskRetryWaiting, TaskID: attempt.TaskID, InstanceID: attempt.InstanceID, AttemptNo: 1}
		registrar := &dispatcherRegistrarStub{retryCall: func(ctx context.Context, _ repository.DispatchCandidate) (*RegisteredAttempt, error) {
			<-ctx.Done()
			return &attempt, nil
		}}
		sender, store := &dispatcherSenderStub{}, &dispatcherStoreStub{}
		d := testDispatcher(t, registrar, sender, store)
		if got, err := d.DispatchTask(context.Background(), candidate); got != (DispatchResult{Registered: true}) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("late committed retry dispatch=%+v err=%v", got, err)
		}
		if sender.calls.Load() != 0 || len(store.snapshot()) != 0 {
			t.Fatal("expired retry budget sent or refunded the committed attempt")
		}
	})
}
