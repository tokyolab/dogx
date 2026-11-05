package dispatch

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestDispatcherRejectsNilDependenciesWithoutStartingWork(t *testing.T) {
	registrar := &dispatcherRegistrarStub{}
	sender := &dispatcherSenderStub{}
	store := &dispatcherStoreStub{}
	var nilRegistrar *dispatcherRegistrarStub
	var nilSender *dispatcherSenderStub
	var nilStore *dispatcherStoreStub
	for _, test := range []struct {
		name      string
		registrar ordinaryRegistrar
		sender    Sender
		store     responseStore
	}{
		{"nil registrar", nil, sender, store},
		{"typed nil registrar", nilRegistrar, sender, store},
		{"nil sender", registrar, nil, store},
		{"typed nil sender", registrar, nilSender, store},
		{"nil store", registrar, sender, nil},
		{"typed nil store", registrar, sender, nilStore},
	} {
		t.Run(test.name, func(t *testing.T) {
			if d, err := NewDispatcher(test.registrar, test.sender, test.store); err == nil || d != nil {
				t.Fatal("invalid dependency accepted")
			}
		})
	}
	if d, err := NewDispatcher(registrar, sender, store, nil); err == nil || d != nil {
		t.Fatal("nil option accepted")
	}
	if _, err := NewDispatcher(registrar, sender, store, WithInstanceHint(nil)); err != nil {
		t.Fatal(err)
	}
	if registrar.calls.Load() != 0 || sender.calls.Load() != 0 || len(store.snapshot()) != 0 {
		t.Fatal("construction started work")
	}
}

func TestDispatcherDoesNotSendWithoutConfirmedRegistration(t *testing.T) {
	commitUnknown := errors.New("synthetic registration commit acknowledgement lost")
	attempt := dispatcherAttempt()
	for _, test := range []struct {
		name    string
		attempt *RegisteredAttempt
		err     error
	}{
		{"skipped candidate", nil, nil},
		{"registration failure", nil, commitUnknown},
		{"assigned identity without commit confirmation", &attempt, commitUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			registrar := &dispatcherRegistrarStub{call: func(_ context.Context, id int64) (*RegisteredAttempt, error) {
				if id != attempt.InstanceID {
					t.Fatal("registration changed the candidate identity")
				}
				return test.attempt, test.err
			}}
			sender, store := &dispatcherSenderStub{}, &dispatcherStoreStub{}
			d := testDispatcher(t, registrar, sender, store)
			got, err := d.DispatchOrdinary(context.Background(), attempt.InstanceID)
			if !errors.Is(err, test.err) || got != (DispatchResult{}) {
				t.Fatalf("unconfirmed registration: result=%+v error=%v", got, err)
			}
			if sender.calls.Load() != 0 || len(store.snapshot()) != 0 {
				t.Fatal("unconfirmed registration sent or saved a response")
			}
		})
	}
}

func TestDispatcherRejectsInvalidOrAlreadyCanceledCallsBeforeRegistration(t *testing.T) {
	registrar, sender, store := &dispatcherRegistrarStub{}, &dispatcherSenderStub{}, &dispatcherStoreStub{}
	d := testDispatcher(t, registrar, sender, store)
	if result, err := d.DispatchOrdinary(context.Background(), 0); err == nil || result.Registered {
		t.Fatal("invalid candidate started registration")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := d.DispatchOrdinary(ctx, 22); !errors.Is(err, context.Canceled) || result.Registered {
		t.Fatalf("canceled dispatch: result=%+v error=%v", result, err)
	}
	if registrar.calls.Load() != 0 || sender.calls.Load() != 0 || len(store.snapshot()) != 0 {
		t.Fatal("invalid or canceled call started work")
	}
}

func TestDispatcherRetriesOriginalEvidenceWithIndependentSaveContext(t *testing.T) {
	for _, outcome := range []Outcome{OutcomeAccepted, OutcomeBusy, OutcomeRejected} {
		t.Run(dispatcherOutcomeName(outcome), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				attempt := dispatcherAttempt()
				var rpcCtx context.Context
				sender := &dispatcherSenderStub{call: func(ctx context.Context, got RegisteredAttempt) (Response, error) {
					if !reflect.DeepEqual(got, attempt) {
						t.Fatal("sender did not receive the original committed attempt and frozen policy")
					}
					var cancel context.CancelFunc
					rpcCtx, cancel = context.WithTimeout(ctx, 3*time.Second)
					defer cancel()
					return dispatcherResponse(outcome), nil
				}}
				failure := errors.New("synthetic response save failure")
				store := &dispatcherStoreStub{write: func(ctx context.Context, _ dispatcherWriteCall, number int) (bool, error) {
					if !errors.Is(rpcCtx.Err(), context.Canceled) || ctx.Err() != nil {
						t.Fatal("response save inherited the canceled RPC context")
					}
					deadline, ok := ctx.Deadline()
					if !ok || deadline.Sub(time.Now()) != time.Second {
						t.Fatal("response write did not receive its one-second budget")
					}
					if number < 3 {
						return false, failure
					}
					return true, nil
				}}
				var hints atomic.Int32
				d := testDispatcher(t, dispatcherCommittedRegistrar(attempt), sender, store, WithInstanceHint(func(id int64) {
					if id != attempt.InstanceID {
						t.Fatal("hint did not identify the original parent")
					}
					hints.Add(1)
				}))
				started := time.Now()
				result, err := d.DispatchOrdinary(context.Background(), attempt.InstanceID)
				if err != nil || result != (DispatchResult{Registered: true, Outcome: outcome, Applied: true}) {
					t.Fatalf("saved response: result=%+v error=%v", result, err)
				}
				calls := store.snapshot()
				if sender.calls.Load() != 1 || len(calls) != 3 || time.Since(started) != 400*time.Millisecond {
					t.Fatal("response persistence retried business, exceeded attempts or skipped backoff")
				}
				for _, call := range calls {
					assertDispatcherWrite(t, call, outcome, attempt)
					if call.binding != calls[0].binding || call.busy != calls[0].busy || call.rejection != calls[0].rejection {
						t.Fatal("response retry changed original metadata or resampled BUSY delay")
					}
				}
				wantHints := int32(1)
				if outcome == OutcomeAccepted {
					wantHints = 0
				}
				if hints.Load() != wantHints {
					t.Fatalf("hints=%d, want %d", hints.Load(), wantHints)
				}
			})
		})
	}
}

func TestDispatcherZeroRowEndsCommitUncertaintyWithoutInventingSuccess(t *testing.T) {
	for _, outcome := range []Outcome{OutcomeAccepted, OutcomeBusy, OutcomeRejected} {
		t.Run(dispatcherOutcomeName(outcome), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				attempt := dispatcherAttempt()
				sender := &dispatcherSenderStub{call: func(context.Context, RegisteredAttempt) (Response, error) {
					return dispatcherResponse(outcome), nil
				}}
				store := &dispatcherStoreStub{write: func(_ context.Context, _ dispatcherWriteCall, number int) (bool, error) {
					if number == 1 {
						return false, errors.New("synthetic commit acknowledgement lost")
					}
					return false, nil
				}}
				var hints atomic.Int32
				d := testDispatcher(t, dispatcherCommittedRegistrar(attempt), sender, store, WithInstanceHint(func(int64) { hints.Add(1) }))
				got, err := d.DispatchOrdinary(context.Background(), attempt.InstanceID)
				if err != nil || got != (DispatchResult{Registered: true, Outcome: outcome}) {
					t.Fatalf("zero-row response: result=%+v error=%v", got, err)
				}
				time.Sleep(time.Minute)
				if sender.calls.Load() != 1 || len(store.snapshot()) != 2 || hints.Load() != 0 {
					t.Fatal("zero-row persistence sent, retried, hinted or continued in the background")
				}
			})
		})
	}
}

func TestDispatcherDoesNotRetryKnownResponseWriteErrors(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		sentinel error
	}{
		{"joined invalid write", errors.Join(errors.New("synthetic write context"), repository.ErrInvalidDispatchWrite), repository.ErrInvalidDispatchWrite},
		{"wrapped invalid write", fmt.Errorf("synthetic wrapper: %w", repository.ErrInvalidDispatchWrite), repository.ErrInvalidDispatchWrite},
		{"joined rejected write", errors.Join(errors.New("synthetic write context"), repository.ErrDispatchWriteRejected), repository.ErrDispatchWriteRejected},
		{"wrapped rejected write", fmt.Errorf("synthetic wrapper: %w", repository.ErrDispatchWriteRejected), repository.ErrDispatchWriteRejected},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				attempt := dispatcherAttempt()
				sender := &dispatcherSenderStub{call: func(context.Context, RegisteredAttempt) (Response, error) {
					return dispatcherResponse(OutcomeRejected), nil
				}}
				store := &dispatcherStoreStub{write: func(context.Context, dispatcherWriteCall, int) (bool, error) {
					return false, test.err
				}}
				var hints atomic.Int32
				d := testDispatcher(t, dispatcherCommittedRegistrar(attempt), sender, store,
					WithInstanceHint(func(int64) { hints.Add(1) }))
				got, err := d.DispatchOrdinary(context.Background(), attempt.InstanceID)
				if !errors.Is(err, test.err) || !errors.Is(err, test.sentinel) ||
					got != (DispatchResult{Registered: true, Outcome: OutcomeRejected}) {
					t.Fatalf("permanent write failure: result=%+v error=%v", got, err)
				}
				if len(store.snapshot()) != 1 || sender.calls.Load() != 1 || hints.Load() != 0 {
					t.Fatal("known response write error retried, resent business or announced a saved result")
				}
			})
		})
	}
}

func TestDispatcherSenderErrorsOverrideAnyAccompanyingEvidence(t *testing.T) {
	failure := errors.New("synthetic transport result unknown")
	for _, outcome := range []Outcome{OutcomeAccepted, OutcomeBusy, OutcomeRejected} {
		t.Run(dispatcherOutcomeName(outcome), func(t *testing.T) {
			attempt := dispatcherAttempt()
			sender := &dispatcherSenderStub{call: func(context.Context, RegisteredAttempt) (Response, error) {
				return dispatcherResponse(outcome), failure
			}}
			store := &dispatcherStoreStub{}
			d := testDispatcher(t, dispatcherCommittedRegistrar(attempt), sender, store)
			got, err := d.DispatchOrdinary(context.Background(), attempt.InstanceID)
			if !errors.Is(err, failure) || got != (DispatchResult{Registered: true}) {
				t.Fatalf("uncertain response: result=%+v error=%v", got, err)
			}
			if sender.calls.Load() != 1 || len(store.snapshot()) != 0 {
				t.Fatal("transport failure became binding, refund, rejection or another send")
			}
		})
	}
}

func TestDispatcherUnknownOrInvalidOutcomeDoesNotWrite(t *testing.T) {
	for _, outcome := range []Outcome{OutcomeUnknown, Outcome(255)} {
		attempt := dispatcherAttempt()
		sender := &dispatcherSenderStub{call: func(context.Context, RegisteredAttempt) (Response, error) {
			return Response{Outcome: outcome}, nil
		}}
		store := &dispatcherStoreStub{}
		d := testDispatcher(t, dispatcherCommittedRegistrar(attempt), sender, store)
		got, err := d.DispatchOrdinary(context.Background(), attempt.InstanceID)
		if got != (DispatchResult{Registered: true}) || (err == nil) != (outcome == OutcomeUnknown) {
			t.Fatalf("unknown outcome: result=%+v error=%v", got, err)
		}
		if sender.calls.Load() != 1 || len(store.snapshot()) != 0 {
			t.Fatal("unknown outcome caused persistence or another send")
		}
	}
}

func TestDispatcherExhaustedRegistrationBudgetDoesNotSendCommittedAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempt := dispatcherAttempt()
		registrar := &dispatcherRegistrarStub{call: func(ctx context.Context, _ int64) (*RegisteredAttempt, error) {
			deadline, ok := ctx.Deadline()
			if !ok || deadline.Sub(time.Now()) != 10*time.Second {
				t.Fatal("registration did not receive the remaining total dispatch budget")
			}
			// A dependency may return after cancellation; commit evidence cannot
			// grant a fresh sending budget or imply that an earlier commit rolled back.
			<-ctx.Done()
			return &attempt, nil
		}}
		sender, store := &dispatcherSenderStub{}, &dispatcherStoreStub{}
		d := testDispatcher(t, registrar, sender, store)
		got, err := d.DispatchOrdinary(context.Background(), attempt.InstanceID)
		if !errors.Is(err, context.DeadlineExceeded) || got != (DispatchResult{Registered: true}) {
			t.Fatalf("late registration: result=%+v error=%v", got, err)
		}
		if sender.calls.Load() != 0 || len(store.snapshot()) != 0 {
			t.Fatal("budget exhaustion sent or refunded a committed attempt")
		}
	})
}

func TestDispatcherWriteTimeoutsUseFiniteRetriesWithoutAnotherSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempt := dispatcherAttempt()
		sender := &dispatcherSenderStub{call: func(context.Context, RegisteredAttempt) (Response, error) {
			return dispatcherResponse(OutcomeAccepted), nil
		}}
		store := &dispatcherStoreStub{write: func(ctx context.Context, _ dispatcherWriteCall, _ int) (bool, error) {
			<-ctx.Done()
			return false, ctx.Err()
		}}
		d := testDispatcher(t, dispatcherCommittedRegistrar(attempt), sender, store)
		started := time.Now()
		got, err := d.DispatchOrdinary(context.Background(), attempt.InstanceID)
		if !errors.Is(err, context.DeadlineExceeded) || got != (DispatchResult{Registered: true, Outcome: OutcomeAccepted}) {
			t.Fatalf("save timeouts: result=%+v error=%v", got, err)
		}
		if time.Since(started) != 3400*time.Millisecond || len(store.snapshot()) != 3 || sender.calls.Load() != 1 {
			t.Fatal("write timeout bypassed finite attempts, backoff or business-send boundaries")
		}
	})
}

func TestDispatcherClientPreparationUsesTotalBudgetBeforeRPCBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempt := dispatcherAttempt()
		sender := &dispatcherSenderStub{call: func(ctx context.Context, _ RegisteredAttempt) (Response, error) {
			// Client preparation belongs to the total operation, so it must not
			// consume an RPC budget which starts only when the call is issued.
			time.Sleep(4 * time.Second)
			if ctx.Err() != nil {
				t.Fatal("dispatcher applied the RPC timeout to client preparation")
			}
			rpcCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			deadline, ok := rpcCtx.Deadline()
			if !ok || deadline.Sub(time.Now()) != 3*time.Second {
				t.Fatal("client preparation consumed the subsequent RPC timeout")
			}
			time.Sleep(100 * time.Millisecond)
			return dispatcherResponse(OutcomeAccepted), nil
		}}
		store := &dispatcherStoreStub{}
		d := testDispatcher(t, dispatcherCommittedRegistrar(attempt), sender, store)
		got, err := d.DispatchOrdinary(context.Background(), attempt.InstanceID)
		if err != nil || got != (DispatchResult{Registered: true, Outcome: OutcomeAccepted, Applied: true}) {
			t.Fatalf("prepared dispatch: result=%+v error=%v", got, err)
		}
		if sender.calls.Load() != 1 || len(store.snapshot()) != 1 {
			t.Fatal("client preparation caused repeated work")
		}
	})
}

func TestDispatcherSavingUsesRemainingHostBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempt := dispatcherAttempt()
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		sender := &dispatcherSenderStub{call: func(context.Context, RegisteredAttempt) (Response, error) {
			time.Sleep(time.Second)
			return dispatcherResponse(OutcomeBusy), nil
		}}
		store := &dispatcherStoreStub{write: func(ctx context.Context, _ dispatcherWriteCall, _ int) (bool, error) {
			deadline, ok := ctx.Deadline()
			if !ok || deadline.Sub(time.Now()) != 500*time.Millisecond {
				t.Fatal("saving received a fresh budget beyond the remaining host deadline")
			}
			<-ctx.Done()
			return false, ctx.Err()
		}}
		var hints atomic.Int32
		d := testDispatcher(t, dispatcherCommittedRegistrar(attempt), sender, store, WithInstanceHint(func(int64) { hints.Add(1) }))
		started := time.Now()
		got, err := d.DispatchOrdinary(ctx, attempt.InstanceID)
		if !errors.Is(err, context.DeadlineExceeded) || got != (DispatchResult{Registered: true, Outcome: OutcomeBusy}) {
			t.Fatalf("host deadline: result=%+v error=%v", got, err)
		}
		if time.Since(started) != 1500*time.Millisecond || len(store.snapshot()) != 1 || sender.calls.Load() != 1 || hints.Load() != 0 {
			t.Fatal("expired host budget started another operation or announced a save")
		}
	})
}

func TestDispatcherSavePhaseExpiresWithoutHidingLateDependencyReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempt := dispatcherAttempt()
		sender := &dispatcherSenderStub{call: func(context.Context, RegisteredAttempt) (Response, error) {
			return dispatcherResponse(OutcomeRejected), nil
		}}
		failure := errors.New("synthetic late database return")
		store := &dispatcherStoreStub{write: func(context.Context, dispatcherWriteCall, int) (bool, error) {
			// Cancellation is not actual completion. The caller must retain its
			// processing position until this deliberately uncooperative call exits.
			time.Sleep(4100 * time.Millisecond)
			return false, failure
		}}
		d := testDispatcher(t, dispatcherCommittedRegistrar(attempt), sender, store)
		started := time.Now()
		got, err := d.DispatchOrdinary(context.Background(), attempt.InstanceID)
		if !errors.Is(err, failure) || !errors.Is(err, context.DeadlineExceeded) || got.Applied {
			t.Fatalf("expired save phase: result=%+v error=%v", got, err)
		}
		if time.Since(started) != 4100*time.Millisecond || len(store.snapshot()) != 1 || sender.calls.Load() != 1 {
			t.Fatal("save phase returned before actual completion or continued after expiry")
		}
	})
}

func TestDispatcherCancellationWaitsForStartedWriteToActuallyExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempt := dispatcherAttempt()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		release := make(chan struct{})
		releaseOnce := sync.OnceFunc(func() { close(release) })
		defer releaseOnce()
		sender := &dispatcherSenderStub{call: func(context.Context, RegisteredAttempt) (Response, error) {
			return dispatcherResponse(OutcomeAccepted), nil
		}}
		store := &dispatcherStoreStub{write: func(ctx context.Context, _ dispatcherWriteCall, _ int) (bool, error) {
			<-release
			return false, ctx.Err()
		}}
		d := testDispatcher(t, dispatcherCommittedRegistrar(attempt), sender, store)
		done := make(chan error, 1)
		go func() { _, err := d.DispatchOrdinary(ctx, attempt.InstanceID); done <- err }()
		synctest.Wait()
		if len(store.snapshot()) != 1 {
			t.Fatal("response write did not start")
		}
		cancel()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("dispatch returned before the database operation exited: %v", err)
		default:
		}
		releaseOnce()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled dispatch returned %v", err)
		}
		if len(store.snapshot()) != 1 || sender.calls.Load() != 1 {
			t.Fatal("cancellation triggered another save or business send")
		}
	})
}

func testDispatcher(t *testing.T, registrar ordinaryRegistrar, sender Sender, store responseStore, options ...Option) *Dispatcher {
	t.Helper()
	d, err := NewDispatcher(registrar, sender, store, options...)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func dispatcherAttempt() RegisteredAttempt {
	now := time.Now()
	return RegisteredAttempt{
		JobID: 11, InstanceID: 22, ExecutorGroupID: 33, TaskID: 44, AttemptNo: 1,
		RegisteredAt: now, DeadlineAt: now.Add(15 * time.Minute),
		ExecutionConfig: model.ExecutionConfig{
			SchemaVersion: 1, ExecutionMode: model.ExecutionModeNormal, ProcessorName: "test.processor",
			Params: "frozen parameters", MaxRetries: 3, RetryIntervalSeconds: 10, TimeoutSeconds: 900,
		},
	}
}

func dispatcherResponse(outcome Outcome) Response {
	return Response{Outcome: outcome, ExecutorInstanceID: uuid.MustParse("00000000-0000-4000-8000-000000000002"),
		ExecutorAddress: "127.0.0.1:12345", Summary: "executor rejected dispatch"}
}

func dispatcherOutcomeName(outcome Outcome) string {
	switch outcome {
	case OutcomeAccepted:
		return "accepted"
	case OutcomeBusy:
		return "busy"
	default:
		return "rejected"
	}
}

func assertDispatcherWrite(t *testing.T, call dispatcherWriteCall, outcome Outcome, attempt RegisteredAttempt) {
	t.Helper()
	if call.outcome != outcome {
		t.Fatal("response used the wrong persistence operation")
	}
	switch outcome {
	case OutcomeAccepted:
		response := dispatcherResponse(outcome)
		want := repository.ExecutorBinding{TaskID: attempt.TaskID, InstanceID: attempt.InstanceID, AttemptNo: attempt.AttemptNo,
			ExecutorInstanceID: response.ExecutorInstanceID, ExecutorAddress: response.ExecutorAddress}
		if call.binding != want {
			t.Fatal("binding changed the original request, ACK identity or Peer address")
		}
	case OutcomeBusy:
		got := call.busy
		if got.TaskID != attempt.TaskID || got.InstanceID != attempt.InstanceID || got.AttemptNo != attempt.AttemptNo ||
			got.Delay < 4*time.Second || got.Delay > 6*time.Second {
			t.Fatal("BUSY changed the original attempt or its allowed delay range")
		}
	case OutcomeRejected:
		want := repository.DispatchRejectionWrite{TaskID: attempt.TaskID, InstanceID: attempt.InstanceID, AttemptNo: attempt.AttemptNo,
			MaxRetries: attempt.ExecutionConfig.MaxRetries, RetryIntervalSeconds: attempt.ExecutionConfig.RetryIntervalSeconds,
			Summary: dispatcherResponse(outcome).Summary}
		if call.rejection != want {
			t.Fatal("rejection changed the original identity, frozen policy or safe diagnosis")
		}
	}
}

type dispatcherRegistrarStub struct {
	calls      atomic.Int32
	call       func(context.Context, int64) (*RegisteredAttempt, error)
	retryCalls atomic.Int32
	retryCall  func(context.Context, repository.RetryCandidate) (*RegisteredAttempt, error)
}

func (s *dispatcherRegistrarStub) RetryOrdinary(ctx context.Context, candidate repository.RetryCandidate) (*RegisteredAttempt, error) {
	s.retryCalls.Add(1)
	if s.retryCall == nil {
		return nil, nil
	}
	return s.retryCall(ctx, candidate)
}

func (s *dispatcherRegistrarStub) StartOrdinary(ctx context.Context, id int64) (*RegisteredAttempt, error) {
	s.calls.Add(1)
	if s.call == nil {
		return nil, nil
	}
	return s.call(ctx, id)
}

func dispatcherCommittedRegistrar(attempt RegisteredAttempt) *dispatcherRegistrarStub {
	return &dispatcherRegistrarStub{call: func(context.Context, int64) (*RegisteredAttempt, error) { return &attempt, nil }}
}

type dispatcherSenderStub struct {
	calls atomic.Int32
	call  func(context.Context, RegisteredAttempt) (Response, error)
}

func (s *dispatcherSenderStub) Dispatch(ctx context.Context, attempt RegisteredAttempt) (Response, error) {
	s.calls.Add(1)
	if s.call == nil {
		return Response{}, nil
	}
	return s.call(ctx, attempt)
}

type dispatcherWriteCall struct {
	outcome   Outcome
	binding   repository.ExecutorBinding
	busy      repository.DispatchBusyWrite
	rejection repository.DispatchRejectionWrite
}

// dispatcherStoreStub records only response writes. It cannot query a newer
// attempt or resend business work, keeping those forbidden paths observable.
type dispatcherStoreStub struct {
	mu    sync.Mutex
	calls []dispatcherWriteCall
	write func(context.Context, dispatcherWriteCall, int) (bool, error)
}

func (s *dispatcherStoreStub) BindExecutor(ctx context.Context, value repository.ExecutorBinding) (bool, error) {
	return s.save(ctx, dispatcherWriteCall{outcome: OutcomeAccepted, binding: value})
}

func (s *dispatcherStoreStub) SaveDispatchBusy(ctx context.Context, value repository.DispatchBusyWrite) (bool, error) {
	return s.save(ctx, dispatcherWriteCall{outcome: OutcomeBusy, busy: value})
}

func (s *dispatcherStoreStub) SaveDispatchRejection(ctx context.Context, value repository.DispatchRejectionWrite) (bool, error) {
	return s.save(ctx, dispatcherWriteCall{outcome: OutcomeRejected, rejection: value})
}

func (s *dispatcherStoreStub) save(ctx context.Context, call dispatcherWriteCall) (bool, error) {
	s.mu.Lock()
	s.calls = append(s.calls, call)
	number := len(s.calls)
	s.mu.Unlock()
	if s.write == nil {
		return true, nil
	}
	return s.write(ctx, call, number)
}

func (s *dispatcherStoreStub) snapshot() []dispatcherWriteCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]dispatcherWriteCall(nil), s.calls...)
}
