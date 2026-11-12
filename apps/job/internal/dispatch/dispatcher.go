package dispatch

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

const (
	// DefaultTotalTimeout bounds registration, target preparation, admission and
	// response persistence. It does not bound accepted business execution.
	DefaultTotalTimeout = 10 * time.Second
	// DefaultBusyDelay is the base delay before retry eligibility after an
	// explicit refusal for lack of executor capacity. Each response adds jitter.
	DefaultBusyDelay      = 5 * time.Second
	responseTimeout       = 4 * time.Second
	responseWriteTimeout  = time.Second
	responseRetryInterval = 200 * time.Millisecond
	responseWriteAttempts = 3
)

// Outcome identifies admission evidence, not business completion. Unknown does
// not prove rejection and never authorizes a refund or another send.
type Outcome uint8

const (
	OutcomeUnknown Outcome = iota
	OutcomeAccepted
	OutcomeBusy
	OutcomeRejected
)

// Response contains evidence from the one dispatch RPC. Accepted requires both
// a validated UUIDv4 from its ACK and the direct address from that call's Peer.
// Busy and Rejected prove that admission did not occur. Summary is a fixed,
// credential-free platform diagnosis, never a remote message or request payload.
type Response struct {
	Outcome            Outcome
	ExecutorInstanceID uuid.UUID
	ExecutorAddress    string
	Summary            string
}

// Sender sends a newly committed attempt exactly once, borrowing its host's
// reusable connection. It does not retry, persist, cancel business work or own
// connection shutdown. Target preparation uses the caller's remaining budget;
// the RPC itself must use a child Context bounded to the deployment's admission
// timeout (three seconds by default) and cancel
// that child before returning. Neither Context controls accepted business work.
// An error leaves admission uncertain even if accompanied
// by a nonzero Response; callers must not interpret it as a definite rejection.
type Sender interface {
	Dispatch(context.Context, RegisteredAttempt) (Response, error)
}

// attemptRegistrar grants permission to send only by returning a newly created,
// confirmed-committed attempt. Nil, nil skips a candidate; every error grants no
// permission, including a lost commit confirmation. It must bound registration
// to two seconds and the caller's remaining deadline, without sending an RPC.
// NOWAIT conflicts preserve repository.ErrInstanceLocked after rollback so the
// host can skip the rest of that parent's current candidate group without an
// immediate retry.
type attemptRegistrar interface {
	// StartInstance starts a waiting parent and creates its ordinary or prepare Task.
	StartInstance(context.Context, int64) (*RegisteredAttempt, error)
	// RegisterTask retains the candidate's original attempt identity and only
	// authorizes the new version returned by the committed conditional update.
	RegisterTask(context.Context, repository.DispatchCandidate) (*RegisteredAttempt, error)
}

// responseStore conditionally writes original dispatch evidence. It must use
// independently committing handles: true, nil confirms one committed change;
// false, nil ends processing without proving an earlier change committed.
// Errors preserve causes but do not establish rollback. Implementations must
// honor repository identity/state predicates and never resend or query to infer
// why a write matched no rows. The host owns the store's database lifetime.
type responseStore interface {
	BindExecutor(context.Context, repository.ExecutorBinding) (bool, error)
	SaveDispatchBusy(context.Context, repository.DispatchBusyWrite) (bool, error)
	SaveDispatchRejection(context.Context, repository.DispatchRejectionWrite) (bool, error)
}

// DispatchResult reports this call's evidence only. Registered means a newly
// committed attempt was obtained, not that an RPC necessarily reached an executor.
// Applied is true only after this call confirms its response write. An accepted
// Outcome is not business success, and a zero-row write leaves Applied false.
type DispatchResult struct {
	Registered bool
	Outcome    Outcome
	Applied    bool
}

// Dispatcher performs a first or retry dispatch of an Instance's Tasks. It owns no
// queue, worker, connection or background retry; the host must acquire dispatch
// capacity before calling it and retain capacity until the call actually returns.
// Dependencies and the optional hint must support concurrent calls. Same-Instance
// registration coalescing and candidate supply remain the host's responsibility.
type Dispatcher struct {
	registrar    attemptRegistrar
	sender       Sender
	tasks        responseStore
	hint         func(int64)
	totalTimeout time.Duration
	busyDelay    time.Duration
}

// Option configures a Dispatcher before it is used concurrently.
type Option func(*Dispatcher)

// WithDispatchTiming changes the total processing budget and the base BUSY
// delay. Both must be positive; busyDelay must leave room for up to 20% jitter.
// Each actual RPC is independently bounded by Sender, within the total budget.
func WithDispatchTiming(totalTimeout, busyDelay time.Duration) Option {
	return func(d *Dispatcher) { d.totalTimeout, d.busyDelay = totalTimeout, busyDelay }
}

// WithInstanceHint supplies a concurrent, nonblocking local parent-check hint.
// It runs only after a confirmed BUSY or rejection write. It is not permission
// to redispatch, and must inspect current facts rather than the old response.
// Nil disables hints; recovery remains responsible for missed notifications.
func WithInstanceHint(hint func(int64)) Option { return func(d *Dispatcher) { d.hint = hint } }

// NewDispatcher binds dependencies without starting work or taking ownership.
// The task store must not be bound to an outer transaction: response persistence
// happens after registration commits and after all network I/O has returned.
func NewDispatcher(registrar attemptRegistrar, sender Sender, tasks responseStore, options ...Option) (*Dispatcher, error) {
	if nilDependency(registrar) || nilDependency(sender) || nilDependency(tasks) {
		return nil, errors.New("dispatch dependencies must not be nil")
	}
	d := &Dispatcher{registrar: registrar, sender: sender, tasks: tasks, totalTimeout: DefaultTotalTimeout, busyDelay: DefaultBusyDelay}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("dispatch option is nil")
		}
		option(d)
	}
	if d.totalTimeout <= 0 || d.busyDelay <= 0 || d.busyDelay > time.Duration((1<<63-1)/6*5) {
		return nil, errors.New("dispatch total timeout and BUSY delay must be positive and representable with jitter")
	}
	return d, nil
}

// DispatchInstance registers and sends at most one new ordinary or prepare attempt. Its
// total budget (ten seconds by default) begins at entry and is clipped by the caller's deadline.
// Only confirmed registration permits sending; an exhausted budget, uncertain
// send or failed save never causes another RPC, refund or background continuation.
//
// Valid ACK, BUSY and definite rejection share a four-second save phase, with
// up to three one-second writes separated by 200 milliseconds. Every retry uses
// the same evidence. A successful or zero-row write stops immediately. Context
// cancellation stops new operations, but this call waits for started dependencies
// to actually return; it is not a hard execution or rollback guarantee.
//
// Errors retain cause chains that may contain driver or remote diagnostics.
// Callers must log safe classifications and IDs, not raw errors or responses.
// Inspect the returned facts even on error; an error cannot undo registration
// or prove that a response write failed to commit.
func (d *Dispatcher) DispatchInstance(ctx context.Context, instanceID int64) (DispatchResult, error) {
	if instanceID <= 0 {
		return DispatchResult{}, errors.New("dispatch instance ID must be positive")
	}
	return d.dispatch(ctx, func(ctx context.Context) (*RegisteredAttempt, error) {
		return d.registrar.StartInstance(ctx, instanceID)
	})
}

// DispatchTask registers and sends at most one new attempt for the candidate's
// original Task, type, state, Instance and attempt number. Attempt zero is valid
// only for a first pending shard or Post. It shares DispatchInstance's total
// budget, single-send and response-save rules. A candidate is not permission to
// resend its old attempt; no registration result or any registration error stops
// processing. Lock conflicts retain repository.ErrInstanceLocked for the host to
// skip that parent's remaining candidate group without an immediate retry.
func (d *Dispatcher) DispatchTask(ctx context.Context, candidate repository.DispatchCandidate) (DispatchResult, error) {
	if !validDispatchCandidate(candidate) {
		return DispatchResult{}, errors.New("dispatch candidate identity or state is invalid")
	}
	return d.dispatch(ctx, func(ctx context.Context) (*RegisteredAttempt, error) {
		return d.registrar.RegisterTask(ctx, candidate)
	})
}

func (d *Dispatcher) dispatch(ctx context.Context, register func(context.Context) (*RegisteredAttempt, error)) (DispatchResult, error) {
	var result DispatchResult
	ctx, cancel := context.WithTimeout(ctx, d.totalTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	attempt, err := register(ctx)
	if err != nil {
		return result, fmt.Errorf("register dispatch: %w", err)
	}
	if attempt == nil {
		return result, nil
	}
	result.Registered = true
	if err := ctx.Err(); err != nil {
		return result, err
	}
	response, err := d.sender.Dispatch(ctx, *attempt)
	if err != nil {
		return result, fmt.Errorf("send dispatch: %w", err)
	}
	result.Outcome = response.Outcome
	var write func(context.Context) (bool, error)
	switch response.Outcome {
	case OutcomeAccepted:
		binding := repository.ExecutorBinding{TaskID: attempt.TaskID, InstanceID: attempt.InstanceID,
			AttemptNo: attempt.AttemptNo, ExecutorInstanceID: response.ExecutorInstanceID, ExecutorAddress: response.ExecutorAddress}
		write = func(ctx context.Context) (bool, error) { return d.tasks.BindExecutor(ctx, binding) }
	case OutcomeBusy:
		// Choose once, not on each save retry. Duplicate delivery must not move
		// an already-saved retry time or refund another execution opportunity.
		jitter := d.busyDelay / 5
		busy := repository.DispatchBusyWrite{TaskID: attempt.TaskID, InstanceID: attempt.InstanceID,
			AttemptNo: attempt.AttemptNo, Delay: d.busyDelay - jitter + time.Duration(rand.Int64N(int64(2*jitter)+1))}
		write = func(ctx context.Context) (bool, error) { return d.tasks.SaveDispatchBusy(ctx, busy) }
	case OutcomeRejected:
		rejection := repository.DispatchRejectionWrite{TaskID: attempt.TaskID, InstanceID: attempt.InstanceID,
			AttemptNo: attempt.AttemptNo, MaxRetries: attempt.ExecutionConfig.MaxRetries,
			RetryIntervalSeconds: attempt.ExecutionConfig.RetryIntervalSeconds, Summary: response.Summary}
		write = func(ctx context.Context) (bool, error) { return d.tasks.SaveDispatchRejection(ctx, rejection) }
	case OutcomeUnknown:
		return result, nil
	default:
		result.Outcome = OutcomeUnknown
		return result, errors.New("invalid dispatch outcome")
	}
	// Sender's RPC child is already canceled. Persistence inherits the original total
	// and host budgets, not the completed admission call's Context.
	result.Applied, err = saveResponse(ctx, write)
	if err != nil {
		return result, fmt.Errorf("save dispatch response: %w", err)
	}
	if result.Applied && response.Outcome != OutcomeAccepted && d.hint != nil {
		d.hint(attempt.InstanceID)
	}
	return result, nil
}

func saveResponse(ctx context.Context, write func(context.Context) (bool, error)) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, responseTimeout)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt < responseWriteAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return false, errors.Join(lastErr, err)
		}
		writeCtx, cancelWrite := context.WithTimeout(ctx, responseWriteTimeout)
		applied, err := write(writeCtx)
		cancelWrite()
		if err == nil {
			return applied, nil
		}
		if errors.Is(err, repository.ErrInvalidDispatchWrite) || errors.Is(err, repository.ErrDispatchWriteRejected) {
			return false, err
		}
		lastErr = err
		if attempt+1 == responseWriteAttempts {
			break
		}
		timer := time.NewTimer(responseRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, errors.Join(lastErr, ctx.Err())
		case <-timer.C:
		}
	}
	return false, lastErr
}

func nilDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
