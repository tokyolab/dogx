// Package instancecheck advances an Instance from durable execution facts after
// result persistence. Local hints accelerate checks; they do not own a round or
// replace database coordination between scheduler nodes.
package instancecheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/zeromicro/go-zero/core/logx"
)

// Outcome distinguishes incomplete facts from an unapplied transition. Only
// OutcomeTerminal permits discarding hints received during the current check.
// No outcome authorizes another business attempt or proves an RPC was canceled.
type Outcome uint8

const (
	// OutcomeSkipped covers a missing or waiting parent and modes whose stage
	// progression is not implemented by this checker.
	OutcomeSkipped Outcome = iota
	// OutcomeNotReady means the read succeeded but ordinary final facts are absent.
	// It is not a database error or evidence that an automatic timeout committed.
	OutcomeNotReady
	// OutcomeStopping observes or newly commits a stop decision and hands it to
	// configured stop processing; it does not confirm child cancellation.
	OutcomeStopping
	// OutcomeNotApplied means the conditional completion changed no row. It does
	// not prove a terminal parent and must not fall through to automatic timeout.
	OutcomeNotApplied
	// OutcomeTerminal confirms a previously observed terminal parent or a new
	// normal completion whose independent commit was confirmed.
	OutcomeTerminal
)

var (
	// ErrInvalidFacts identifies unknown or internally inconsistent persisted
	// states. Callers must not treat it as incomplete results or repair the Task.
	ErrInvalidFacts = errors.New("invalid instance completion facts")
	// ErrInvalidConfig identifies an unusable required frozen execution setting.
	// The current Job configuration must never substitute for this snapshot.
	ErrInvalidConfig = errors.New("invalid instance completion configuration")
)

// completionStore reads parent and ordinary Task facts in one statement snapshot
// without explicit locks. A missing parent returns nil, nil. Complete requires
// stable final Task facts and an independent database handle: true, nil confirms
// one committed parent transition; false, nil confirms only that no row changed.
// Errors retain causes and do not establish rollback or terminal state.
// DatabaseTime reads fresh database time separately after incomplete facts; it
// does not reuse the original read's snapshot or the caller's local clock.
type completionStore interface {
	FindCompletion(context.Context, int64) (*repository.InstanceCompletion, error)
	Complete(context.Context, int64, model.InstanceStatus, *string) (bool, error)
	DatabaseTime(context.Context) (time.Time, error)
}

// timeoutRequester rechecks parent state and fresh database time under the single
// parent lock. New is returned only after a timeout STOPPING commit is confirmed;
// errors never authorize notification, including uncertain commit outcomes.
type timeoutRequester interface {
	RequestTimeout(context.Context, int64) (instancestop.Decision, error)
}

// Checker performs one finite completion and optional timeout check. It owns no
// connection, worker or retry loop. Dependencies must permit concurrent calls. The caller
// retains the processing slot until Check actually returns, even after cancellation.
type Checker struct {
	instances completionStore
	timeout   timeoutRequester
	afterStop func(instancestop.Decision)
	resume    func(int64)
}

// Option configures a Checker before concurrent use begins.
type Option func(*Checker) error

// WithStopping enables timeout decisions after explicit ordinary NotReady facts.
// afterDecision hands a newly committed stop to independent database cancellation
// and best-effort notification. resume only resumes database cancellation for an
// already-stopping parent and must not send another remote notification. Both
// callbacks must be concurrency-safe, nonblocking admission hints without I/O;
// they retain no check Context and their rejection cannot undo committed state.
func WithStopping(requester timeoutRequester, afterDecision func(instancestop.Decision), resume func(int64)) Option {
	return func(c *Checker) error {
		if nilCheckDependency(requester) || afterDecision == nil || resume == nil {
			return errors.New("instance stopping dependencies must not be nil")
		}
		c.timeout, c.afterStop, c.resume = requester, afterDecision, resume
		return nil
	}
}

// NewChecker binds the completion repository without starting background work.
// Without WithStopping it only checks normal completion; no timeout or stop
// handoff is attempted. The host owns all supplied dependencies and callbacks.
func NewChecker(instances completionStore, options ...Option) (*Checker, error) {
	if nilCheckDependency(instances) {
		return nil, errors.New("instance completion repository is nil")
	}
	c := &Checker{instances: instances}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("instance check option must not be nil")
		}
		if err := option(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Check interprets parent state before decoding the settings still needed for
// completion. An ordinary final Task permits a conditional parent update even
// after the round deadline; missing or canceled Tasks diagnose an anomaly but
// never cause Task recreation. Only explicit ordinary NotReady facts may enter
// the optional timeout path, after checking a valid deadline against fresh database
// time. That path rechecks parent state and time under its own short transaction.
// Waiting and unsupported sharded progression never become timeout candidates.
// Existing STOPPING parents only resume database cancellation when configured.
//
// A zero-row update is OutcomeNotApplied, not OutcomeNotReady or terminal. Read,
// write and uncertain-commit failures return an error with no terminal claim and
// no immediate retry. Error causes may contain storage diagnostics; hosts log
// safe classifications and IDs rather than raw errors or frozen configuration.
func (c *Checker) Check(ctx context.Context, id int64) (Outcome, error) {
	if id <= 0 {
		return OutcomeSkipped, errors.New("instance check ID must be positive")
	}
	if err := ctx.Err(); err != nil {
		return OutcomeSkipped, err
	}
	facts, err := c.instances.FindCompletion(ctx, id)
	if err != nil {
		return OutcomeSkipped, fmt.Errorf("read instance completion: %w", err)
	}
	if facts == nil {
		return OutcomeSkipped, nil
	}
	if facts.ID != id {
		return OutcomeSkipped, ErrInvalidFacts
	}
	switch facts.Status {
	case model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled:
		return OutcomeTerminal, nil
	case model.InstanceWaiting:
		return OutcomeSkipped, nil
	case model.InstanceStopping:
		c.resumeStopping(id)
		return OutcomeStopping, nil
	case model.InstanceRunning:
	default:
		return OutcomeSkipped, ErrInvalidFacts
	}
	// Only these settings participate in this decision. Processor parameters,
	// retry settings and current Job edits cannot change final execution facts.
	var config struct {
		SchemaVersion *int32               `json:"schema_version"`
		ExecutionMode *model.ExecutionMode `json:"execution_mode"`
		PostEnabled   *bool                `json:"post_enabled"`
	}
	if err := json.Unmarshal(facts.ExecutionConfig, &config); err != nil {
		return OutcomeSkipped, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	if config.SchemaVersion == nil || *config.SchemaVersion != 1 || config.ExecutionMode == nil {
		return OutcomeSkipped, ErrInvalidConfig
	}
	switch *config.ExecutionMode {
	case model.ExecutionModeSharded:
		return OutcomeSkipped, nil
	case model.ExecutionModeNormal:
		if config.PostEnabled == nil || *config.PostEnabled {
			return OutcomeSkipped, ErrInvalidConfig
		}
	default:
		return OutcomeSkipped, ErrInvalidConfig
	}
	if facts.TaskID == nil {
		logx.Errorf("running ordinary instance has no task: instance_id=%d", id)
		return c.notReady(ctx, facts)
	}
	if *facts.TaskID <= 0 || facts.TaskStatus == nil {
		return OutcomeSkipped, ErrInvalidFacts
	}
	var target model.InstanceStatus
	switch *facts.TaskStatus {
	case model.TaskWaiting, model.TaskRunning, model.TaskRetryWaiting:
		return c.notReady(ctx, facts)
	case model.TaskCanceled:
		logx.Errorf("running ordinary instance has a canceled task: instance_id=%d task_id=%d", id, *facts.TaskID)
		return c.notReady(ctx, facts)
	case model.TaskSucceeded:
		target = model.InstanceSucceeded
	case model.TaskFailed:
		target = model.InstanceFailed
	default:
		return OutcomeSkipped, ErrInvalidFacts
	}
	changed, err := c.instances.Complete(ctx, id, target, facts.ResultSummary)
	if err != nil {
		return OutcomeSkipped, fmt.Errorf("complete instance: %w", err)
	}
	if !changed {
		return OutcomeNotApplied, nil
	}
	return OutcomeTerminal, nil
}

func (c *Checker) notReady(ctx context.Context, facts *repository.InstanceCompletion) (Outcome, error) {
	if c.timeout == nil {
		return OutcomeNotReady, nil
	}
	if facts.DeadlineAt == nil || facts.DeadlineAt.IsZero() {
		return OutcomeSkipped, ErrInvalidFacts
	}
	if err := ctx.Err(); err != nil {
		return OutcomeSkipped, err
	}
	now, err := c.instances.DatabaseTime(ctx)
	if err != nil {
		return OutcomeSkipped, fmt.Errorf("read instance timeout time: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return OutcomeSkipped, err
	}
	if now.IsZero() {
		return OutcomeSkipped, ErrInvalidFacts
	}
	if now.Before(*facts.DeadlineAt) {
		return OutcomeNotReady, nil
	}
	decision, err := c.timeout.RequestTimeout(ctx, facts.ID)
	if err != nil {
		return OutcomeSkipped, fmt.Errorf("request instance timeout: %w", err)
	}
	if decision.InstanceID != facts.ID || decision.New && (decision.Status != model.InstanceStopping ||
		decision.ExecutorGroupID <= 0 || decision.EndReason == nil || *decision.EndReason != model.EndReasonInstanceTimeout) {
		return OutcomeSkipped, ErrInvalidFacts
	}
	switch decision.Status {
	case model.InstanceStopping:
		if decision.New {
			c.handoff(facts.ID, func() { c.afterStop(decision) })
		} else {
			c.resumeStopping(facts.ID)
		}
		return OutcomeStopping, nil
	case model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled:
		return OutcomeTerminal, nil
	case model.InstanceWaiting:
		return OutcomeSkipped, nil
	case model.InstanceRunning:
		return OutcomeNotReady, nil
	default:
		return OutcomeSkipped, ErrInvalidFacts
	}
}

func (c *Checker) resumeStopping(id int64) {
	if c.resume != nil {
		c.handoff(id, func() { c.resume(id) })
	}
}

func (*Checker) handoff(id int64, hint func()) {
	// A volatile hint cannot undo a confirmed decision. Keep panic diagnostics
	// private and let durable STOPPING recovery repair missed database admission.
	defer func() {
		if recover() != nil {
			logx.Errorw("job instance stopping handoff failed", logx.Field("instance_id", id))
		}
	}()
	hint()
}

func nilCheckDependency(dependency any) bool {
	if dependency == nil {
		return true
	}
	value := reflect.ValueOf(dependency)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
