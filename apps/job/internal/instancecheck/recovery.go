package instancecheck

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/zeromicro/go-zero/core/logx"
)

// runningSource supplies a database clock and ordered RUNNING parent candidates.
// It must propagate cancellation and return only after storage cleanup finishes.
// Pages contain positive IDs and start times strictly before eligibleBefore, in
// ascending (StartedAt, ID) order after the optional cursor. An empty page ends
// the traversal; partial data returned with an error must not be used.
type runningSource interface {
	DatabaseTime(context.Context) (time.Time, error)
	ListRunningCandidates(ctx context.Context, eligibleBefore time.Time, after *repository.RunningCandidate, limit int) ([]repository.RunningCandidate, error)
}

// recoveryAdmission shares the process-owned check queue with normal hints.
// WaitRecoveryCapacity waits for room without reserving it. HintRecovery waits
// for a new position or merges an existing ID even when full. A successful hint
// retains no caller Context and advances discovery, not durable execution state.
type recoveryAdmission interface {
	WaitRecoveryCapacity(context.Context) error
	HintRecovery(context.Context, int64) error
}

// RecoveryConfig bounds one leader's RUNNING discovery branch. All values must
// be positive. MinAge is subtracted from one database time per traversal; it does
// not change across continuation passes. RunTimeout includes capacity waits,
// database time, queries and handoff; SQLTimeout caps each individual query.
type RecoveryConfig struct {
	PageSize   int
	RunTimeout time.Duration
	SQLTimeout time.Duration
	MinAge     time.Duration
}

// Recovery discovers older RUNNING parents and hands them to the existing local
// check queue. It never interprets deadlines, reads Task facts or owns execution.
// The host supplies elected-term ticks and retains dependencies until Run returns.
// Losing leadership discards discovery's cursor, without canceling admitted work.
type Recovery struct {
	source    runningSource
	admission recoveryAdmission
	config    RecoveryConfig
	active    atomic.Bool
}

type runningPosition struct {
	eligibleBefore time.Time
	after          *repository.RunningCandidate
}

// NewRecovery binds discovery dependencies without starting background work.
func NewRecovery(source runningSource, admission recoveryAdmission, config RecoveryConfig) (*Recovery, error) {
	if nilRecoveryDependency(source) || nilRecoveryDependency(admission) {
		return nil, errors.New("running recovery dependencies must not be nil")
	}
	if config.PageSize <= 0 || config.RunTimeout <= 0 || config.SQLTimeout <= 0 || config.MinAge <= 0 {
		return nil, errors.New("running recovery settings must be positive")
	}
	return &Recovery{source: source, admission: admission, config: config}, nil
}

// Run performs an immediate pass, then one serial pass per received tick. The
// caller owns ticks and must keep it open until cancellation. Run never closes
// it, launches overlapping passes or manufactures replacement ticks. Only one
// Run may be active, including while canceled storage calls are still returning.
//
// Each pass has a fresh budget. Failure or budget expiry preserves the fixed
// eligibility bound and last admitted cursor for the next tick; an exhausted
// traversal starts anew on the next tick. Each later Run starts from zero.
func (r *Recovery) Run(ctx context.Context, ticks <-chan struct{}) error {
	if ctx == nil || ticks == nil {
		return errors.New("running recovery context and ticks must not be nil")
	}
	if !r.active.CompareAndSwap(false, true) {
		return errors.New("running recovery is already running")
	}
	defer r.active.Store(false)
	var position runningPosition
	for ctx.Err() == nil {
		pass, cancel := context.WithTimeout(ctx, r.config.RunTimeout)
		err := r.scan(pass, &position)
		cancel()
		if err != nil && ctx.Err() == nil {
			// Database diagnostics can contain private data; retain the durable
			// recovery obligation without logging raw storage or queue errors.
			logx.Error("running instance discovery failed")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-ticks:
			if !ok {
				if err := ctx.Err(); err != nil {
					return err
				}
				return errors.New("running recovery tick stream closed")
			}
		}
	}
	return ctx.Err()
}

func (r *Recovery) scan(ctx context.Context, position *runningPosition) error {
	for {
		// Do not hold a connection, transaction or page from a previous pass
		// while waiting. Normal hints may consume this unreserved space later.
		if err := r.admission.WaitRecoveryCapacity(ctx); err != nil {
			return err
		}
		if position.eligibleBefore.IsZero() {
			now, err := recoveryQuery(ctx, r.config.SQLTimeout, r.source.DatabaseTime)
			if err != nil {
				return err
			}
			if now.IsZero() {
				return ErrInvalidFacts
			}
			position.eligibleBefore = now.Add(-r.config.MinAge)
		}
		candidates, err := recoveryQuery(ctx, r.config.SQLTimeout, func(query context.Context) ([]repository.RunningCandidate, error) {
			return r.source.ListRunningCandidates(query, position.eligibleBefore, position.after, r.config.PageSize)
		})
		if err != nil {
			return err
		}
		if !validRunningCandidates(candidates, *position, r.config.PageSize) {
			return ErrInvalidFacts
		}
		if len(candidates) == 0 {
			*position = runningPosition{}
			return nil
		}
		for _, candidate := range candidates {
			if err := r.admission.HintRecovery(ctx, candidate.ID); err != nil {
				return err
			}
			// A copied cursor survives releasing this bounded page. Querying a
			// candidate is not admission, so the first refused ID is never skipped.
			accepted := candidate
			position.after = &accepted
		}
	}
}

func validRunningCandidates(candidates []repository.RunningCandidate, position runningPosition, limit int) bool {
	if len(candidates) > limit {
		return false
	}
	previous := position.after
	for index := range candidates {
		candidate := &candidates[index]
		if candidate.ID <= 0 || candidate.StartedAt.IsZero() || !candidate.StartedAt.Before(position.eligibleBefore) {
			return false
		}
		if previous != nil && (candidate.StartedAt.Before(previous.StartedAt) ||
			candidate.StartedAt.Equal(previous.StartedAt) && candidate.ID <= previous.ID) {
			return false
		}
		previous = candidate
	}
	return true
}

func recoveryQuery[T any](ctx context.Context, timeout time.Duration, call func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	query, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	value, err := call(query)
	if err == nil {
		err = query.Err()
	}
	return value, err
}

func nilRecoveryDependency(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
