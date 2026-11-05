package instancestop

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

// stoppingSource supplies bounded, ordered parent IDs without claiming work.
// Implementations propagate cancellation to storage and preserve errors. A zero
// upper bound or empty page means no current candidates; neither is a failure.
type stoppingSource interface {
	StoppingUpperID(context.Context) (int64, error)
	ListStoppingCandidates(context.Context, int64, int64, int) ([]int64, error)
}

// RecoveryConfig bounds one leader's STOPPING discovery branch. Every value is
// positive. RunTimeout covers a scan pass, while SQLTimeout caps each query.
// PageSize limits one retained page, not the total work in a pass or traversal.
type RecoveryConfig struct {
	PageSize   int
	RunTimeout time.Duration
	SQLTimeout time.Duration
}

// Recovery only discovers durable STOPPING parents and hands IDs to process-wide
// cancellation workers. It does not run cancellation transactions or send RPCs.
// The host owns the source and queue until Run actually exits. Losing leadership
// cancels discovery and discards its cursor, but never cancels admitted work.
type Recovery struct {
	source stoppingSource
	hint   func(int64) bool
	config RecoveryConfig
	active atomic.Bool
}

type stoppingPosition struct{ upper, after int64 }

// NewRecovery binds a source and nonblocking admission function without starting
// work. Admission returns true for a new or merged ID, false when unavailable.
func NewRecovery(source stoppingSource, hint func(int64) bool, config RecoveryConfig) (*Recovery, error) {
	if source == nil || (reflect.ValueOf(source).Kind() == reflect.Pointer && reflect.ValueOf(source).IsNil()) || hint == nil {
		return nil, errors.New("stopping recovery dependencies must not be nil")
	}
	if config.PageSize <= 0 || config.RunTimeout <= 0 || config.SQLTimeout <= 0 {
		return nil, errors.New("stopping recovery settings must be positive")
	}
	return &Recovery{source: source, hint: hint, config: config}, nil
}

// Run performs an immediate bounded pass and then one pass per received tick for
// one elected term. The host owns the non-nil tick channel and must drop ticks
// while discovery is busy, not accumulate catch-up work. Closing ticks ends Run
// with an error. Only one Run may be active; a later term starts from zero. Full
// admission ends the pass immediately. Query failure or budget expiry retains
// only the last admitted position for the next pass, never a permanent watermark.
// Cancellation waits for actual query return before releasing term resources.
func (r *Recovery) Run(ctx context.Context, ticks <-chan struct{}) error {
	if ticks == nil {
		return errors.New("stopping recovery ticks must not be nil")
	}
	if !r.active.CompareAndSwap(false, true) {
		return errors.New("stopping recovery is already running")
	}
	defer r.active.Store(false)
	var position stoppingPosition
	for ctx.Err() == nil {
		pass, cancel := context.WithTimeout(ctx, r.config.RunTimeout)
		err := r.scan(pass, &position)
		cancel()
		if err != nil && ctx.Err() == nil {
			// Storage diagnostics can contain private data; preserve the durable
			// obligation without logging the underlying error or inventing success.
			logx.Error("stopping instance discovery failed")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-ticks:
			if !ok {
				return errors.New("stopping recovery ticks closed")
			}
		}
	}
	return ctx.Err()
}

func (r *Recovery) scan(ctx context.Context, position *stoppingPosition) error {
	if position.upper == 0 {
		upper, err := sqlCall(ctx, r.config.SQLTimeout, r.source.StoppingUpperID)
		if err != nil {
			return err
		}
		if upper < 0 {
			return ErrInvalidState
		}
		if upper == 0 {
			return nil
		}
		position.upper = upper
	}
	for {
		ids, err := sqlCall(ctx, r.config.SQLTimeout, func(query context.Context) ([]int64, error) {
			return r.source.ListStoppingCandidates(query, position.after, position.upper, r.config.PageSize)
		})
		if err != nil {
			return err
		}
		if !validOrderedIDs(ids, r.config.PageSize) || (len(ids) > 0 && (ids[0] <= position.after || ids[len(ids)-1] > position.upper)) {
			return ErrInvalidState
		}
		if len(ids) == 0 {
			*position = stoppingPosition{}
			return nil
		}
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !r.hint(id) {
				return nil
			}
			position.after = id
		}
		if position.after == position.upper {
			*position = stoppingPosition{}
			return nil
		}
	}
}
