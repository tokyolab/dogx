// Package historycleanup removes expired, terminal execution history in the
// elected scheduler. It never deletes Job definitions or active execution data.
package historycleanup

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/zeromicro/go-zero/core/logx"
)

// historySource reads the database clock and bounded pages of expired parents.
// Pages contain only positive IDs and nonzero finish times strictly before the
// cutoff, ordered by (FinishedAt, ID) after the optional cursor. An empty page
// ends the traversal; data returned with an error must not be consumed. Calls
// must honor cancellation and return only after their storage work has ended.
type historySource interface {
	DatabaseTime(context.Context) (time.Time, error)
	ListExpiredHistory(ctx context.Context, before time.Time, after *repository.HistoryCandidate, limit int) ([]repository.HistoryCandidate, error)
}

// Config controls serial history maintenance. Retention is elapsed time since
// an Instance finished; Interval is the delay after each pass actually returns,
// including a failed pass. PageSize is in [1, 100]. OperationTimeout bounds each
// discovery call and each whole Instance transaction, including pool/lock waits.
// Cancellation requests rollback but cannot prove that an uncertain commit failed
// or force a database driver to return. All settings must be positive.
type Config struct {
	Retention        time.Duration
	Interval         time.Duration
	PageSize         int
	OperationTimeout time.Duration
}

// Cleaner performs one traversal immediately upon Run, then waits Interval
// before each subsequent traversal. Each traversal fixes one database cutoff and
// deletes one complete Instance at a time, without loading Task content or
// retaining a persistent cursor. A failed Instance is retained for a later pass
// without preventing other candidates from being processed. Discovery failures
// end the pass. Confirmed deletions survive either kind of later failure.
//
// Run belongs to an elected term. The host must cancel it when leadership ends,
// await its actual return before another local term, and keep storage open until
// it returns. Different nodes may briefly overlap during a handoff; parent NOWAIT
// locks serialize deletion without waiting on another cleanup transaction.
type Cleaner struct {
	source   historySource
	transact repository.TransactionRunner
	config   Config
	active   atomic.Bool
}

type passResult struct {
	deleted        int64
	tasks          int64
	skipped        int64
	failed         int64
	failedInstance int64
	failureStage   string
}

var errInvalidHistory = errors.New("invalid history cleanup facts")

// New validates dependencies and settings without querying storage or starting
// maintenance. It does not own the supplied repository or transaction runner.
func New(source historySource, transact repository.TransactionRunner, config Config) (*Cleaner, error) {
	if source == nil || (reflect.ValueOf(source).Kind() == reflect.Pointer && reflect.ValueOf(source).IsNil()) || transact == nil {
		return nil, errors.New("history cleanup dependencies must not be nil")
	}
	if config.Retention <= 0 || config.Interval <= 0 || config.PageSize <= 0 || config.PageSize > 100 || config.OperationTimeout <= 0 {
		return nil, errors.New("history cleanup settings must be positive and page size at most 100")
	}
	return &Cleaner{source: source, transact: transact, config: config}, nil
}

// Run rejects concurrent calls on this Cleaner, including while canceled storage
// calls are still returning. It starts no new operation after observing term
// cancellation. Ordinary pass failures are logged without private database
// diagnostics and retried after Interval; cancellation returns ctx.Err(). A later
// Run starts a fresh traversal and does not inherit an earlier term's cursor.
func (c *Cleaner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("history cleanup context must not be nil")
	}
	if !c.active.CompareAndSwap(false, true) {
		return errors.New("history cleanup is already running")
	}
	defer c.active.Store(false)
	for ctx.Err() == nil {
		result, err := c.scan(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fields := []logx.LogField{
			logx.Field("instances_deleted", result.deleted),
			logx.Field("tasks_deleted", result.tasks),
			logx.Field("instances_skipped", result.skipped),
			logx.Field("instances_failed", result.failed),
		}
		if err != nil {
			// Driver errors can include stored parameters or connection details.
			// Only confirmed commits are counted, even when a later commit is unknown.
			fields = append(fields, logx.Field("stage", result.failureStage),
				logx.Field("instance_id", result.failedInstance), logx.Field("reason", failureReason(err)))
			logx.WithContext(ctx).Errorw("job history cleanup failed", fields...)
		} else {
			logx.WithContext(ctx).Infow("job history cleanup completed", fields...)
		}
		timer := time.NewTimer(c.config.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

func (c *Cleaner) scan(ctx context.Context) (passResult, error) {
	var result passResult
	now, err := query(ctx, c.config.OperationTimeout, c.source.DatabaseTime)
	if err != nil {
		result.failureStage = "database_clock"
		return result, fmt.Errorf("read history cleanup clock: %w", err)
	}
	if now.IsZero() {
		result.failureStage = "database_clock"
		return result, errInvalidHistory
	}
	before := now.Add(-c.config.Retention)
	var after *repository.HistoryCandidate
	var firstFailure error
	for {
		candidates, err := query(ctx, c.config.OperationTimeout, func(ctx context.Context) ([]repository.HistoryCandidate, error) {
			return c.source.ListExpiredHistory(ctx, before, after, c.config.PageSize)
		})
		if err != nil {
			result.failureStage = "candidate_query"
			result.failedInstance = 0
			return result, fmt.Errorf("list expired history: %w", err)
		}
		if !validCandidates(candidates, before, after, c.config.PageSize) {
			result.failureStage = "candidate_query"
			result.failedInstance = 0
			return result, errInvalidHistory
		}
		if len(candidates) == 0 {
			return result, firstFailure
		}
		for _, candidate := range candidates {
			deleted, tasks, err := c.deleteInstance(ctx, candidate.ID, before)
			if err != nil && !errors.Is(err, repository.ErrInstanceLocked) {
				if ctx.Err() != nil {
					return result, ctx.Err()
				}
				result.failed++
				if firstFailure == nil {
					result.failureStage, result.failedInstance = "instance_transaction", candidate.ID
					firstFailure = fmt.Errorf("delete expired instance %d: %w", candidate.ID, err)
				}
			} else if deleted {
				result.deleted++
				result.tasks += tasks
			} else {
				result.skipped++
			}
			// A failed, contested or newly ineligible parent is left for a later
			// pass. Keeping only the first failure bounds diagnostics and lets a
			// damaged or unusually expensive record coexist with useful progress.
			position := candidate
			after = &position
		}
	}
}

func failureReason(err error) string {
	switch {
	case errors.Is(err, errInvalidHistory):
		return "invalid_history"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "storage_error"
	}
}

func (c *Cleaner) deleteInstance(ctx context.Context, id int64, before time.Time) (bool, int64, error) {
	if err := ctx.Err(); err != nil {
		return false, 0, err
	}
	operation, cancel := context.WithTimeout(ctx, c.config.OperationTimeout)
	defer cancel()
	var deleted bool
	var tasks int64
	err := c.transact(operation, func(repos repository.Repositories) error {
		if err := operation.Err(); err != nil {
			return err
		}
		parent, err := repos.Instances.LockForHistoryCleanup(operation, id)
		if err != nil || parent == nil {
			return err
		}
		if parent.ID != id {
			return errInvalidHistory
		}
		if !terminal(parent.Status) || parent.FinishedAt == nil || parent.FinishedAt.IsZero() || !parent.FinishedAt.Before(before) {
			return nil
		}
		tasks, err = repos.Tasks.DeleteHistoryForInstance(operation, id)
		if err != nil {
			return err
		}
		if tasks < 0 {
			return errInvalidHistory
		}
		deleted, err = repos.Instances.DeleteHistory(operation, id, before)
		if err != nil {
			return err
		}
		if !deleted {
			// A terminal parent with unfinished children is inconsistent. The
			// repository guard retains it; failing this transaction also restores
			// any terminal children deleted above instead of erasing partial history.
			return errInvalidHistory
		}
		return nil
	})
	if err != nil {
		return false, 0, err
	}
	return deleted, tasks, nil
}

func terminal(status model.InstanceStatus) bool {
	return status == model.InstanceSucceeded || status == model.InstanceFailed || status == model.InstanceCanceled
}

func validCandidates(candidates []repository.HistoryCandidate, before time.Time, after *repository.HistoryCandidate, limit int) bool {
	if len(candidates) > limit {
		return false
	}
	for i := range candidates {
		candidate := &candidates[i]
		if candidate.ID <= 0 || candidate.FinishedAt.IsZero() || !candidate.FinishedAt.Before(before) {
			return false
		}
		if after != nil && (candidate.FinishedAt.Before(after.FinishedAt) || candidate.FinishedAt.Equal(after.FinishedAt) && candidate.ID <= after.ID) {
			return false
		}
		after = candidate
	}
	return true
}

func query[T any](ctx context.Context, timeout time.Duration, call func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	operation, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	value, err := call(operation)
	if err == nil {
		err = operation.Err()
	}
	return value, err
}
