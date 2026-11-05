// Package instancestop records a round's stop decision and converges its durable
// unfinished Tasks in separate bounded transactions. Database cancellation does
// not prove executor code has exited; remote notification is a separate concern.
package instancestop

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	// DefaultSQLTimeout bounds each database statement, including connection or
	// row-lock waiting. The caller separately controls the whole transaction.
	DefaultSQLTimeout = time.Second
	// DefaultBatchSize bounds one unfinished-Task query and conditional update.
	// It does not bound the total number of Tasks belonging to an Instance.
	DefaultBatchSize  = 256
	manualStopSummary = "Manual stop requested for this instance."
	timeoutSummary    = "Instance execution deadline reached."
)

// ErrInvalidState identifies inconsistent persisted stop facts or an impossible
// repository result. Callers must not repair state or publish stop success from it.
var ErrInvalidState = errors.New("invalid instance stop state")

// Decision contains facts observed in a successfully completed parent transaction.
// New is true only for this call's newly committed STOPPING decision. An
// existing stopping or terminal state is returned unchanged with New false. Neither
// case proves child cancellation or executor exit. Only New grants permission to
// publish a new stop notification; the caller owns all post-commit handoff.
type Decision struct {
	InstanceID      int64
	ExecutorGroupID int64
	Status          model.InstanceStatus
	EndReason       *string
	New             bool
}

// BatchResult describes one committed cancellation batch or an observed parent.
// More permits another separately budgeted batch only after a nonempty child batch
// committed. Canceled counts newly canceled rows and can be zero if results won
// the race. Completed is true only for a newly committed final parent transition;
// an already-terminal parent is represented by Status with Completed false.
// A zero-row final update leaves Status stopping, More false and Completed false.
type BatchResult struct {
	InstanceID int64
	Status     model.InstanceStatus
	Canceled   int64
	More       bool
	Completed  bool
}

// Controller owns no connection, queue, goroutine or remote client. Its runner
// must confirm commit before returning nil and may not retry callbacks. Calls may
// run concurrently when the runner supports them; the database parent lock supplies
// coordination. The host retains resources until each call actually returns.
type Controller struct {
	transact   repository.TransactionRunner
	sqlTimeout time.Duration
	batchSize  int
}

// Option configures a Controller before concurrent use begins.
type Option func(*Controller)

// WithSQLTimeout changes the positive per-statement budget. It never extends an
// earlier caller deadline or supplies a new whole-operation/shutdown budget.
func WithSQLTimeout(timeout time.Duration) Option {
	return func(c *Controller) { c.sqlTimeout = timeout }
}

// WithBatchSize changes the positive limit of each child query/update batch.
func WithBatchSize(size int) Option { return func(c *Controller) { c.batchSize = size } }

// NewController binds a transaction runner without accessing the database. All
// SQL handles used during a callback belong to that transaction and must not
// escape it; only the runner's successful return confirms a new decision or batch.
func NewController(transact repository.TransactionRunner, options ...Option) (*Controller, error) {
	if transact == nil {
		return nil, errors.New("instance stop transaction runner is nil")
	}
	c := &Controller{transact: transact, sqlTimeout: DefaultSQLTimeout, batchSize: DefaultBatchSize}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("instance stop option is nil")
		}
		option(c)
	}
	if c.sqlTimeout <= 0 || c.batchSize <= 0 {
		return nil, errors.New("instance stop settings must be positive")
	}
	return c, nil
}

// RequestManual records an active Instance's manual stop under its single parent
// row lock. The lock may wait within the SQL/caller budgets. Database time is read
// after the lock; an already-expired running Instance still uses manual_stop.
// Waiting Instances retain NULL start/deadline fields. Existing stopping/terminal
// parents retain their reason and result without a new decision or notification.
// A missing parent preserves repository.ErrInstanceNotFound; invalid active timing
// is diagnosed and rejected instead of repaired or treated as unlimited execution.
//
// The caller supplies the whole operation Context, including transaction begin and
// commit. Each statement has its own bounded child Context. Cancellation requests
// database cleanup but is not a hard completion or rollback guarantee. Any runner
// error, including a lost commit confirmation, returns a zero Decision. This method
// never cancels child rows, sends RPCs or invokes post-commit callbacks.
func (c *Controller) RequestManual(ctx context.Context, id int64) (Decision, error) {
	return c.requestStop(ctx, id, false)
}

// RequestTimeout attempts a timeout stop only after the caller has successfully
// read this round's completion facts and explicitly found them not ready. It must
// not follow a failed read, failed or zero-row normal completion, or an unsupported
// execution mode. Complete final results take the normal completion path first.
//
// The parent lock coordinates with normal completion, manual stopping and new
// execution authorization. After acquiring it, this method reads fresh database
// time and stops only a still-running parent at or past its valid deadline. It
// neither rereads Tasks under that lock nor promises global completion priority;
// results can arrive after the caller's read. Waiting parents remain untouched,
// while existing stopping or terminal facts retain their original reason.
//
// Commit confirmation, missing-parent errors, SQL budgets and actual resource
// ownership follow RequestManual. Only a new confirmed decision sets New; the
// caller separately hands it to database convergence and best-effort notification.
func (c *Controller) RequestTimeout(ctx context.Context, id int64) (Decision, error) {
	return c.requestStop(ctx, id, true)
}

func (c *Controller) requestStop(ctx context.Context, id int64, timeout bool) (Decision, error) {
	if id <= 0 {
		return Decision{}, errors.New("instance stop ID must be positive")
	}
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	var result Decision
	err := c.transact(ctx, func(repos repository.Repositories) error {
		instance, err := sqlCall(ctx, c.sqlTimeout, func(sqlCtx context.Context) (*repository.StoppingInstance, error) {
			return repos.Instances.LockForStop(sqlCtx, id, false)
		})
		if err != nil {
			return err
		}
		if instance == nil {
			return repository.ErrInstanceNotFound
		}
		if instance.ID != id {
			return ErrInvalidState
		}
		result = observedDecision(instance)
		switch instance.Status {
		case model.InstanceStopping, model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled:
			return nil
		case model.InstanceWaiting:
			if timeout {
				return nil
			}
		case model.InstanceRunning:
		default:
			return ErrInvalidState
		}
		now, err := sqlCall(ctx, c.sqlTimeout, repos.Instances.DatabaseTime)
		if err != nil {
			return err
		}
		if now.IsZero() || instance.ExecutorGroupID <= 0 {
			return ErrInvalidState
		}
		if !validActiveTimes(instance) {
			logx.WithContext(ctx).Errorw("instance stop rejected invalid execution times", logx.Field("instance_id", id))
			return ErrInvalidState
		}
		reason, summary := model.EndReasonManualStop, manualStopSummary
		if timeout {
			if now.Before(*instance.DeadlineAt) {
				return nil
			}
			reason, summary = model.EndReasonInstanceTimeout, timeoutSummary
		}
		changed, err := sqlCall(ctx, c.sqlTimeout, func(sqlCtx context.Context) (bool, error) {
			return repos.Instances.RecordStop(sqlCtx, id, instance.Status, reason, summary, now)
		})
		if err != nil {
			return err
		}
		if !changed {
			// The active parent is locked by this transaction. Losing the exact
			// transition is an invariant failure, not a successfully repeated stop.
			return ErrInvalidState
		}
		result.Status, result.EndReason, result.New = model.InstanceStopping, &reason, true
		return nil
	})
	if err != nil {
		return Decision{}, fmt.Errorf("request stop for instance %d: %w", id, err)
	}
	return result, nil
}

// ProcessBatch performs one child-cancellation transaction for a stopping parent.
// It acquires the parent by primary-key NOWAIT before reading children. A conflict
// preserves repository.ErrInstanceLocked after rollback; callers must end the
// current processing turn rather than immediately retrying the same parent.
// Missing or non-stopping parents cause no writes. Existing terminal facts remain
// unchanged. The stopping reason must be manual_stop or instance_timeout.
//
// At most batchSize unfinished IDs are read, then canceled with one fresh database
// time. Concurrent child results can reduce the affected count without failure.
// Only an empty child query permits the final conditional NOT EXISTS transition.
// No child cursor, whole-round transaction, type-specific branch or internal loop
// is used. More and Completed become visible only after confirmed commit; any error
// returns a zero result, including commit uncertainty. SQL budgets and actual-return
// resource ownership follow RequestManual. This method never sends notifications.
func (c *Controller) ProcessBatch(ctx context.Context, id int64) (BatchResult, error) {
	if id <= 0 {
		return BatchResult{}, errors.New("instance stop ID must be positive")
	}
	if err := ctx.Err(); err != nil {
		return BatchResult{}, err
	}
	var result BatchResult
	err := c.transact(ctx, func(repos repository.Repositories) error {
		instance, err := sqlCall(ctx, c.sqlTimeout, func(sqlCtx context.Context) (*repository.StoppingInstance, error) {
			return repos.Instances.LockForStop(sqlCtx, id, true)
		})
		if err != nil {
			// A NOWAIT failure aborts the transaction. Let the runner roll back
			// before callers classify the conflict and move to another parent.
			return err
		}
		if instance == nil {
			return nil
		}
		if instance.ID != id {
			return ErrInvalidState
		}
		result = BatchResult{InstanceID: id, Status: instance.Status}
		switch instance.Status {
		case model.InstanceWaiting, model.InstanceRunning, model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled:
			return nil
		case model.InstanceStopping:
		default:
			return ErrInvalidState
		}
		target, summary, ok := stopOutcome(instance.EndReason)
		if !ok || instance.FinishedAt != nil {
			return ErrInvalidState
		}
		ids, err := sqlCall(ctx, c.sqlTimeout, func(sqlCtx context.Context) ([]int64, error) {
			return repos.Tasks.ListUnfinishedIDs(sqlCtx, id, c.batchSize)
		})
		if err != nil {
			return err
		}
		if !validOrderedIDs(ids, c.batchSize) {
			return ErrInvalidState
		}
		if len(ids) == 0 {
			changed, err := sqlCall(ctx, c.sqlTimeout, func(sqlCtx context.Context) (bool, error) {
				return repos.Instances.FinishStop(sqlCtx, id, *instance.EndReason)
			})
			if err != nil {
				return err
			}
			if changed {
				result.Status, result.Completed = target, true
			}
			return nil
		}
		now, err := sqlCall(ctx, c.sqlTimeout, repos.Instances.DatabaseTime)
		if err != nil {
			return err
		}
		if now.IsZero() {
			return ErrInvalidState
		}
		canceled, err := sqlCall(ctx, c.sqlTimeout, func(sqlCtx context.Context) (int64, error) {
			return repos.Tasks.CancelUnfinished(sqlCtx, repository.TaskCancellation{InstanceID: id, TaskIDs: ids,
				Reason: *instance.EndReason, Summary: summary, ProcessedAt: now})
		})
		if err != nil {
			return err
		}
		if canceled < 0 || canceled > int64(len(ids)) {
			return ErrInvalidState
		}
		result.Canceled, result.More = canceled, true
		return nil
	})
	if err != nil {
		return BatchResult{}, fmt.Errorf("process stopping instance %d: %w", id, err)
	}
	return result, nil
}

func observedDecision(instance *repository.StoppingInstance) Decision {
	result := Decision{InstanceID: instance.ID, ExecutorGroupID: instance.ExecutorGroupID, Status: instance.Status}
	if instance.EndReason != nil {
		reason := *instance.EndReason
		result.EndReason = &reason
	}
	return result
}

func validActiveTimes(instance *repository.StoppingInstance) bool {
	if instance.FinishedAt != nil {
		return false
	}
	if instance.Status == model.InstanceWaiting {
		return instance.StartedAt == nil && instance.DeadlineAt == nil
	}
	return instance.StartedAt != nil && !instance.StartedAt.IsZero() && instance.DeadlineAt != nil && instance.DeadlineAt.After(*instance.StartedAt)
}

func stopOutcome(reason *string) (model.InstanceStatus, string, bool) {
	if reason != nil {
		switch *reason {
		case model.EndReasonManualStop:
			return model.InstanceCanceled, "Task canceled because its instance was manually stopped.", true
		case model.EndReasonInstanceTimeout:
			return model.InstanceFailed, "Task canceled because its instance timed out.", true
		}
	}
	return 0, "", false
}

func validOrderedIDs(ids []int64, limit int) bool {
	if len(ids) > limit {
		return false
	}
	var previous int64
	for _, id := range ids {
		if id <= previous {
			return false
		}
		previous = id
	}
	return true
}

// sqlCall retains the transaction's caller Context while bounding one statement.
// A late result after cancellation requests rollback instead of authorizing the
// next statement; actual callback/commit confirmation still belongs to the runner.
func sqlCall[T any](ctx context.Context, timeout time.Duration, call func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := call(ctx)
	if err == nil {
		err = ctx.Err()
	}
	return result, err
}
