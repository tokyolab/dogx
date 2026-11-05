package taskrecovery

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	// DefaultScanInterval is the delay after a run actually ends, before adding
	// independent 20 percent jitter. It is not a periodic scheduling tick.
	DefaultScanInterval = 10 * time.Second
	// DefaultPageSize bounds the retained candidate page, not a run's total work.
	DefaultPageSize = 128
	// DefaultRunTimeout covers clock reads, queries and all short-batch settlement
	// in one run. Later runs receive a fresh budget without widening the traversal.
	DefaultRunTimeout = 5 * time.Second
)

// candidateSource reads an ordered, bounded page without claiming work. scanNow
// and observationCutoff remain fixed for a traversal, including runs after failure
// or budget exhaustion. A nil cursor starts at the beginning; otherwise only keys
// strictly after its original registration time and Task ID may be returned.
// The source must not mutate the cursor or issue a write. Missing work returns an
// empty slice, while errors preserve their causes and do not advance any position.
type candidateSource interface {
	ListUnboundCandidates(context.Context, time.Time, time.Time, *repository.UnboundCursor, int) ([]repository.UnboundCandidate, error)
}

// batchSettler interprets only a contiguous original prefix, using fresh database
// time and independent writes as documented by Settler.SettleBatch. Nil error
// means all supplied candidates were processed. On error, Processed may include
// one attempted failed write but must exclude unstarted work and failed reads.
// It must not modify candidates or retain them after the call actually returns.
type batchSettler interface {
	SettleBatch(context.Context, []repository.UnboundCandidate) (BatchResult, error)
}

// Config controls one node's independent observation recovery. Every value must
// be positive. PageSize is a query/page bound; internal settlement batches remain
// at most MaxBatchSize. OperationTimeout bounds discovery calls, and RunTimeout
// also bounds settlement through its caller Context. The host must configure its
// Settler with the same OperationTimeout and ObservationPeriod; these settings do
// not alter an already-constructed dependency. All nodes use the same observation
// period. ScanInterval permits up to 20 percent jitter without duration overflow.
type Config struct {
	ScanInterval      time.Duration
	PageSize          int
	RunTimeout        time.Duration
	OperationTimeout  time.Duration
	ObservationPeriod time.Duration
}

// Runtime owns one serial scan/settlement goroutine per node. It does not use
// leader ownership, a work queue, a processing pool or retained target snapshots.
// Only one bounded candidate page and temporary traversal coordinates survive
// during a run; between runs it retains coordinates only. All lifecycle methods
// are concurrency-safe, and dependencies remain owned by the host through Wait.
type Runtime struct {
	clock      databaseClock
	candidates candidateSource
	settler    batchSettler
	config     Config
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	mu         sync.Mutex
	started    bool
	stopped    bool
}

// traversal is temporary discovery progress, never a durable consumption watermark.
// It belongs to the single runtime goroutine and is discarded at the traversal tail
// or process stop. Candidates before this cursor are revisited in a later traversal.
type traversal struct {
	scanNow time.Time
	cutoff  time.Time
	cursor  *repository.UnboundCursor
}

// NewRuntime validates settings and binds dependencies without starting work or
// taking ownership of their resources. The clock and candidate source can be
// separate repositories; no combined adapter or additional connection is needed.
func NewRuntime(clock databaseClock, candidates candidateSource, settler batchSettler, config Config) (*Runtime, error) {
	if nilDependency(clock) || nilDependency(candidates) || nilDependency(settler) {
		return nil, errors.New("unbound recovery dependencies must not be nil")
	}
	if config.ScanInterval <= 0 || config.PageSize <= 0 || config.RunTimeout <= 0 || config.OperationTimeout <= 0 || config.ObservationPeriod <= 0 {
		return nil, errors.New("unbound recovery settings must be positive")
	}
	if config.ScanInterval > time.Duration(1<<63-1)/6*5 {
		return nil, errors.New("unbound recovery interval is too large for jitter")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Runtime{clock: clock, candidates: candidates, settler: settler, config: config,
		ctx: ctx, cancel: cancel, done: make(chan struct{})}, nil
}

// Start begins one immediate run and returns without waiting for discovery.
// Repeated Start, including Start after Stop, fails; this runtime cannot restart.
func (r *Runtime) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopped {
		return errors.New("unbound recovery has already started or stopped")
	}
	r.started = true
	go r.loop()
	return nil
}

// Stop cancels discovery and in-flight settlement and prevents further runs. It
// is nonblocking and idempotent. Cancellation does not prove a database call ended
// or a write rolled back; callers must use Wait before releasing dependencies.
// Stop before Start completes the unused runtime without launching a goroutine.
func (r *Runtime) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	r.stopped = true
	r.cancel()
	if !r.started {
		close(r.done)
	}
}

// Wait waits for actual loop and dependency-call completion. It does not stop the
// runtime, extend its budgets or claim database success. A caller timeout leaves
// resources owned until a later Wait confirms completion; a nil result is that
// lifetime barrier. An unstarted runtime waits until it is started or stopped.
func (r *Runtime) Wait(ctx context.Context) error {
	select {
	case <-r.done:
		return nil
	default:
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runtime) loop() {
	defer close(r.done)
	var state traversal
	for r.ctx.Err() == nil {
		if err := r.run(&state); err != nil && r.ctx.Err() == nil {
			// Stored configuration and driver errors can contain private values.
			// Log a fixed classification, never the original error or candidate data.
			logx.Errorw("unbound task recovery run failed", logx.Field("reason", recoveryFailureKind(err)))
		}
		if r.ctx.Err() != nil {
			return
		}
		base := r.config.ScanInterval
		jitter := base / 5
		timer := time.NewTimer(base - jitter + time.Duration(rand.Int64N(int64(2*jitter)+1)))
		select {
		case <-r.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (r *Runtime) run(state *traversal) error {
	ctx, cancel := context.WithTimeout(r.ctx, r.config.RunTimeout)
	defer cancel()
	err := r.scan(ctx, state)
	if ctx.Err() != nil && errors.Is(err, context.DeadlineExceeded) {
		// Exhausting the whole run while backlog remains is expected. A shorter
		// operation timeout is still a failure when this run budget remains live.
		return nil
	}
	return err
}

func (r *Runtime) scan(ctx context.Context, state *traversal) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if state.scanNow.IsZero() {
		opCtx, cancel := context.WithTimeout(ctx, r.config.OperationTimeout)
		now, err := r.clock.DatabaseTime(opCtx)
		if err == nil {
			err = opCtx.Err()
		}
		cancel()
		if err != nil {
			return fmt.Errorf("read unbound recovery scan time: %w", err)
		}
		cutoff := now.Add(-r.config.ObservationPeriod)
		if now.IsZero() || cutoff.IsZero() {
			return ErrInvalidFacts
		}
		state.scanNow, state.cutoff = now, cutoff
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var cursor *repository.UnboundCursor
		if state.cursor != nil {
			value := *state.cursor
			cursor = &value
		}
		opCtx, cancel := context.WithTimeout(ctx, r.config.OperationTimeout)
		page, err := r.candidates.ListUnboundCandidates(opCtx, state.scanNow, state.cutoff, cursor, r.config.PageSize)
		if err == nil {
			err = opCtx.Err()
		}
		cancel()
		if err != nil {
			return fmt.Errorf("read unbound recovery candidates: %w", err)
		}
		if !validUnboundPage(page, state, r.config.PageSize) {
			return ErrInvalidFacts
		}
		for offset := 0; offset < len(page); {
			if err := ctx.Err(); err != nil {
				return err
			}
			count := min(MaxBatchSize, len(page)-offset)
			batch := page[offset : offset+count]
			result, err := r.settler.SettleBatch(ctx, batch)
			if result.Processed < 0 || result.Processed > len(batch) || result.Applied < 0 || result.Applied > result.Processed ||
				(err == nil && result.Processed != len(batch)) {
				return ErrInvalidFacts
			}
			if result.Processed > 0 {
				last := batch[result.Processed-1]
				state.cursor = &repository.UnboundCursor{AttemptRegisteredAt: last.AttemptRegisteredAt, TaskID: last.TaskID}
			}
			if err != nil {
				return fmt.Errorf("settle unbound recovery candidates: %w", err)
			}
			offset += result.Processed
		}
		if len(page) < r.config.PageSize {
			// A tail is not permission to use this run's remaining budget to start
			// over. New or late-committed older keys wait for the next delayed run.
			*state = traversal{}
			return nil
		}
	}
}

func validUnboundPage(page []repository.UnboundCandidate, state *traversal, limit int) bool {
	if len(page) > limit {
		return false
	}
	cursor := state.cursor
	for _, candidate := range page {
		if candidate.TaskID <= 0 || candidate.InstanceID <= 0 || candidate.AttemptNo <= 0 || candidate.AttemptRegisteredAt.IsZero() || candidate.AttemptRegisteredAt.After(state.cutoff) {
			return false
		}
		if cursor != nil && (candidate.AttemptRegisteredAt.Before(cursor.AttemptRegisteredAt) ||
			(candidate.AttemptRegisteredAt.Equal(cursor.AttemptRegisteredAt) && candidate.TaskID <= cursor.TaskID)) {
			return false
		}
		cursor = &repository.UnboundCursor{AttemptRegisteredAt: candidate.AttemptRegisteredAt, TaskID: candidate.TaskID}
	}
	return true
}

func recoveryFailureKind(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "operation_timeout"
	case errors.Is(err, context.Canceled):
		return "operation_canceled"
	case errors.Is(err, ErrInvalidConfig):
		return "invalid_config"
	case errors.Is(err, ErrInvalidFacts):
		return "invalid_facts"
	default:
		return "operation_failed"
	}
}
