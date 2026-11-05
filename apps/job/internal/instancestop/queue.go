package instancestop

import (
	"container/list"
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/zeromicro/go-zero/core/logx"
)

var errProcessorPanicked = errors.New("instance stop processor panicked")

// processor commits one bounded child-cancellation batch or observes the parent.
// ProcessBatch must support concurrent calls for different IDs, retain resources
// until its transaction actually returns, and report More only after confirmed
// commit. Errors, including repository.ErrInstanceLocked, end this turn without
// authorizing an immediate retry. The host owns all database dependencies.
type processor interface {
	ProcessBatch(context.Context, int64) (BatchResult, error)
}

// Config controls retained Instance IDs and database cancellation workers.
// Capacity includes waiting and processing IDs. Concurrency bounds actual calls,
// including transaction cleanup after cancellation. MaxBatches and RunTimeout
// limit each worker's turn before a successful unfinished Instance yields to the
// queue tail. RunTimeout is a time allowance checked between batches, not a
// transaction deadline; the processor independently bounds each SQL statement.
// All values must be positive. The host supplies deployment defaults.
type Config struct {
	Capacity    int
	Concurrency int
	RunTimeout  time.Duration
	MaxBatches  int
}

// Queue coalesces volatile database-cancellation hints by Instance ID. Repeated
// waiting or processing hints share one position without creating another turn.
// A successful unfinished turn keeps that position and rotates to the tail;
// conflict or failure releases it, discarding hints merged before that release.
// A later new hint or recovery scan may admit the Instance again.
//
// All methods support concurrent calls. Start is single-use. Stop closes new
// admission while retained work drains. Drain applies the host's shared shutdown
// Context; Wait is the actual-return barrier before dependencies may be closed.
// The queue neither sends executor notifications nor owns database connections.
type Queue struct {
	processor processor
	config    Config
	runCtx    context.Context
	cancel    context.CancelFunc
	done      chan struct{}

	mu         sync.Mutex
	started    bool
	stopping   bool
	completed  bool
	fullLogged bool
	entries    map[int64]struct{}
	pending    list.List
	changed    chan struct{}
	drainCtx   context.Context
	stopDrain  func() bool
	drainErr   error
}

// NewQueue binds the processor without starting workers or accepting hints.
// Start must precede Hint. The host must eventually Stop and Wait, using Drain
// when retained work must respect an existing shutdown deadline.
func NewQueue(processor processor, config Config) (*Queue, error) {
	if nilProcessor(processor) {
		return nil, errors.New("instance stop processor must not be nil")
	}
	if config.Capacity <= 0 || config.Concurrency <= 0 || config.RunTimeout <= 0 || config.MaxBatches <= 0 {
		return nil, errors.New("instance stop queue settings must be positive")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Queue{processor: processor, config: config, runCtx: ctx, cancel: cancel,
		done: make(chan struct{}), entries: make(map[int64]struct{}), changed: make(chan struct{})}, nil
}

// Start opens admission and starts the configured fixed number of workers.
// A previous Start or Stop prevents reuse and never creates replacement workers.
func (q *Queue) Start() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.started || q.stopping {
		return errors.New("instance stop queue has already started or stopped")
	}
	q.started = true
	var workers sync.WaitGroup
	for range q.config.Concurrency {
		workers.Go(q.work)
	}
	go func() {
		workers.Wait()
		q.finish()
	}()
	return nil
}

// Hint immediately admits or merges a positive Instance ID. False means invalid
// input, closed admission or full capacity for different IDs. Existing IDs still
// merge when full, without reserving a follow-up after failure or completion.
// True confirms only volatile admission, not durable cancellation or delivery.
// No caller Context is retained and no capacity wait is introduced.
func (q *Queue) Hint(id int64) bool {
	logFull := false
	q.mu.Lock()
	defer func() {
		q.mu.Unlock()
		if logFull {
			// Suppress repeated drops until an ID actually releases its slot.
			// Logging must not hold the admission or completion lock.
			logx.Errorw("job instance stop hint dropped because the queue is full", logx.Field("instance_id", id))
		}
	}()
	if id <= 0 || !q.started || q.stopping {
		return false
	}
	if _, ok := q.entries[id]; ok {
		return true
	}
	if len(q.entries) == q.config.Capacity {
		logFull, q.fullLogged = !q.fullLogged, true
		return false
	}
	q.entries[id] = struct{}{}
	q.pending.PushBack(id)
	q.notifyLocked()
	return true
}

// Stop closes admission without canceling admitted batches or successful tail
// rotations. Repeated calls are safe. Stop before Start completes an unused queue.
// It does not release shared dependencies; Wait confirms actual worker exit.
func (q *Queue) Stop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopping {
		return
	}
	q.stopping = true
	q.notifyLocked()
	if !q.started {
		q.completed = true
		q.cancel()
		close(q.done)
	}
}

// Drain closes admission and waits with the host's non-nil shutdown Context.
// The first call installs the shared budget; later calls cannot extend it. Its
// cancellation requests in-flight transaction cleanup and abandons pending IDs,
// but cannot force an uncooperative dependency to return. Wait remains required
// before the host releases dependencies after a Drain timeout. Individual batch
// errors are logged; nil does not promise that every parent became terminal.
func (q *Queue) Drain(ctx context.Context) error {
	if ctx == nil {
		return errors.New("instance stop drain context must not be nil")
	}
	q.Stop()
	q.mu.Lock()
	if q.completed {
		err := q.drainErr
		q.mu.Unlock()
		return err
	}
	first := q.drainCtx == nil
	if first {
		q.drainCtx = ctx
		q.stopDrain = context.AfterFunc(ctx, q.cancel)
		if ctx.Err() != nil {
			q.cancel()
		}
		q.notifyLocked()
	}
	q.mu.Unlock()
	if err := q.Wait(ctx); err != nil {
		if first {
			q.cancel()
		}
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.drainErr
}

// Wait observes actual exit of all workers and started ProcessBatch calls. It
// neither stops admission nor cancels work. A Context error ends only this wait;
// another Wait may continue after a Drain timeout. Nil means dependencies are no
// longer in use, not that database cancellation or executor exit is complete.
func (q *Queue) Wait(ctx context.Context) error {
	if ctx == nil {
		return errors.New("instance stop wait context must not be nil")
	}
	select {
	case <-q.done:
		return nil
	default:
	}
	select {
	case <-q.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *Queue) work() {
	for {
		id := q.take()
		if id == 0 {
			return
		}
		more, err := q.process(id)
		q.complete(id, err == nil && more)
		if err != nil && !errors.Is(err, repository.ErrInstanceLocked) {
			// Driver diagnostics and panic values can contain private content.
			// Parent-lock contention is expected; it is not a technical failure.
			logx.Errorw("job instance stop processing failed", logx.Field("instance_id", id))
		}
	}
}

func (q *Queue) take() int64 {
	for {
		q.mu.Lock()
		if q.runCtx.Err() != nil || (q.stopping && len(q.entries) == 0) {
			q.mu.Unlock()
			return 0
		}
		if position := q.pending.Front(); position != nil {
			id := q.pending.Remove(position).(int64)
			q.mu.Unlock()
			return id
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-changed:
		case <-q.runCtx.Done():
		}
	}
}

func (q *Queue) process(id int64) (more bool, err error) {
	ctx, cancel := q.processingContext()
	defer cancel()
	defer func() {
		if recover() != nil {
			more, err = false, errProcessorPanicked
		}
	}()
	until := time.Now().Add(q.config.RunTimeout)
	for batch := 0; batch < q.config.MaxBatches; batch++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		result, err := q.processor.ProcessBatch(ctx, id)
		if err != nil {
			return false, err
		}
		if !result.More {
			return false, nil
		}
		// The time allowance yields only between confirmed batches. Canceling
		// an active transaction here would turn fair rotation into a failure.
		if !time.Now().Before(until) {
			return true, nil
		}
	}
	return true, nil
}

func (q *Queue) processingContext() (context.Context, context.CancelFunc) {
	q.mu.Lock()
	drainCtx := q.drainCtx
	q.mu.Unlock()
	if drainCtx != nil {
		if deadline, ok := drainCtx.Deadline(); ok {
			return context.WithDeadline(q.runCtx, deadline)
		}
	}
	return context.WithCancel(q.runCtx)
}

func (q *Queue) complete(id int64, more bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Completion and Hint use the same lock. A hint before this release only
	// merges; one arriving after it creates a new entry that cannot be erased
	// by the previous turn. Successful rotation retains the existing slot.
	if more && q.runCtx.Err() == nil {
		q.pending.PushBack(id)
	} else {
		delete(q.entries, id)
		q.fullLogged = false
	}
	q.notifyLocked()
}

func (q *Queue) finish() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.drainCtx != nil && q.drainCtx.Err() != nil {
		q.drainErr = q.drainCtx.Err()
	}
	if len(q.entries) != 0 && q.drainErr == nil {
		q.drainErr = q.runCtx.Err()
		if q.drainErr == nil {
			q.drainErr = errors.New("instance stop workers exited with pending hints")
		}
	}
	q.pending.Init()
	clear(q.entries)
	if q.stopDrain != nil {
		q.stopDrain()
	}
	q.cancel()
	q.completed = true
	close(q.done)
}

func (q *Queue) notifyLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}

func nilProcessor(value processor) bool {
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
