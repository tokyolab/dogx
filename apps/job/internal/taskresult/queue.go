package taskresult

import (
	"container/heap"
	"context"
	"errors"
	"math/rand/v2"
	"reflect"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

const (
	// DefaultQueueCapacity includes waiting, backing-off and in-flight reports.
	DefaultQueueCapacity = 512
	// DefaultSaveConcurrency bounds active saves without reserving DB connections.
	DefaultSaveConcurrency = 8
)

// ErrNotReady means result admission is unavailable before startup or after
// stopping. A full running buffer instead returns false, nil from Accept.
var ErrNotReady = errors.New("task result queue is not ready")

var errSavePanicked = errors.New("task result saver panicked")

// ResultSaver processes an immutable report under the supplied worker Context.
// Implementations must support concurrent calls, bound each attempt, and return
// only after that attempt has finished using shared dependencies. Nil error
// permits releasing the report; every error requires retaining the same report,
// even when SaveResult claims a write was applied. Cancellation does not establish
// rollback. Errors and panic values may contain private data and are not logged.
type ResultSaver interface {
	Save(context.Context, Report) (SaveResult, error)
}

// QueueConfig controls this process's result retention and save concurrency.
// Capacity includes queued, backing-off and in-flight reports. Concurrency counts
// active Save calls, not retained reports. Zero selects the documented defaults;
// negative values are invalid. The Saver owns each attempt's processing budget.
type QueueConfig struct {
	Capacity    int
	Concurrency int
}

// Queue owns bounded, volatile result retention and concurrent save workers.
// Failed attempts remain in their original retention positions but release their
// workers during backoff. Reports are not deduplicated, expired or retried through
// an unbounded side queue. Memory retention is measured in records, not bytes.
//
// All methods support concurrent calls. Start is single-use; Stop closes admission
// and Drain applies the host's existing shutdown Context. A Drain timeout is not
// proof that workers exited: Wait is the actual completion barrier required before
// closing the saver dependencies. Queue owns no database or RPC connections.
type Queue struct {
	saver  ResultSaver
	config QueueConfig
	runCtx context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu        sync.Mutex
	started   bool
	stopping  bool
	completed bool
	retained  int
	sequence  uint64
	pending   resultHeap
	changed   chan struct{}
	drainCtx  context.Context
	stopDrain func() bool
	drainErr  error
}

// retainedResult stays owned by Queue until a nil save error or final shutdown.
// Removing it from pending only marks an in-flight attempt; retained is unchanged.
type retainedResult struct {
	report   Report
	due      time.Time
	sequence uint64
	failures uint8
}

// NewQueue validates dependencies without starting workers or accepting reports.
// The caller must Start before exposing admission and eventually Stop and Drain.
func NewQueue(saver ResultSaver, config QueueConfig) (*Queue, error) {
	if nilResultSaver(saver) {
		return nil, errors.New("task result saver is nil")
	}
	if config.Capacity < 0 || config.Concurrency < 0 {
		return nil, errors.New("task result queue settings must not be negative")
	}
	if config.Capacity == 0 {
		config.Capacity = DefaultQueueCapacity
	}
	if config.Concurrency == 0 {
		config.Concurrency = DefaultSaveConcurrency
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Queue{saver: saver, config: config, runCtx: ctx, cancel: cancel,
		done: make(chan struct{}), changed: make(chan struct{})}, nil
}

// Start publishes admission and starts a fixed set of workers. It returns an
// error after any previous Start or Stop; stopped queues cannot be restarted.
func (q *Queue) Start() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.started || q.stopping {
		return errors.New("task result queue has already started or stopped")
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

// Accept immediately admits a report or refuses it; it never waits for capacity
// or persistence. True, nil transfers its immutable buffers to Queue, independently
// of this Context's later cancellation. False, nil means full; errors mean no
// admission. The caller owns rejected reports. Authentication and wire validation
// belong at the boundary; persisted ownership remains the Saver's responsibility.
func (q *Queue) Accept(ctx context.Context, report Report) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !q.started || q.stopping {
		return false, ErrNotReady
	}
	if q.retained == q.config.Capacity {
		return false, nil
	}
	q.sequence++
	heap.Push(&q.pending, &retainedResult{report: report, due: time.Now(), sequence: q.sequence})
	q.retained++
	q.notifyLocked()
	return true, nil
}

// Stop atomically closes admission without canceling accepted saves or backoff.
// Concurrent admissions are either fully retained before Stop or refused after it.
// Repeated calls are safe. With unavailable storage, Drain must supply the host's
// finite shutdown Context; Stop alone does not impose a result retention limit.
func (q *Queue) Stop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopping = true
	q.notifyLocked()
	if !q.started && !q.completed {
		q.completed = true
		q.cancel()
		close(q.done)
	}
}

// Drain closes admission and waits for retained reports to finish. The first call
// supplies the host's shared shutdown Context, canceling in-flight saves and future
// retries when it expires; no new grace period is created. Later callers only wait
// with their own Contexts and cannot extend or replace that shared budget.
//
// Nil means all reports were released and workers exited. On timeout or cancellation
// unfinished reports may be lost at process exit. A saver ignoring cancellation
// can outlive Drain; call Wait to observe its actual return before releasing shared
// dependencies. No method can forcibly terminate a Go goroutine.
func (q *Queue) Drain(ctx context.Context) error {
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

// Wait observes actual worker completion, including saves that ignore cancellation.
// It does not stop admission, cancel workers or grant more processing time. Nil
// confirms dependency use has ended, not that every report was saved; Drain reports
// incomplete shutdown. Concurrent calls are safe, including after a Drain timeout.
func (q *Queue) Wait(ctx context.Context) error {
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
		entry := q.take()
		if entry == nil {
			return
		}
		q.mu.Lock()
		drainCtx := q.drainCtx
		q.mu.Unlock()
		ctx, cancel := context.WithCancel(q.runCtx)
		if drainCtx != nil {
			if deadline, ok := drainCtx.Deadline(); ok {
				cancel()
				ctx, cancel = context.WithDeadline(q.runCtx, deadline)
			}
		}
		err := ctx.Err()
		if err == nil {
			err = q.save(ctx, entry.report)
		}
		cancel()
		finishedAt := time.Now()
		var retryDelay time.Duration
		q.mu.Lock()
		if err == nil {
			q.retained--
		} else if q.runCtx.Err() == nil {
			if entry.failures < 6 {
				entry.failures++
			}
			retryDelay = saveRetryDelay(entry.failures)
			entry.due = finishedAt.Add(retryDelay)
			heap.Push(&q.pending, entry)
		}
		q.notifyLocked()
		q.mu.Unlock()
		if retryDelay > 0 {
			// Driver errors and panic values may embed result payloads. Only log
			// platform classifications and report identities, never the raw cause.
			// Synchronous logging must not hold the admission lock.
			logx.Errorw("job task result save will retry",
				logx.Field("job_id", entry.report.JobID), logx.Field("instance_id", entry.report.InstanceID),
				logx.Field("task_id", entry.report.TaskID), logx.Field("attempt_no", entry.report.AttemptNo),
				logx.Field("reason", saveFailureReason(err)), logx.Field("retry_delay", retryDelay.String()))
		}
	}
}

func (q *Queue) take() *retainedResult {
	for {
		q.mu.Lock()
		if q.runCtx.Err() != nil || (q.stopping && q.retained == 0) {
			q.mu.Unlock()
			return nil
		}
		changed := q.changed
		var delay time.Duration
		if len(q.pending) > 0 {
			delay = time.Until(q.pending[0].due)
			if delay <= 0 {
				entry := heap.Pop(&q.pending).(*retainedResult)
				q.mu.Unlock()
				return entry
			}
		}
		q.mu.Unlock()
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-changed:
			case <-q.runCtx.Done():
			}
			timer.Stop()
		} else {
			select {
			case <-changed:
			case <-q.runCtx.Done():
			}
		}
	}
}

func (q *Queue) save(ctx context.Context, report Report) (err error) {
	defer func() {
		if recover() != nil {
			// A platform panic must not remove an already acknowledged report or
			// silently lose a worker. Retain it like other platform failures.
			err = errSavePanicked
		}
	}()
	_, err = q.saver.Save(ctx, report)
	return err
}

func (q *Queue) finish() {
	q.mu.Lock()
	unsaved := q.retained
	if q.retained != 0 {
		q.drainErr = q.runCtx.Err()
		if q.drainCtx != nil && q.drainCtx.Err() != nil {
			q.drainErr = q.drainCtx.Err()
		}
		if q.drainErr == nil {
			q.drainErr = errors.New("task result workers exited with unsaved reports")
		}
	}
	q.pending = nil
	q.retained = 0
	q.cancel()
	q.mu.Unlock()
	if unsaved != 0 {
		logx.Errorw("job result queue stopped before all reports were saved", logx.Field("unsaved_results", unsaved))
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopDrain != nil {
		q.stopDrain()
	}
	q.completed = true
	close(q.done)
}

func nilResultSaver(saver ResultSaver) bool {
	if saver == nil {
		return true
	}
	value := reflect.ValueOf(saver)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (q *Queue) notifyLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}

func saveRetryDelay(failures uint8) time.Duration {
	steps := [...]time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second}
	index := min(max(int(failures)-1, 0), len(steps)-1)
	return time.Duration(float64(steps[index]) * (0.8 + rand.Float64()*0.4))
}

func saveFailureReason(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "save_timeout"
	case errors.Is(err, context.Canceled):
		return "save_canceled"
	case errors.Is(err, ErrInvalidExecutionConfig):
		return "invalid_execution_config"
	case errors.Is(err, ErrInvalidStoredState):
		return "invalid_stored_state"
	case errors.Is(err, ErrUnsupportedTaskType):
		return "unsupported_task_type"
	case errors.Is(err, errSavePanicked):
		return "save_panic"
	default:
		return "storage_error"
	}
}

// resultHeap orders pending attempts by local due time, then original admission.
// In-flight entries are absent, preventing concurrent selection of the same entry.
type resultHeap []*retainedResult

func (h resultHeap) Len() int { return len(h) }
func (h resultHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].sequence < h[j].sequence
	}
	return h[i].due.Before(h[j].due)
}
func (h resultHeap) Swap(i, j int)   { h[i], h[j] = h[j], h[i] }
func (h *resultHeap) Push(value any) { *h = append(*h, value.(*retainedResult)) }
func (h *resultHeap) Pop() any {
	last := len(*h) - 1
	entry := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return entry
}
