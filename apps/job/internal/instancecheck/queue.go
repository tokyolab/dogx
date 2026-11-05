package instancecheck

import (
	"container/list"
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

const mergeWindow = 100 * time.Millisecond

var errCheckPanicked = errors.New("instance checker panicked")

// checker reads persisted completion facts and performs one bounded progression.
// Check must support concurrent calls for different IDs and return only after its
// database work and cleanup actually finish. Only OutcomeTerminal with a nil
// error confirms that pending follow-up hints may be discarded. A zero-row write,
// cancellation, or error does not establish a terminal parent or a rollback.
// Queue supplies the complete processing budget; Check must not restart it for
// individual operations. The host owns the checker's shared dependencies.
type checker interface {
	Check(context.Context, int64) (Outcome, error)
}

// Config controls this process's unique retained IDs and check concurrency.
// Capacity includes merging, waiting and in-flight IDs. Concurrency bounds actual
// Check calls, including calls still cleaning up after cancellation. Timeout starts
// when a worker begins processing, excluding merge and queue waits. All three
// values must be positive; the host supplies the deployment defaults.
type Config struct {
	Capacity    int
	Concurrency int
	Timeout     time.Duration
}

// Queue coalesces volatile completion hints by Instance ID. A first hint becomes
// eligible after 100 milliseconds; later waiting hints do not postpone it. Hints
// received during a check retain one subsequent check in the same capacity slot,
// unless the running check confirms a terminal parent. Errors alone never retry.
//
// All methods support concurrent calls. Start is single-use. Stop closes hint
// admission but allows already-admitted checks to drain; Drain applies the host's
// existing shutdown Context. Wait observes actual completion before dependencies
// may be released. The queue owns no database or RPC connections.
type Queue struct {
	checker checker
	config  Config
	runCtx  context.Context
	cancel  context.CancelFunc
	done    chan struct{}

	mu         sync.Mutex
	started    bool
	stopping   bool
	completed  bool
	fullLogged bool
	entries    map[int64]*checkEntry
	pending    list.List
	changed    chan struct{}
	drainCtx   context.Context
	stopDrain  func() bool
	drainErr   error
}

// checkEntry retains one capacity slot while waiting or processing. nextDue is
// set only by the first hint during a Check; it is not a failure retry deadline.
type checkEntry struct {
	id         int64
	due        time.Time
	processing bool
	nextDue    time.Time
}

// NewQueue validates dependencies and configuration without starting workers or
// accepting hints. Start must precede admission. The host must eventually Stop
// and Wait, using Drain to impose its shared shutdown budget when needed.
func NewQueue(checker checker, config Config) (*Queue, error) {
	if nilChecker(checker) {
		return nil, errors.New("instance checker must not be nil")
	}
	if config.Capacity <= 0 || config.Concurrency <= 0 || config.Timeout <= 0 {
		return nil, errors.New("instance check queue settings must be positive")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Queue{checker: checker, config: config, runCtx: ctx, cancel: cancel,
		done: make(chan struct{}), entries: make(map[int64]*checkEntry), changed: make(chan struct{})}, nil
}

// Start opens admission and starts a fixed set of workers. A previous Start or
// Stop prevents reuse; repeated calls do not create more workers.
func (q *Queue) Start() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.started || q.stopping {
		return errors.New("instance check queue has already started or stopped")
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

// Hint immediately admits or merges an Instance ID without waiting for capacity
// or a check result. False means an invalid ID, unavailable admission or a full
// set of different IDs. Existing IDs still merge when full. A true result is only
// an in-memory hint, not evidence of completion or durable delivery.
//
// Waiting hints preserve their original due time. The first hint during a check
// records a new 100-millisecond window; subsequent hints cannot extend it. No
// caller Context is retained, so returning or canceled RPCs do not cancel checks.
func (q *Queue) Hint(id int64) bool {
	logFull := false
	q.mu.Lock()
	defer func() {
		q.mu.Unlock()
		if logFull {
			// Suppress repeated drops until a retained ID actually releases its
			// position. Logging must not hold the admission or completion lock.
			logx.Errorw("job instance check hint dropped because the queue is full", logx.Field("instance_id", id))
		}
	}()
	if id <= 0 || !q.started || q.stopping {
		return false
	}
	if q.mergeLocked(id) {
		return true
	}
	if len(q.entries) == q.config.Capacity {
		logFull, q.fullLogged = !q.fullLogged, true
		return false
	}
	q.addLocked(id)
	return true
}

// WaitRecoveryCapacity waits until retained IDs occupy less than half of total
// capacity, rounded down with a minimum threshold of one. Discovery calls it
// before another candidate query, without holding a connection or transaction.
// It reserves no space: normal hints may consume capacity immediately afterward.
// Cancellation ends only this wait. Closed or not-yet-started admission fails.
func (q *Queue) WaitRecoveryCapacity(ctx context.Context) error {
	return q.waitRecovery(ctx, 0)
}

// HintRecovery admits a positive recovery ID under the shared half-capacity
// threshold, waiting within ctx when a new position is unavailable. Existing IDs
// merge even at full capacity; a hint during a check retains the same follow-up
// as Hint. No separate recovery quota or processing pool is maintained.
//
// Nil confirms admission or merge, not completion. The caller advances its scan
// cursor only after nil. The Context is not retained by admitted work; cancellation
// of discovery cannot revoke a check. Waiting creates no additional goroutine.
func (q *Queue) HintRecovery(ctx context.Context, id int64) error {
	if id <= 0 {
		return errors.New("instance check recovery ID must be positive")
	}
	return q.waitRecovery(ctx, id)
}

func (q *Queue) waitRecovery(ctx context.Context, id int64) error {
	if ctx == nil {
		return errors.New("instance check recovery context must not be nil")
	}
	for {
		q.mu.Lock()
		if err := ctx.Err(); err != nil {
			q.mu.Unlock()
			return err
		}
		if !q.started || q.stopping || q.runCtx.Err() != nil {
			q.mu.Unlock()
			return errors.New("instance check recovery admission is unavailable")
		}
		if id > 0 && q.mergeLocked(id) {
			q.mu.Unlock()
			return nil
		}
		if len(q.entries) < max(1, q.config.Capacity/2) {
			if id > 0 {
				q.addLocked(id)
			}
			q.mu.Unlock()
			return nil
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (q *Queue) mergeLocked(id int64) bool {
	entry := q.entries[id]
	if entry == nil {
		return false
	}
	if entry.processing && entry.nextDue.IsZero() {
		entry.nextDue = time.Now().Add(mergeWindow)
	}
	return true
}

func (q *Queue) addLocked(id int64) {
	entry := &checkEntry{id: id, due: time.Now().Add(mergeWindow)}
	q.entries[id] = entry
	q.pending.PushBack(entry)
	q.notifyLocked()
}

// Stop closes new hint admission without canceling admitted checks or their merge
// waits. Repeated calls are safe. Stop before Start completes the unused queue;
// otherwise Wait observes the actual draining workers. No dependencies are closed.
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

// Drain closes admission and waits with the host's existing non-nil shutdown
// Context. The first call installs that budget for all workers; later calls only
// wait and cannot extend it. Expiration cancels in-flight checks and abandons
// remaining hints, but cannot force an uncooperative call to return. Wait remains
// the completion barrier before the host releases shared dependencies.
// Individual check failures are logged; nil does not mean the parents completed.
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

// Wait waits for every worker and started Check call to return. It does not stop
// admission, cancel work or grant another budget. Nil confirms no dependency use
// remains, not that the parents reached terminal states. A Context error stops
// only this wait; another call may continue waiting after a Drain timeout.
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
		ctx, cancel := q.checkContext()
		outcome, err := q.check(ctx, entry.id)
		cancel()
		q.complete(entry, err == nil && outcome == OutcomeTerminal)
		if err != nil {
			// Driver errors and panic values can include private data. A failed
			// check is observable without logging either or inventing a retry.
			logx.Errorw("job instance check failed", logx.Field("instance_id", entry.id))
		}
	}
}

func (q *Queue) take() *checkEntry {
	for {
		q.mu.Lock()
		if q.runCtx.Err() != nil || (q.stopping && len(q.entries) == 0) {
			q.mu.Unlock()
			return nil
		}
		changed := q.changed
		now := time.Now()
		var earliest time.Time
		for position := q.pending.Front(); position != nil; position = position.Next() {
			entry := position.Value.(*checkEntry)
			if !entry.due.After(now) {
				q.pending.Remove(position)
				entry.processing = true
				q.mu.Unlock()
				return entry
			}
			if earliest.IsZero() || entry.due.Before(earliest) {
				earliest = entry.due
			}
		}
		q.mu.Unlock()
		if earliest.IsZero() {
			select {
			case <-changed:
			case <-q.runCtx.Done():
			}
			continue
		}
		timer := time.NewTimer(time.Until(earliest))
		select {
		case <-timer.C:
		case <-changed:
		case <-q.runCtx.Done():
		}
		timer.Stop()
	}
}

func (q *Queue) checkContext() (context.Context, context.CancelFunc) {
	q.mu.Lock()
	drainCtx := q.drainCtx
	q.mu.Unlock()
	deadline := time.Now().Add(q.config.Timeout)
	if drainCtx != nil {
		if sharedDeadline, ok := drainCtx.Deadline(); ok && sharedDeadline.Before(deadline) {
			deadline = sharedDeadline
		}
	}
	return context.WithDeadline(q.runCtx, deadline)
}

func (q *Queue) check(ctx context.Context, id int64) (outcome Outcome, err error) {
	if err := ctx.Err(); err != nil {
		return OutcomeSkipped, err
	}
	defer func() {
		if recover() != nil {
			outcome, err = OutcomeSkipped, errCheckPanicked
		}
	}()
	return q.checker.Check(ctx, id)
}

func (q *Queue) complete(entry *checkEntry, terminal bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Completion and Hint share this lock. A concurrent event either becomes
	// this entry's follow-up or creates a new entry after deletion; old cleanup
	// cannot erase a hint that was already admitted to the still-running check.
	if !terminal && !entry.nextDue.IsZero() && q.runCtx.Err() == nil {
		entry.processing = false
		entry.due, entry.nextDue = entry.nextDue, time.Time{}
		// Keep the original capacity slot, but yield to other eligible IDs.
		// An elapsed due time must not move this entry ahead of the queue tail.
		q.pending.PushBack(entry)
	} else {
		delete(q.entries, entry.id)
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
			q.drainErr = errors.New("instance check workers exited with pending hints")
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

func nilChecker(value checker) bool {
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
