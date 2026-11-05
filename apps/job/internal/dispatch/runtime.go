package dispatch

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	dispatchPageLimit   = 128
	dispatchPageBudget  = 4
	dispatchParentShare = 8
	waitingScan         = 0
	retryScan           = 1
)

var errScanPaused = errors.New("dispatch candidate scan paused for contention")

// waitingSource reads first-dispatch candidates and database time without claiming
// work. MaxWaitingID returns zero for an empty set. ListWaitingIDs returns at most
// limit ascending positive IDs strictly after cursor and at most upperID. Queries
// do not filter by execution mode. DatabaseTime fixes a retry traversal's horizon;
// registration must independently read time after acquiring its parent lock.
// Methods honor ctx and return only after actual database work has ended.
type waitingSource interface {
	MaxWaitingID(context.Context) (int64, error)
	ListWaitingIDs(context.Context, int64, int64, int) ([]int64, error)
	DatabaseTime(context.Context) (time.Time, error)
}

// runtimeTasks shares response persistence and retry discovery without owning the
// connection. Retry queries return original attempt identities in ascending
// (NextDispatchAt, TaskID) order, after cursor and no later than scanNow. A nil
// cursor starts a traversal. Queries neither claim work nor filter Task types.
type runtimeTasks interface {
	responseStore
	ListRetryCandidates(context.Context, time.Time, *repository.RetryCursor, int) ([]repository.RetryCandidate, error)
}

// RuntimeConfig bounds this process's shared first/retry dispatch supply. All
// values must be positive. WaitingCapacity includes query reservations, temporary
// pages and hints; workers are separately bounded by Concurrency. ScanInterval is
// the idle/no-progress interval before independent 20% jitter. QueryTimeout bounds
// each source call. TotalTimeout and BusyDelay configure Dispatcher.
type RuntimeConfig struct {
	Concurrency     int
	WaitingCapacity int
	ScanInterval    time.Duration
	QueryTimeout    time.Duration
	TotalTimeout    time.Duration
	BusyDelay       time.Duration
}

// Runtime supplies waiting Instances and due retry Tasks through one scanner,
// one bounded waiting set and fixed workers. Candidate identity deduplication
// lasts through the actual dispatch return; per-parent registration coordination
// ends at the transaction return, before RPC or response persistence.
// All methods are concurrency-safe. Start is single-use. Stop closes supply;
// Drain applies the host's existing shutdown budget to already-started work.
// Wait observes actual completion before shared dependencies may be released.
type Runtime struct {
	source     waitingSource
	tasks      runtimeTasks
	dispatcher *Dispatcher
	config     RuntimeConfig
	scanCtx    context.Context
	cancelScan context.CancelFunc
	workCtx    context.Context
	cancelWork context.CancelFunc
	done       chan struct{}

	mu             sync.Mutex
	started        bool
	stopping       bool
	completed      bool
	waiting        int
	reserved       int
	pending        []waitingCandidate
	owned          map[candidateKey]uint64
	registering    map[int64]waitingCandidate
	lastParent     int64
	parentShare    int
	changed        chan struct{}
	sequence       uint64
	scanGeneration [2]uint64
	pagesLeft      int
	budgetResumeAt time.Time
	conflicted     map[int64]time.Time
	conflictUntil  time.Time
	conflictFull   bool
	drainCtx       context.Context
	stopDrain      func() bool
	drainErr       error
}

type candidateKey struct{ instanceID, taskID, attemptNo int64 }

type waitingCandidate struct {
	sequence       uint64
	instanceID     int64
	retry          repository.RetryCandidate
	scanKind       int
	scanGeneration uint64
}

func (c waitingCandidate) key() candidateKey {
	if c.retry.TaskID != 0 {
		return candidateKey{taskID: c.retry.TaskID, attemptNo: c.retry.AttemptNo}
	}
	return candidateKey{instanceID: c.instanceID}
}

// candidateScan holds only one source's temporary traversal. A source reaching
// its tail waits independently, so it cannot stop the other source's progress.
type candidateScan struct {
	upperID     int64
	cursor      int64
	scanNow     time.Time
	retryCursor *repository.RetryCursor
	generation  uint64
	readyAt     time.Time
}

// runtimeRegistrar reports confirmed progress and releases the parent gate at
// the actual registration return. A parent lock conflict suppresses that current
// group briefly; it does not undo other already-authorized sends.
type runtimeRegistrar struct {
	runtime   *Runtime
	registrar ordinaryRegistrar
}

func (r runtimeRegistrar) StartOrdinary(ctx context.Context, id int64) (attempt *RegisteredAttempt, err error) {
	candidate, ok := r.runtime.registrationCandidate(id)
	if !ok {
		return nil, nil
	}
	defer func() { r.runtime.registrationFinished(candidate, attempt != nil && err == nil, err) }()
	return r.registrar.StartOrdinary(ctx, id)
}

func (r runtimeRegistrar) RetryOrdinary(ctx context.Context, retry repository.RetryCandidate) (attempt *RegisteredAttempt, err error) {
	candidate, ok := r.runtime.registrationCandidate(retry.InstanceID)
	if !ok {
		return nil, nil
	}
	defer func() { r.runtime.registrationFinished(candidate, attempt != nil && err == nil, err) }()
	return r.registrar.RetryOrdinary(ctx, retry)
}

func (r *Runtime) registrationCandidate(id int64) (waitingCandidate, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	candidate, ok := r.registering[id]
	r.expireConflictsLocked(time.Now())
	// take already admitted this bounded registration before any subsequent
	// contention pause. Its conflict-record space is included in Concurrency.
	return candidate, ok && !r.stopping
}

// NewRuntime validates dependencies without querying or starting workers. The
// host supplies shared repositories and owns their connections through Wait.
// instanceHint optionally enqueues completion after confirmed response persistence;
// it must be concurrent, nonblocking and must not perform completion inline.
func NewRuntime(source waitingSource, registrar ordinaryRegistrar, sender Sender, tasks runtimeTasks, config RuntimeConfig, instanceHint func(int64)) (*Runtime, error) {
	if nilDependency(source) || nilDependency(registrar) || nilDependency(tasks) {
		return nil, errors.New("dispatch runtime dependencies must not be nil")
	}
	if config.Concurrency <= 0 || config.WaitingCapacity <= 0 || config.ScanInterval <= 0 || config.QueryTimeout <= 0 || config.TotalTimeout <= 0 || config.BusyDelay <= 0 {
		return nil, errors.New("dispatch runtime settings must be positive")
	}
	if config.ScanInterval > time.Duration(1<<63-1)/6*5 || config.Concurrency > int(^uint(0)>>1)-config.WaitingCapacity {
		return nil, errors.New("dispatch runtime settings are too large")
	}
	r := &Runtime{source: source, tasks: tasks, config: config, done: make(chan struct{}),
		owned: make(map[candidateKey]uint64), registering: make(map[int64]waitingCandidate),
		conflicted: make(map[int64]time.Time), changed: make(chan struct{}), pagesLeft: dispatchPageBudget}
	dispatcher, err := NewDispatcher(runtimeRegistrar{runtime: r, registrar: registrar}, sender, tasks,
		WithDispatchTiming(config.TotalTimeout, config.BusyDelay), WithInstanceHint(instanceHint))
	if err != nil {
		return nil, err
	}
	r.dispatcher = dispatcher
	r.scanCtx, r.cancelScan = context.WithCancel(context.Background())
	r.workCtx, r.cancelWork = context.WithCancel(context.Background())
	return r, nil
}

// Start opens admission and starts one serial scanner and fixed workers. A prior
// Start or Stop prevents reuse. It does not wait for successful storage discovery.
func (r *Runtime) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopping {
		return errors.New("dispatch runtime has already started or stopped")
	}
	r.started = true
	var workers sync.WaitGroup
	workers.Go(r.scan)
	for range r.config.Concurrency {
		workers.Go(r.work)
	}
	go func() { workers.Wait(); r.finish() }()
	return nil
}

// Hint immediately admits or merges one first-dispatch Instance. False means
// invalid input, unavailable admission, a recent parent conflict or no waiting
// space. It retains no caller Context and gives scanning no progress credit.
// Retries are discovered from durable Task candidates, not inferred from a hint.
func (r *Runtime) Hint(instanceID int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if instanceID <= 0 || !r.started || r.stopping {
		return false
	}
	r.expireConflictsLocked(time.Now())
	key := candidateKey{instanceID: instanceID}
	if _, ok := r.owned[key]; ok {
		return true
	}
	if _, blocked := r.conflicted[instanceID]; blocked {
		return false
	}
	if r.waiting == r.config.WaitingCapacity {
		return false
	}
	r.sequence++
	candidate := waitingCandidate{sequence: r.sequence, instanceID: instanceID}
	r.pending = append(r.pending, candidate)
	r.owned[key] = candidate.sequence
	r.waiting++
	r.notifyLocked()
	return true
}

// Stop closes hints, drops unregistered waiting candidates and cancels scans.
// Durable state remains the backlog. Already-started registration and authorized
// sends keep their independent budget for response persistence. Stop is
// nonblocking and idempotent; Stop before Start completes the unused runtime.
func (r *Runtime) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return
	}
	r.stopping = true
	for _, candidate := range r.pending {
		delete(r.owned, candidate.key())
	}
	r.waiting -= len(r.pending)
	r.pending = nil
	r.cancelScan()
	r.notifyLocked()
	if !r.started {
		r.completed = true
		r.cancelWork()
		close(r.done)
	}
}

// Drain stops supply and installs the host's existing non-nil shutdown Context
// for in-flight operations. Only the first call supplies their budget. Expiration
// cancels work but does not prove return or rollback; Wait remains the actual
// dependency-lifetime barrier. Later calls cannot extend the original budget.
func (r *Runtime) Drain(ctx context.Context) error {
	r.Stop()
	r.mu.Lock()
	if r.completed {
		err := r.drainErr
		r.mu.Unlock()
		return err
	}
	first := r.drainCtx == nil
	if first {
		r.drainCtx = ctx
		r.stopDrain = context.AfterFunc(ctx, r.cancelWork)
		if ctx.Err() != nil {
			r.cancelWork()
		}
	}
	r.mu.Unlock()
	if err := r.Wait(ctx); err != nil {
		if first {
			r.cancelWork()
		}
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.drainErr
}

// Wait observes actual scanner and worker completion without changing admission
// or processing budgets. Nil confirms dependency use ended, not business success.
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

func (r *Runtime) scan() {
	var scans [2]candidateScan
	next := waitingScan
	for {
		kind, limit, ok := r.nextPage(&scans, &next)
		if !ok {
			return
		}
		state := &scans[kind]
		page, end, err := r.readPage(kind, state, limit)
		r.mu.Lock()
		// Slow queries do not consume the idle interval itself. Start the
		// no-progress pause after the page actually returns, unless a confirmed
		// registration already restored the shared budget while it was running.
		if r.pagesLeft == 0 && r.budgetResumeAt.IsZero() {
			r.budgetResumeAt = time.Now().Add(r.scanDelay())
		}
		r.mu.Unlock()
		if err != nil {
			r.releasePage()
			if errors.Is(err, errScanPaused) {
				continue
			}
			if r.scanCtx.Err() == nil {
				logx.Error("dispatch candidate scan failed")
			}
			state.readyAt = time.Now().Add(r.scanDelay())
			continue
		}
		if !r.admitPage(page, kind, state.generation) {
			return
		}
		// All original rows were admitted, merged or legitimately skipped before
		// advancing. Local parent rotation cannot bypass an unaccepted SQL row.
		if len(page) != 0 {
			last := page[len(page)-1]
			if kind == waitingScan {
				state.cursor = last.instanceID
			} else {
				state.retryCursor = &repository.RetryCursor{NextDispatchAt: last.retry.NextDispatchAt, TaskID: last.retry.TaskID}
			}
		}
		if end {
			*state = candidateScan{readyAt: time.Now().Add(r.scanDelay())}
		}
	}
}

func (r *Runtime) nextPage(scans *[2]candidateScan, next *int) (int, int, bool) {
	for {
		r.mu.Lock()
		if r.stopping {
			r.mu.Unlock()
			return 0, 0, false
		}
		now := time.Now()
		r.expireConflictsLocked(now)
		if r.pagesLeft == 0 && !r.budgetResumeAt.IsZero() && !now.Before(r.budgetResumeAt) {
			r.pagesLeft, r.budgetResumeAt = dispatchPageBudget, time.Time{}
		}
		kind := -1
		var wakeAt time.Time
		for offset := range 2 {
			index := (*next + offset) % 2
			if !scans[index].readyAt.After(now) {
				if kind < 0 {
					kind = index
				}
			} else {
				wakeAt = earlier(wakeAt, scans[index].readyAt)
			}
		}
		if r.pagesLeft == 0 {
			wakeAt = earlier(wakeAt, r.budgetResumeAt)
		}
		if r.conflictFull {
			wakeAt = earlier(wakeAt, r.conflictUntil)
		}
		if kind >= 0 && r.pagesLeft > 0 && !r.conflictFull && r.waiting < r.config.WaitingCapacity {
			r.reserved = min(dispatchPageLimit, r.config.WaitingCapacity-r.waiting)
			r.waiting += r.reserved
			if scans[kind].generation == 0 {
				r.scanGeneration[kind]++
				scans[kind].generation = r.scanGeneration[kind]
			}
			limit := r.reserved
			*next = (kind + 1) % 2
			r.mu.Unlock()
			return kind, limit, true
		}
		changed := r.changed
		r.mu.Unlock()
		if !r.waitChange(changed, wakeAt) {
			return 0, 0, false
		}
	}
}

func (r *Runtime) readPage(kind int, state *candidateScan, limit int) ([]waitingCandidate, bool, error) {
	if kind == waitingScan {
		if state.upperID == 0 {
			ctx, cancel := context.WithTimeout(r.scanCtx, r.config.QueryTimeout)
			upper, err := r.source.MaxWaitingID(ctx)
			if err == nil {
				err = ctx.Err()
			}
			cancel()
			if err != nil {
				return nil, false, err
			}
			if upper < 0 {
				return nil, false, errors.New("waiting query returned an invalid upper bound")
			}
			if upper == 0 {
				return nil, true, nil
			}
			state.upperID = upper
		}
		if err := r.chargePage(); err != nil {
			return nil, false, err
		}
		ctx, cancel := context.WithTimeout(r.scanCtx, r.config.QueryTimeout)
		ids, err := r.source.ListWaitingIDs(ctx, state.cursor, state.upperID, limit)
		if err == nil {
			err = ctx.Err()
		}
		cancel()
		if err != nil {
			return nil, false, err
		}
		if !validWaitingPage(ids, state.cursor, state.upperID, limit) {
			return nil, false, errors.New("waiting query returned an invalid page")
		}
		page := make([]waitingCandidate, len(ids))
		for index, id := range ids {
			page[index].instanceID = id
		}
		return page, len(ids) < limit || (len(ids) != 0 && ids[len(ids)-1] == state.upperID), nil
	}
	if state.scanNow.IsZero() {
		ctx, cancel := context.WithTimeout(r.scanCtx, r.config.QueryTimeout)
		scanNow, err := r.source.DatabaseTime(ctx)
		if err == nil {
			err = ctx.Err()
		}
		cancel()
		if err != nil {
			return nil, false, err
		}
		if scanNow.IsZero() {
			return nil, false, errors.New("retry scan database time is empty")
		}
		state.scanNow = scanNow
	}
	if err := r.chargePage(); err != nil {
		return nil, false, err
	}
	ctx, cancel := context.WithTimeout(r.scanCtx, r.config.QueryTimeout)
	rows, err := r.tasks.ListRetryCandidates(ctx, state.scanNow, state.retryCursor, limit)
	if err == nil {
		err = ctx.Err()
	}
	cancel()
	if err != nil {
		return nil, false, err
	}
	if !validRetryPage(rows, state.scanNow, state.retryCursor, limit) {
		return nil, false, errors.New("retry query returned an invalid page")
	}
	page := make([]waitingCandidate, len(rows))
	for index, retry := range rows {
		page[index] = waitingCandidate{instanceID: retry.InstanceID, retry: retry}
	}
	return page, len(rows) < limit, nil
}

func (r *Runtime) chargePage() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return context.Canceled
	}
	r.expireConflictsLocked(time.Now())
	if r.conflictFull {
		return errScanPaused
	}
	r.pagesLeft--
	if r.pagesLeft == 0 {
		r.budgetResumeAt = time.Time{}
	}
	return nil
}

func (r *Runtime) releasePage() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waiting -= r.reserved
	r.reserved = 0
	r.notifyLocked()
}

func (r *Runtime) admitPage(page []waitingCandidate, kind int, generation uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waiting -= r.reserved
	r.reserved = 0
	if r.stopping {
		r.notifyLocked()
		return false
	}
	r.expireConflictsLocked(time.Now())
	groups := make(map[int64][]waitingCandidate)
	var parents []int64
	for _, candidate := range page {
		if _, ok := r.owned[candidate.key()]; ok {
			continue
		}
		if _, blocked := r.conflicted[candidate.instanceID]; blocked {
			continue
		}
		r.sequence++
		candidate.sequence, candidate.scanKind, candidate.scanGeneration = r.sequence, kind, generation
		r.owned[candidate.key()] = candidate.sequence
		if _, ok := groups[candidate.instanceID]; !ok {
			parents = append(parents, candidate.instanceID)
		}
		groups[candidate.instanceID] = append(groups[candidate.instanceID], candidate)
		r.waiting++
	}
	if len(parents) != 0 {
		start := rand.IntN(len(parents))
		// Rotate parent groups, then offer at most eight candidates per group
		// before returning to it. The temporary page already owns its capacity.
		for remaining := true; remaining; {
			remaining = false
			for offset := range len(parents) {
				id := parents[(start+offset)%len(parents)]
				group := groups[id]
				count := min(dispatchParentShare, len(group))
				r.pending = append(r.pending, group[:count]...)
				groups[id] = group[count:]
				remaining = remaining || len(group) > count
			}
		}
	}
	r.notifyLocked()
	return true
}

func (r *Runtime) work() {
	for {
		candidate, ok := r.take()
		if !ok {
			return
		}
		r.dispatch(candidate)
		// A canceled budget may bypass Registrar entirely. Sequence comparison
		// prevents this old cleanup from releasing a later parent registration.
		r.registrationFinished(candidate, false, nil)
		r.mu.Lock()
		if r.owned[candidate.key()] == candidate.sequence {
			delete(r.owned, candidate.key())
		}
		r.notifyLocked()
		r.mu.Unlock()
	}
}

func (r *Runtime) take() (waitingCandidate, bool) {
	for {
		r.mu.Lock()
		if r.stopping {
			r.mu.Unlock()
			return waitingCandidate{}, false
		}
		r.expireConflictsLocked(time.Now())
		index := -1
		if !r.conflictFull {
			for position, candidate := range r.pending {
				if _, busy := r.registering[candidate.instanceID]; busy {
					continue
				}
				if index < 0 {
					index = position
				}
				if r.parentShare >= dispatchParentShare && candidate.instanceID != r.lastParent {
					index = position
					break
				}
				if r.parentShare < dispatchParentShare {
					break
				}
			}
		}
		if index >= 0 {
			candidate := r.pending[index]
			copy(r.pending[index:], r.pending[index+1:])
			r.pending[len(r.pending)-1] = waitingCandidate{}
			r.pending = r.pending[:len(r.pending)-1]
			r.waiting--
			r.registering[candidate.instanceID] = candidate
			if r.lastParent == candidate.instanceID {
				r.parentShare = min(r.parentShare+1, dispatchParentShare)
			} else {
				r.lastParent, r.parentShare = candidate.instanceID, 1
			}
			r.notifyLocked()
			r.mu.Unlock()
			return candidate, true
		}
		changed := r.changed
		var wakeAt time.Time
		if r.conflictFull {
			wakeAt = r.conflictUntil
		}
		r.mu.Unlock()
		if !r.waitChange(changed, wakeAt) {
			return waitingCandidate{}, false
		}
	}
}

func (r *Runtime) dispatch(candidate waitingCandidate) {
	defer func() {
		if recover() != nil {
			logx.Errorw("job dispatch panicked", logx.Field("instance_id", candidate.instanceID))
		}
	}()
	var err error
	if candidate.retry.TaskID == 0 {
		_, err = r.dispatcher.DispatchOrdinary(r.workCtx, candidate.instanceID)
	} else {
		_, err = r.dispatcher.DispatchRetry(r.workCtx, candidate.retry)
	}
	if err != nil && !errors.Is(err, repository.ErrInstanceLocked) {
		// Driver and remote causes may contain private data. Neither an error
		// nor a parent-lock conflict authorizes an immediate local resend.
		logx.Errorw("job dispatch did not complete", logx.Field("instance_id", candidate.instanceID))
	}
}

func (r *Runtime) registrationFinished(candidate waitingCandidate, confirmed bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.registering[candidate.instanceID]; !ok || current != candidate {
		return
	}
	delete(r.registering, candidate.instanceID)
	if errors.Is(err, repository.ErrInstanceLocked) {
		r.blockParentLocked(candidate.instanceID)
	}
	if confirmed && candidate.scanGeneration != 0 && candidate.scanGeneration == r.scanGeneration[candidate.scanKind] {
		r.pagesLeft, r.budgetResumeAt = dispatchPageBudget, time.Time{}
	}
	r.notifyLocked()
}

func (r *Runtime) blockParentLocked(id int64) {
	now := time.Now()
	r.expireConflictsLocked(now)
	if _, exists := r.conflicted[id]; !exists {
		until := now.Add(r.scanDelay())
		r.conflicted[id] = until
		r.conflictUntil = earlier(r.conflictUntil, until)
	}
	// Pause new queries and registrations at WaitingCapacity entries, leaving
	// room for at most Concurrency already-started registrations to report their
	// own conflicts. This bounds the map without dropping late conflict IDs.
	// Each parent's first deadline is independent and never extended by repeats.
	r.conflictFull = len(r.conflicted) >= r.config.WaitingCapacity
	kept := r.pending[:0]
	for _, candidate := range r.pending {
		if candidate.instanceID == id {
			delete(r.owned, candidate.key())
			r.waiting--
		} else {
			kept = append(kept, candidate)
		}
	}
	clear(r.pending[len(kept):])
	r.pending = kept
}

func (r *Runtime) expireConflictsLocked(now time.Time) {
	if !r.conflictUntil.IsZero() && !now.Before(r.conflictUntil) {
		r.conflictUntil = time.Time{}
		for id, until := range r.conflicted {
			if !now.Before(until) {
				delete(r.conflicted, id)
			} else {
				r.conflictUntil = earlier(r.conflictUntil, until)
			}
		}
		r.conflictFull = len(r.conflicted) >= r.config.WaitingCapacity
		r.notifyLocked()
	}
}

func (r *Runtime) waitChange(changed <-chan struct{}, deadline time.Time) bool {
	if deadline.IsZero() {
		select {
		case <-changed:
			return true
		case <-r.scanCtx.Done():
			return false
		}
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-changed:
		return true
	case <-timer.C:
		return true
	case <-r.scanCtx.Done():
		return false
	}
}

func (r *Runtime) scanDelay() time.Duration {
	base := r.config.ScanInterval
	return base - base/5 + time.Duration(rand.Int64N(int64(base/5*2)+1))
}

func (r *Runtime) finish() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.drainCtx != nil && r.drainCtx.Err() != nil {
		r.drainErr = r.drainCtx.Err()
	}
	if r.stopDrain != nil {
		r.stopDrain()
	}
	r.cancelScan()
	r.cancelWork()
	r.completed = true
	close(r.done)
}

func (r *Runtime) notifyLocked() { close(r.changed); r.changed = make(chan struct{}) }

func earlier(a, b time.Time) time.Time {
	if a.IsZero() || b.Before(a) {
		return b
	}
	return a
}

func validWaitingPage(ids []int64, cursor, upperID int64, limit int) bool {
	if len(ids) > limit {
		return false
	}
	for _, id := range ids {
		if id <= cursor || id > upperID {
			return false
		}
		cursor = id
	}
	return true
}

func validRetryPage(rows []repository.RetryCandidate, scanNow time.Time, cursor *repository.RetryCursor, limit int) bool {
	if len(rows) > limit {
		return false
	}
	for _, row := range rows {
		if row.TaskID <= 0 || row.InstanceID <= 0 || row.AttemptNo <= 0 || row.NextDispatchAt.IsZero() || row.NextDispatchAt.After(scanNow) {
			return false
		}
		if cursor != nil && (row.NextDispatchAt.Before(cursor.NextDispatchAt) || (row.NextDispatchAt.Equal(cursor.NextDispatchAt) && row.TaskID <= cursor.TaskID)) {
			return false
		}
		cursor = &repository.RetryCursor{NextDispatchAt: row.NextDispatchAt, TaskID: row.TaskID}
	}
	return true
}
