package execution

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// Notification identifies a newly committed Instance stop decision. The caller
// must confirm that decision before Hint; reading an existing stopping state or
// recovering database cancellation does not authorize another notification pass.
type Notification struct {
	InstanceID      int64
	ExecutorGroupID int64
	Reason          string
}

// notificationSource supplies one bounded page of distinct saved addresses,
// including terminal Tasks. Results follow the database column's collation and
// are strictly after the given cursor; callers must not reorder them in Go.
// Empty results end enumeration. Errors return no usable page. Implementations
// honor ctx and return only after the query and its cleanup actually finish.
type notificationSource interface {
	ListCancelTargets(ctx context.Context, instanceID int64, after string, limit int) ([]string, error)
}

// notificationSender sends to one saved address with bounded same-target
// retries. Cancel supports concurrent calls, respects ctx and returns only after
// actual dependency cleanup. A nil error does not prove business execution exited.
type notificationSender interface {
	Cancel(context.Context, CancelTarget) error
}

// NotificationConfig bounds this process's best-effort notification work.
// InstanceCapacity includes queued, enumerating and still-sending Instances.
// TargetCapacity includes queued, active and retrying targets. Concurrency bounds
// sender calls, including cleanup after cancellation. PageSize is in [1, 32];
// the reader retains at most this one page outside TargetCapacity. QueryTimeout
// covers one query and InstanceTimeout starts at admission, including all waits.
// All values must be positive; the host supplies deployment defaults.
type NotificationConfig struct {
	InstanceCapacity int
	PageSize         int
	TargetCapacity   int
	Concurrency      int
	QueryTimeout     time.Duration
	InstanceTimeout  time.Duration
}

// Notifier enumerates saved addresses and sends cancellation independently of
// database stop processing. One reader rotates Instances after each page; fixed
// sender workers share bounded target storage. Local duplicates merge only while
// retained and never extend the original deadline or request a follow-up pass.
// There is no persistent delivery registry, status polling or reliable resend.
//
// Methods support concurrent calls. Start is single-use. Stop closes admission
// and drains accepted work; Drain uses the host's existing shutdown budget.
// Context cancellation requests dependency exit but does not release a running
// call's capacity. Wait is the actual completion barrier before shared database
// and transport dependencies can be closed. The host owns those dependencies.
type Notifier struct {
	source notificationSource
	sender notificationSender
	config NotificationConfig
	runCtx context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu          sync.Mutex
	started     bool
	stopping    bool
	completed   bool
	fullLogged  bool
	entries     map[int64]*notificationEntry
	enumerating list.List
	targets     list.List
	targetCount int
	changed     chan struct{}
	drainCtx    context.Context
	stopDrain   func() bool
	drainErr    error
}

// notificationEntry retains ownership through actual query/sender return.
// The cursor advances only when a target is admitted, not when its page is read.
type notificationEntry struct {
	notification Notification
	ctx          context.Context
	cancel       context.CancelFunc
	deadline     time.Time
	cursor       string
	position     *list.Element
	reading      bool
	enumerated   bool
	expired      bool
	outstanding  int
}

type notificationTarget struct {
	entry   *notificationEntry
	address string
}

// NewNotifier validates configuration without starting workers or accepting
// hints. Start must precede Hint. The source is queried serially; the sender must
// tolerate up to config.Concurrency simultaneous calls.
func NewNotifier(source notificationSource, sender notificationSender, config NotificationConfig) (*Notifier, error) {
	if nilExecutorDependency(source) || nilExecutorDependency(sender) {
		return nil, errors.New("cancellation notification dependencies must not be nil")
	}
	if config.InstanceCapacity <= 0 || config.PageSize < 1 || config.PageSize > 32 ||
		config.TargetCapacity <= 0 || config.Concurrency <= 0 || config.QueryTimeout <= 0 || config.InstanceTimeout <= 0 {
		return nil, errors.New("invalid cancellation notification settings")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Notifier{source: source, sender: sender, config: config, runCtx: ctx, cancel: cancel,
		done: make(chan struct{}), entries: make(map[int64]*notificationEntry), changed: make(chan struct{})}, nil
}

// Start starts one reader, fixed sender workers and deadline housekeeping.
// Repeated calls, including after Stop, return an error without creating workers.
func (n *Notifier) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started || n.stopping {
		return errors.New("cancellation notifier has already started or stopped")
	}
	n.started = true
	var workers sync.WaitGroup
	workers.Go(n.read)
	workers.Go(n.expire)
	for range n.config.Concurrency {
		workers.Go(n.send)
	}
	go func() {
		workers.Wait()
		n.finish()
	}()
	return nil
}

// Hint immediately admits or merges a confirmed stop decision. False means
// invalid input, closed admission, conflicting duplicate metadata or full
// Instance capacity. Existing matching Instances merge even when full. True
// confirms only volatile admission, never delivery. No caller Context is retained.
// A merged hint preserves the original deadline and does not create another pass.
func (n *Notifier) Hint(notification Notification) bool {
	logFull := false
	n.mu.Lock()
	defer func() {
		n.mu.Unlock()
		if logFull {
			logx.Errorw("job cancellation notification dropped because the queue is full", logx.Field("instance_id", notification.InstanceID))
		}
	}()
	if notification.InstanceID <= 0 || notification.ExecutorGroupID <= 0 ||
		(notification.Reason != model.EndReasonManualStop && notification.Reason != model.EndReasonInstanceTimeout) ||
		!n.started || n.stopping {
		return false
	}
	if entry := n.entries[notification.InstanceID]; entry != nil {
		return entry.notification == notification
	}
	if len(n.entries) == n.config.InstanceCapacity {
		logFull, n.fullLogged = !n.fullLogged, true
		return false
	}
	deadline := time.Now().Add(n.config.InstanceTimeout)
	ctx, cancel := context.WithDeadline(n.runCtx, deadline)
	entry := &notificationEntry{notification: notification, ctx: ctx, cancel: cancel, deadline: deadline}
	n.entries[notification.InstanceID] = entry
	entry.position = n.enumerating.PushBack(entry)
	n.notifyLocked()
	return true
}

// Stop closes admission and lets admitted notifications finish under their
// original budgets. It is idempotent. Stop before Start completes the unused
// notifier immediately. It never closes shared dependencies.
func (n *Notifier) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopping {
		return
	}
	n.stopping = true
	n.notifyLocked()
	if !n.started {
		n.completed = true
		n.cancel()
		close(n.done)
	}
}

// Drain closes admission and applies the first caller's non-nil shutdown Context
// to all accepted work. Later callers cannot replace or extend that budget. At
// expiration, pending work is discarded and active calls are canceled; Wait must
// still observe their actual return before dependencies are released. Individual
// notification failures are logged and do not become reliable delivery errors.
func (n *Notifier) Drain(ctx context.Context) error {
	if ctx == nil {
		return errors.New("cancellation notification drain context must not be nil")
	}
	n.Stop()
	n.mu.Lock()
	if n.completed {
		err := n.drainErr
		n.mu.Unlock()
		return err
	}
	first := n.drainCtx == nil
	if first {
		n.drainCtx = ctx
		n.stopDrain = context.AfterFunc(ctx, n.cancel)
		if ctx.Err() != nil {
			n.cancel()
		}
		n.notifyLocked()
	}
	n.mu.Unlock()
	if err := n.Wait(ctx); err != nil {
		if first {
			n.cancel()
		}
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.drainErr
}

// Wait waits for every worker and started dependency call to actually return.
// It neither stops admission nor grants a new processing budget. A Context error
// only ends this wait; callers may wait again after Drain has timed out.
func (n *Notifier) Wait(ctx context.Context) error {
	select {
	case <-n.done:
		return nil
	default:
	}
	select {
	case <-n.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *Notifier) read() {
	for {
		entry := n.takeEnumeration()
		if entry == nil {
			return
		}
		ctx, cancel := n.operationContext(entry, n.config.QueryTimeout)
		addresses, err := n.query(ctx, entry)
		cancel()
		if err == nil && len(addresses) > n.config.PageSize {
			err = errors.New("cancellation address source exceeded its page limit")
		}
		complete := err != nil || len(addresses) == 0
		if err != nil {
			n.logFailure("job cancellation address query failed", entry, err)
		} else {
			// Only this reader retains a page. When targets are full it stops here,
			// keeping both the unaccepted address and cursor for the same Instance.
			for _, address := range addresses {
				if address == "" || !n.offerTarget(entry, address) {
					complete = true
					break
				}
			}
		}
		n.finishEnumeration(entry, complete)
	}
}

func (n *Notifier) takeEnumeration() *notificationEntry {
	for {
		n.mu.Lock()
		if n.runCtx.Err() != nil || n.stopping && len(n.entries) == 0 {
			n.mu.Unlock()
			return nil
		}
		if n.targetCount < n.config.TargetCapacity && n.enumerating.Len() > 0 {
			position := n.enumerating.Front()
			entry := position.Value.(*notificationEntry)
			n.enumerating.Remove(position)
			entry.position = nil
			entry.reading = true
			n.mu.Unlock()
			return entry
		}
		changed := n.changed
		n.mu.Unlock()
		select {
		case <-changed:
		case <-n.runCtx.Done():
		}
	}
}

func (n *Notifier) offerTarget(entry *notificationEntry, address string) bool {
	for {
		n.mu.Lock()
		if entry.ctx.Err() != nil || !time.Now().Before(entry.deadline) {
			n.mu.Unlock()
			return false
		}
		if n.targetCount < n.config.TargetCapacity {
			n.targets.PushBack(notificationTarget{entry: entry, address: address})
			n.targetCount++
			entry.outstanding++
			entry.cursor = address
			n.notifyLocked()
			n.mu.Unlock()
			return true
		}
		changed := n.changed
		n.mu.Unlock()
		select {
		case <-changed:
		case <-entry.ctx.Done():
		}
	}
}

func (n *Notifier) finishEnumeration(entry *notificationEntry, complete bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	entry.reading = false
	entry.enumerated = entry.enumerated || complete || entry.ctx.Err() != nil || !time.Now().Before(entry.deadline)
	if !entry.enumerated {
		entry.position = n.enumerating.PushBack(entry)
	}
	n.releaseLocked(entry)
	n.notifyLocked()
}

func (n *Notifier) query(ctx context.Context, entry *notificationEntry) (addresses []string, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	defer func() {
		if recover() != nil {
			addresses, err = nil, errors.New("cancellation address source panicked")
		}
	}()
	addresses, err = n.source.ListCancelTargets(ctx, entry.notification.InstanceID, entry.cursor, n.config.PageSize)
	if err == nil {
		err = ctx.Err()
	}
	return addresses, err
}

func (n *Notifier) send() {
	for {
		target, ok := n.takeTarget()
		if !ok {
			return
		}
		ctx, cancel := n.operationContext(target.entry, 0)
		err := n.deliver(ctx, target)
		cancel()
		n.mu.Lock()
		n.targetCount--
		target.entry.outstanding--
		n.releaseLocked(target.entry)
		n.notifyLocked()
		n.mu.Unlock()
		if err != nil {
			n.logFailure("job cancellation target notification unconfirmed", target.entry, err)
		}
	}
}

func (n *Notifier) takeTarget() (notificationTarget, bool) {
	for {
		n.mu.Lock()
		if n.runCtx.Err() != nil || n.stopping && len(n.entries) == 0 {
			n.mu.Unlock()
			return notificationTarget{}, false
		}
		if position := n.targets.Front(); position != nil {
			target := position.Value.(notificationTarget)
			n.targets.Remove(position)
			// Retain both capacity slots through actual Cancel return, even if
			// the target expired while waiting or ignores Context cancellation.
			n.mu.Unlock()
			return target, true
		}
		changed := n.changed
		n.mu.Unlock()
		select {
		case <-changed:
		case <-n.runCtx.Done():
		}
	}
}

func (n *Notifier) deliver(ctx context.Context, target notificationTarget) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	defer func() {
		if recover() != nil {
			err = errors.New("cancellation sender panicked")
		}
	}()
	decision := target.entry.notification
	return n.sender.Cancel(ctx, CancelTarget{InstanceID: decision.InstanceID, ExecutorGroupID: decision.ExecutorGroupID,
		Address: target.address, Reason: decision.Reason})
}

func (n *Notifier) operationContext(entry *notificationEntry, timeout time.Duration) (context.Context, context.CancelFunc) {
	n.mu.Lock()
	drain := n.drainCtx
	n.mu.Unlock()
	deadline := entry.deadline
	if queryDeadline := time.Now().Add(timeout); timeout > 0 && queryDeadline.Before(deadline) {
		deadline = queryDeadline
	}
	if drain != nil {
		if shared, ok := drain.Deadline(); ok && shared.Before(deadline) {
			deadline = shared
		}
	}
	return context.WithDeadline(entry.ctx, deadline)
}

// expire releases expired waiting work even when all dependency calls are still
// returning from cancellation. Running calls keep their ownership until return;
// one housekeeping goroutine replaces a goroutine per admitted Instance.
func (n *Notifier) expire() {
	for {
		n.mu.Lock()
		now := time.Now()
		var next time.Time
		var expired []*notificationEntry
		for _, entry := range n.entries {
			if entry.ctx.Err() != nil || !now.Before(entry.deadline) {
				if !entry.expired {
					expired = append(expired, entry)
					entry.expired = true
				}
				// The existing deadline cancels active calls with DeadlineExceeded.
				// Calling cancel here can race that timer and replace its cause with
				// Canceled. Mark waiting work expired without canceling active owners;
				// shutdown cancellation already propagates through runCtx.
				entry.enumerated = true
				if entry.position != nil {
					n.enumerating.Remove(entry.position)
					entry.position = nil
				}
			} else if next.IsZero() || entry.deadline.Before(next) {
				next = entry.deadline
			}
		}
		for position := n.targets.Front(); position != nil; {
			following := position.Next()
			target := position.Value.(notificationTarget)
			if target.entry.expired {
				n.targets.Remove(position)
				n.targetCount--
				target.entry.outstanding--
			}
			position = following
		}
		for _, entry := range n.entries {
			n.releaseLocked(entry)
		}
		if len(expired) > 0 {
			n.notifyLocked()
		}
		finished := n.runCtx.Err() != nil || n.stopping && len(n.entries) == 0
		changed := n.changed
		n.mu.Unlock()
		for _, entry := range expired {
			cause := entry.ctx.Err()
			if !time.Now().Before(entry.deadline) {
				// Housekeeping and the Context timer may become runnable together.
				// Report the elapsed budget even before the timer callback runs.
				cause = context.DeadlineExceeded
			}
			n.logFailure("job cancellation notification budget exhausted", entry, cause)
		}
		if finished {
			return
		}
		if next.IsZero() {
			select {
			case <-changed:
			case <-n.runCtx.Done():
			}
			continue
		}
		timer := time.NewTimer(time.Until(next))
		select {
		case <-timer.C:
		case <-changed:
		case <-n.runCtx.Done():
		}
		timer.Stop()
	}
}

func (n *Notifier) releaseLocked(entry *notificationEntry) {
	if !entry.enumerated || entry.reading || entry.outstanding != 0 {
		return
	}
	entry.cancel()
	delete(n.entries, entry.notification.InstanceID)
	n.fullLogged = false
}

func (n *Notifier) finish() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.drainCtx != nil {
		n.drainErr = n.drainCtx.Err()
	}
	for _, entry := range n.entries {
		entry.cancel()
	}
	clear(n.entries)
	n.enumerating.Init()
	n.targets.Init()
	n.targetCount = 0
	if n.stopDrain != nil {
		n.stopDrain()
	}
	n.cancel()
	n.completed = true
	close(n.done)
}

func (n *Notifier) notifyLocked() {
	close(n.changed)
	n.changed = make(chan struct{})
}

func (n *Notifier) logFailure(message string, entry *notificationEntry, err error) {
	classification := "dependency_failure"
	if errors.Is(err, context.DeadlineExceeded) {
		classification = "deadline_exceeded"
	} else if errors.Is(err, context.Canceled) {
		classification = "canceled"
	}
	// Neither the saved address, request/authentication fields, panic value nor
	// upstream error text is safe log content. Keep diagnostics to fixed labels.
	logx.Errorw(message, logx.Field("instance_id", entry.notification.InstanceID), logx.Field("failure", classification))
}
