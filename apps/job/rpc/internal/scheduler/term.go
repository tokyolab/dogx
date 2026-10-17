package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/schedule"
)

type registeredRule struct {
	expr  string
	entry cron.EntryID
}

// leaderTerm owns disposable state for one lease term and cannot be reused after
// leadership is lost. Only configLoop mutates rules; cronMu coordinates rule
// application with Cron shutdown. Other goroutines only offer hints.
type leaderTerm struct {
	runtime *Runtime
	ctx     context.Context
	queue   *jobQueue
	reload  chan struct{}
	cron    cronEngine
	cronMu  sync.Mutex
	started bool
	stopped bool
	rules   map[int64]registeredRule
}

// runTerm blocks until the term is canceled and all local work exits. It does not
// own Instance execution. Queue closure discards only in-memory hints; a later
// valid term reconciles persistent progress and missed triggers.
func (r *Runtime) runTerm(ctx context.Context) {
	t := &leaderTerm{runtime: r, ctx: ctx, queue: newJobQueue(r.config.QueueCapacity), reload: make(chan struct{}, 1),
		cron: r.newCron(), rules: make(map[int64]registeredRule)}
	var workers, sources sync.WaitGroup
	for range r.config.CreationConcurrency {
		workers.Go(t.worker)
	}
	sources.Go(t.configLoop)
	sources.Go(t.configTicker)
	sources.Go(t.watchLoop)
	sources.Go(t.recoveryLoop)
	t.signalReload()
	<-ctx.Done()
	t.queue.close()
	cleaned := make(chan struct{})
	go func() {
		defer close(cleaned)
		// Serialize Stop with applying a batch and its Entries barrier. Without
		// this lock, a late load could restart Cron after cleanup or mistake a
		// stopped in-memory Entries snapshot for initialization acknowledgement.
		t.cronMu.Lock()
		t.stopped = true
		cronStopped := t.cron.Stop()
		t.cronMu.Unlock()
		sources.Wait()
		<-cronStopped.Done()
		workers.Wait()
	}()
	watchdog := time.NewTimer(r.timing.request)
	defer watchdog.Stop()
	select {
	case <-cleaned:
		return
	case <-watchdog.C:
		r.log.error("term-cleanup", 0, errors.New("old term still exiting; local reelection remains blocked"))
		// This is a diagnostic threshold, not permission to abandon a driver
		// operation or release process permits before the operation returns.
		<-cleaned
	}
}

func (t *leaderTerm) offer(id int64) bool {
	if t.ctx.Err() != nil {
		return false
	}
	if !t.queue.offer(id) {
		if t.ctx.Err() == nil {
			t.runtime.counters.rejected.Add(1)
			t.runtime.log.error("queue-full", id, errors.New("pending Job capacity exhausted"))
		}
		return false
	}
	return true
}

func (t *leaderTerm) worker() {
	for {
		id, ok := t.queue.take()
		if !ok {
			return
		}
		if t.ctx.Err() != nil {
			return
		}
		// The process-scoped creator owns the shared automatic/manual permit and the
		// complete 3-second operation budget; neither is recreated each term.
		if _, err := t.runtime.trigger.TriggerAutomatic(t.ctx, id); err != nil && t.ctx.Err() == nil {
			t.runtime.counters.triggerErrors.Add(1)
			t.runtime.log.error("automatic-trigger", id, err)
		}
		t.queue.complete(id)
	}
}

// signalReload coalesces hints without waiting for a load. A hint received during
// a load remains pending so a stale query cannot hide a newer commit. Hints carry
// no configuration and provide no consumption acknowledgement.
func (t *leaderTerm) signalReload() {
	if t.ctx.Err() != nil {
		return
	}
	select {
	case t.reload <- struct{}{}:
	default:
	}
}

func (t *leaderTerm) configTicker() {
	ticker := time.NewTicker(t.runtime.timing.config)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.signalReload()
		}
	}
}

func (t *leaderTerm) configLoop() {
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-t.reload:
		}
		if t.ctx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(t.ctx, t.runtime.timing.query)
		rules, err := t.runtime.rules.LoadRules(ctx)
		if err == nil {
			// A dependency can finish unwinding after its deadline. Treat that
			// result as failed even if it neglected to return the context error.
			err = ctx.Err()
		}
		cancel()
		if t.ctx.Err() != nil {
			return
		}
		if err != nil {
			t.runtime.counters.loadErrors.Add(1)
			t.runtime.log.error("load-rules", 0, err)
			continue
		}
		t.apply(rules)
		// A hint arriving during LoadRules remains in the capacity-one channel.
		// Draining it here would lose a newer committed configuration change.
	}
}

// apply installs a successfully loaded full snapshot and removes rules absent
// from it. Invalid rules are removed and logged. Application is not a database
// transaction; term cleanup handles a partial batch after cancellation. Added or
// changed rules receive an extra hint after Cron initialization is acknowledged,
// covering trigger times crossed while loading.
func (t *leaderTerm) apply(rules []repository.Rule) {
	t.cronMu.Lock()
	defer t.cronMu.Unlock()
	if t.stopped || t.ctx.Err() != nil {
		return
	}
	next := make(map[int64]registeredRule, len(rules))
	changed := make([]int64, 0)
	for _, rule := range rules {
		// Applying a large batch can outlive the lease. Stop adding entries as
		// soon as cancellation is observed; cleanup owns any partial batch.
		if t.ctx.Err() != nil {
			return
		}
		if old, ok := t.rules[rule.ID]; ok && old.expr == rule.CronExpr {
			next[rule.ID] = old
			continue
		}
		parsed, err := schedule.Parse(rule.CronExpr)
		if err == nil && parsed.Next(t.runtime.now()).IsZero() {
			err = errors.New("cron has no next occurrence")
		}
		if old, ok := t.rules[rule.ID]; ok {
			t.cron.Remove(old.entry)
			delete(t.rules, rule.ID)
		}
		if err != nil {
			t.runtime.log.error("invalid-rule", rule.ID, err)
			continue
		}
		id := rule.ID
		entry := t.cron.Schedule(parsed, cron.FuncJob(func() { t.offer(id) }))
		next[id] = registeredRule{expr: rule.CronExpr, entry: entry}
		changed = append(changed, id)
	}
	for id, old := range t.rules {
		if _, ok := next[id]; !ok {
			t.cron.Remove(old.entry)
		}
	}
	t.rules = next
	if t.ctx.Err() != nil {
		return
	}
	if !t.started {
		t.cron.Start()
		t.started = true
	}
	// robfig v3.0.1 processes snapshots on its run goroutine after initializing
	// startup/addition Next values. One round trip acknowledges the whole batch;
	// Start or Schedule returning alone does not provide this guarantee.
	t.cron.Entries()
	for _, id := range changed {
		if !t.offer(id) {
			break
		}
	}
}

// recoveryLoop queries immediately at term start, then waits the recovery
// interval after each batch. It is independent of rule synchronization; recovery
// queries within one term never overlap or replay accumulated timer events.
func (t *leaderTerm) recoveryLoop() {
	for {
		t.recoverDue()
		// Start the next wait after the single batch. A slow query never queues
		// accumulated ticker events or starts overlapping recovery queries.
		timer := time.NewTimer(t.runtime.timing.recovery)
		select {
		case <-t.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// recoverDue reads one batch of the earliest due Jobs and offers IDs to the same
// trigger queue. It stops when the queue rejects a hint, keeps no cross-pass
// cursor, and never advances database progress merely because a hint was queued.
// Each pass queries again; it need not drain the backlog. TriggerService remains
// responsible for misfire and duplicate decisions.
func (t *leaderTerm) recoverDue() {
	if t.ctx.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(t.ctx, t.runtime.timing.query)
	rows, err := t.runtime.rules.ListDue(ctx, t.runtime.now(), t.runtime.config.RecoveryLimit)
	if err == nil {
		err = ctx.Err()
	}
	cancel()
	if err != nil {
		if t.ctx.Err() == nil {
			t.runtime.counters.recoveryErrors.Add(1)
			t.runtime.log.error("recover-due", 0, err)
		}
		return
	}
	for _, row := range rows {
		// The canceled query context does not escape into workers. Queue items
		// carry only IDs, and actual operations use the original term context.
		if !t.offer(row.ID) {
			return
		}
	}
}

// watchLoop reads an etcd revision, watches from the next revision, and requests a
// full reload to cover subscription races. It rebuilds ended or failed watches
// with backoff. Hints may coalesce, so minute-level reconciliation is still needed.
// The watch neither dispatches Tasks nor confirms that a management change has
// been applied by the leader.
func (t *leaderTerm) watchLoop() {
	attempt := 0
	for t.ctx.Err() == nil {
		ctx, cancel := context.WithTimeout(t.ctx, t.runtime.timing.request)
		revision, err := t.runtime.coordination.revision(ctx, t.runtime.keys.changed)
		cancel()
		if err == nil {
			watchCtx, stopWatch := context.WithCancel(t.ctx)
			stream := t.runtime.coordination.watch(watchCtx, t.runtime.keys.changed, revision+1)
			t.signalReload()
			for t.ctx.Err() == nil {
				select {
				case <-t.ctx.Done():
				case response, ok := <-stream:
					if !ok || response.Canceled || response.Err() != nil {
						err = response.Err()
						if err == nil {
							err = errors.New("configuration watch closed or requires rebuilding")
						}
						goto rebuild
					}
					if len(response.Events) > 0 {
						attempt = 0
						t.signalReload()
					}
				}
			}
		rebuild:
			// A new watcher starts only after the old one is canceled. Transport
			// reconnects are left to the official client until it closes the stream.
			stopWatch()
		}
		if t.ctx.Err() != nil {
			return
		}
		t.runtime.counters.watchRestarts.Add(1)
		t.runtime.log.error("watch-rebuild", 0, err)
		if !t.runtime.backoff(t.ctx, attempt) {
			return
		}
		attempt++
	}
}
