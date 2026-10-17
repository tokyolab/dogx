// Package scheduler runs the Job trigger leader in the management RPC process.
// Its hints only identify Jobs; the creator rereads database facts before creating
// a WAITING Instance. No Task execution or Instance ownership lives here.
package scheduler

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/trigger"
	"github.com/zeromicro/go-zero/core/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// RuleSource reads enabled rules and due candidates without claiming Jobs or
// advancing trigger progress. Loading and recovery may run concurrently;
// implementations must support concurrent use and propagate contexts to storage.
// Results are hints: triggering must reread and validate the Job in a transaction.
type RuleSource interface {
	// LoadRules returns all enabled, undeleted rules. An empty result removes
	// all local rules. On error, the caller preserves its previous rules until
	// a later successful load.
	LoadRules(context.Context) ([]repository.Rule, error)
	// ListDue returns enabled, undeleted Jobs whose next_trigger_at is at or
	// before the given time, ordered by next_trigger_at then ID. The positive
	// limit bounds the result size; no candidates is not an error.
	ListDue(context.Context, time.Time, int) ([]repository.DueJob, error)
}

// TriggerService turns disposable Job ID hints into persistent trigger decisions.
// Workers may call it concurrently. It owns shared creation permits, transactions,
// and automatic-trigger identity checks; leadership or in-memory coalescing does
// not replace database duplicate prevention.
type TriggerService interface {
	// TriggerAutomatic rereads the Job and decides whether to create an Instance.
	// A nil error can mean a normal skip, such as a disabled or not-yet-due Job;
	// callers must inspect AutomaticResult. Success requires confirmed commit;
	// errors must not expose an uncommitted InstanceID.
	TriggerAutomatic(context.Context, int64) (trigger.AutomaticResult, error)
}

// cronEngine is the per-term robfig Cron dependency. It schedules callbacks, not
// persistent progress. Replacements must preserve the initialization barrier of
// Entries and the callback completion signal returned by Stop.
type cronEngine interface {
	// Schedule registers a recurring rule and returns its local ID. Returning
	// does not guarantee that the initial Next value has been computed.
	Schedule(cron.Schedule, cron.Job) cron.EntryID
	// Remove removes a rule without canceling callbacks that have already started.
	Remove(cron.EntryID)
	// Start starts the timing loop without waiting for all rules to initialize.
	Start()
	// Entries returns a snapshot from the running loop after preceding additions
	// have computed their initial Next values. When stopped, it only snapshots
	// memory and cannot acknowledge timing-loop initialization.
	Entries() []cron.Entry
	// Stop stops future scheduling without canceling running callbacks. The
	// returned context's Done closes after those callbacks exit; callers must
	// wait for that signal, not just for Stop to return.
	Stop() context.Context
}

// Runtime manages election, rule synchronization, Cron, and missed-trigger
// recovery, not Task execution. Construct it with New. Only one Run may be active
// per instance; Notify and Stats may run concurrently. The caller owns the etcd
// client and must close it only after Run and other calls have finished.
type Runtime struct {
	config       Config
	keys         keys
	trigger      TriggerService
	rules        RuleSource
	coordination coordination
	timing       timings
	now          func() time.Time
	newCron      func() cronEngine
	running      atomic.Bool
	counters     counters
	log          rateLog
}

type counters struct {
	terms, rejected, triggerErrors, loadErrors, recoveryErrors, watchRestarts, notificationErrors atomic.Uint64
}

// Stats are monotonic process counters; leadership is deliberately not a health
// signal. They remain available while a previous term is waiting for cleanup.
type Stats struct {
	Terms, QueueRejected, TriggerErrors, LoadErrors, RecoveryErrors, WatchRestarts, NotificationErrors uint64
}

// Stats returns cumulative counters safely under concurrent use. Fields are read
// separately, so the result is not an atomic snapshot of a single instant.
func (r *Runtime) Stats() Stats {
	return Stats{r.counters.terms.Load(), r.counters.rejected.Load(), r.counters.triggerErrors.Load(),
		r.counters.loadErrors.Load(), r.counters.recoveryErrors.Load(), r.counters.watchRestarts.Load(), r.counters.notificationErrors.Load()}
}

// New validates dependencies and configuration without starting election or
// background work. The caller must share a process-scoped TriggerService and
// manage the supplied client's lifetime.
func New(client *clientv3.Client, rules RuleSource, trigger TriggerService, config Config) (*Runtime, error) {
	if client == nil || rules == nil || trigger == nil {
		return nil, errors.New("scheduler requires etcd, rule source and trigger creator")
	}
	cfg, err := config.defaults()
	if err != nil {
		return nil, err
	}
	timing := defaultTimings()
	timing.query = cfg.QueryTimeout
	return &Runtime{config: cfg, keys: makeKeys(cfg.Prefix), rules: rules, trigger: trigger,
		coordination: etcdCoordination{client: client, request: timing.request}, timing: timing, now: time.Now,
		newCron: func() cronEngine { return cron.New(cron.WithLocation(time.Local)) }}, nil
}

// Notify hints that the leader should reload after a confirmed management commit;
// non-leaders may call it too. It makes one synchronous, time-budgeted etcd write
// without a lease, application retries, or background work. A nil error only
// confirms the hint write. Failure does not roll back the management operation;
// periodic reconciliation recovers missed hints.
func (r *Runtime) Notify(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, r.timing.notify)
	defer cancel()
	err := r.coordination.put(ctx, r.keys.changed, "reload")
	if err != nil {
		r.counters.notificationErrors.Add(1)
		r.log.error("notification", 0, err)
	}
	return err
}

// Run blocks while managing consecutive candidacies and leader terms, retrying
// election failures with backoff. A concurrent Run on the same Runtime fails.
// Cancellation returns ctx.Err() after in-flight operations, Cron callbacks, and
// session cleanup have exited; it does not promise to return at the deadline.
// A new local term cannot start before the old term exits. Run does not close the
// caller's etcd client.
func (r *Runtime) Run(ctx context.Context) error {
	if !r.running.CompareAndSwap(false, true) {
		return errors.New("scheduler is already running")
	}
	defer r.running.Store(false)
	attempt := 0
	for ctx.Err() == nil {
		candidateCtx, cancel := context.WithCancel(ctx)
		session, err := r.coordination.open(candidateCtx, r.config.LeaseTTLSeconds, r.keys.election)
		if err != nil {
			cancel()
			r.log.error("election-session", 0, err)
			if !r.backoff(ctx, attempt) {
				break
			}
			attempt++
			continue
		}
		monitorDone := make(chan struct{})
		go func() {
			defer close(monitorDone)
			select {
			case <-session.done():
				cancel()
			case <-candidateCtx.Done():
			}
		}()
		err = session.campaign(candidateCtx)
		// A successful Campaign may race with Session.Done. Do not install a
		// runtime merely because the campaign returned nil.
		select {
		case <-session.done():
			cancel()
		default:
		}
		if err == nil && candidateCtx.Err() == nil {
			attempt = 0
			r.counters.terms.Add(1)
			r.runTerm(candidateCtx)
		} else if err != nil && ctx.Err() == nil {
			r.log.error("election-campaign", 0, err)
		}
		cancel()
		<-monitorDone
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), r.timing.request)
		if err := session.close(cleanupCtx); err != nil {
			r.log.error("election-cleanup", 0, err)
		}
		cleanupCancel()
		if !r.backoff(ctx, attempt) {
			break
		}
		attempt++
	}
	return ctx.Err()
}

func (r *Runtime) backoff(ctx context.Context, attempt int) bool {
	delay := r.timing.backoffMin
	for i := 0; i < attempt && delay < r.timing.backoffMax; i++ {
		if delay > r.timing.backoffMax/2 {
			delay = r.timing.backoffMax
		} else {
			delay *= 2
		}
	}
	// Equal jitter is bounded by the configured cap; its first delay is at
	// least the minimum, so a failed watch never turns into a tight retry loop.
	low := delay / 2
	if low < r.timing.backoffMin {
		low = r.timing.backoffMin
	}
	if delay > low {
		delay = low + time.Duration(rand.Int64N(int64(delay-low)+1))
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

type rateLog struct {
	mu   sync.Mutex
	last map[string]time.Time
}

func (l *rateLog) error(event string, jobID int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.last == nil {
		l.last = make(map[string]time.Time)
	}
	now := time.Now()
	if now.Sub(l.last[event]) < time.Minute {
		return
	}
	l.last[event] = now
	// Categories are fixed and bounded; do not retain an unbounded per-Job
	// suppression map as malformed or deleted IDs accumulate over process life.
	logx.Errorf("job scheduler event=%s job_id=%d error=%v", event, jobID, err)
}
