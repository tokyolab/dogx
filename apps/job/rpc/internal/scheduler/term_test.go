package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/service"
)

func unitRuntime(trigger TriggerService) *Runtime {
	return &Runtime{config: Config{Prefix: "/test/unit", QueueCapacity: 1024, CreationConcurrency: 32, LeaseTTLSeconds: 15, RecoveryLimit: 1024, QueryTimeout: 5 * time.Second},
		keys: makeKeys("/test/unit"), trigger: trigger, timing: defaultTimings(), now: time.Now,
		coordination: coordinationStub{}, newCron: func() cronEngine { return cron.New(cron.WithLocation(time.Local)) }}
}

func unitTerm(ctx context.Context, runtime *Runtime, engine cronEngine) *leaderTerm {
	return &leaderTerm{runtime: runtime, ctx: ctx, queue: newJobQueue(runtime.config.QueueCapacity),
		reload: make(chan struct{}, 1), cron: engine, rules: make(map[int64]registeredRule)}
}

func awaitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for operation")
	}
}

func TestApplyAcknowledgesWholeBatchBeforeDueChecksAndRetainsUnchanged(t *testing.T) {
	r := unitRuntime(triggerServiceStub{})
	r.config.QueueCapacity = 2048
	c := &cronStub{}
	term := unitTerm(context.Background(), r, c)
	rules := make([]repository.Rule, 1100)
	for i := range rules {
		rules[i] = repository.Rule{ID: int64(i + 1), CronExpr: "0 0 0 1 JAN *"}
	}
	c.onBarrier = func() {
		if !c.started || len(c.entries) != len(rules) || term.queue.size != 0 {
			t.Fatal("due checks preceded the running Cron batch acknowledgement")
		}
	}
	term.apply(rules)
	if c.starts != 1 || c.barriers != 1 || len(term.rules) != 1100 {
		t.Fatalf("full load was truncated: starts=%d barriers=%d rules=%d", c.starts, c.barriers, len(term.rules))
	}
	for range rules {
		id, _ := term.queue.take()
		term.queue.complete(id)
	}
	unchangedEntry := term.rules[4].entry
	term.apply(rules)
	if c.starts != 1 || c.nextID != 1100 || term.queue.size != 0 {
		t.Fatal("unchanged rules were rebuilt or offered again")
	}
	c.onBarrier = nil
	updated := append([]repository.Rule(nil), rules[2:]...)
	updated[0].CronExpr = "0 0 0 1 FEB *"
	updated = append(updated, repository.Rule{ID: 1, CronExpr: "@daily"}, repository.Rule{ID: 1101, CronExpr: "0 0 0 1 JAN *"})
	term.apply(updated)
	if len(term.rules) != 1099 || term.rules[4].entry != unchangedEntry || term.queue.size != 2 {
		t.Fatalf("diff application rebuilt unchanged rules or retained deleted/bad rules: rules=%d queue=%d", len(term.rules), term.queue.size)
	}
	if _, exists := term.rules[1]; exists {
		t.Fatal("invalid replacement retained its previous rule")
	}
	if _, exists := term.rules[2]; exists {
		t.Fatal("omitted rule remained registered")
	}
	for _, want := range []int64{3, 1101} {
		id, _ := term.queue.take()
		if id != want {
			t.Fatalf("changed due check=%d, want %d", id, want)
		}
		term.queue.complete(id)
	}
	term.apply(nil)
	if len(term.rules) != 0 || len(c.entries) != 0 || c.starts != 1 {
		t.Fatal("successful empty configuration did not remove every rule")
	}
}

func TestApplyStopsInstallingWhenTermCanceledDuringLargeBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &cronStub{onAdd: cancel}
	term := unitTerm(ctx, unitRuntime(triggerServiceStub{}), c)
	term.apply([]repository.Rule{{ID: 1, CronExpr: "* * * * * *"}, {ID: 2, CronExpr: "* * * * * *"}})
	if c.nextID != 1 || c.starts != 0 || c.barriers != 0 || term.queue.size != 0 {
		t.Fatal("canceled batch continued installing or offering work")
	}
	term.apply([]repository.Rule{{ID: 3, CronExpr: "* * * * * *"}})
	if c.nextID != 1 {
		t.Fatal("late load installed a canceled term's rules")
	}
}

func TestConfigFailurePreservesRulesAndHintDuringLoadSurvives(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failed, loading, release, applied := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	r := unitRuntime(triggerServiceStub{load: func(ctx context.Context) ([]repository.Rule, error) {
		switch calls.Add(1) {
		case 1:
			close(failed)
			return []repository.Rule{{ID: 99, CronExpr: "* * * * * *"}}, errors.New("incomplete query")
		case 2:
			close(loading)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return []repository.Rule{{ID: 2, CronExpr: "0 0 0 1 JAN *"}}, nil
		default:
			return nil, nil
		}
	}})
	c := &cronStub{}
	term := unitTerm(ctx, r, c)
	term.apply([]repository.Rule{{ID: 1, CronExpr: "0 0 0 1 JAN *"}})
	c.onBarrier = func() {
		if len(c.entries) == 0 {
			close(applied)
		}
	}
	done := make(chan struct{})
	go func() { defer close(done); term.configLoop() }()
	t.Cleanup(func() { cancel(); awaitSignal(t, done) })
	term.signalReload()
	awaitSignal(t, failed)
	// With no second signal, a failed load must wait rather than spin itself.
	select {
	case <-loading:
		t.Fatal("failed load immediately retried itself")
	case <-time.After(30 * time.Millisecond):
	}
	term.cronMu.Lock()
	_, retained := term.rules[1]
	_, partialApplied := term.rules[99]
	term.cronMu.Unlock()
	if !retained || partialApplied {
		t.Fatal("failed load discarded old rules or installed partial results")
	}
	term.signalReload()
	awaitSignal(t, loading)
	term.signalReload()
	term.signalReload()
	close(release)
	awaitSignal(t, applied)
	cancel()
	awaitSignal(t, done)
	if calls.Load() != 3 || len(term.rules) != 0 || r.Stats().LoadErrors != 1 {
		t.Fatalf("pending signal or successful empty batch lost: calls=%d rules=%d stats=%+v", calls.Load(), len(term.rules), r.Stats())
	}
}

func TestRecoveryUsesOneBoundedBatchAndWorkersKeepTermContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var queryCtx context.Context
	var calls int
	triggered := make(chan struct{}, 1)
	r := unitRuntime(triggerServiceStub{
		due: func(ctx context.Context, at time.Time, limit int) ([]repository.DueJob, error) {
			queryCtx, calls = ctx, calls+1
			if limit != 1024 || time.Since(at) > time.Second {
				t.Errorf("wrong recovery bound or stale decision time: limit=%d at=%s", limit, at)
			}
			return []repository.DueJob{{ID: 1}, {ID: 2}, {ID: 3}}, nil
		},
		trigger: func(ctx context.Context, id int64) (service.AutomaticResult, error) {
			if ctx.Err() != nil || id != 1 {
				t.Errorf("query context leaked into worker: job=%d error=%v", id, ctx.Err())
			}
			triggered <- struct{}{}
			cancel()
			return service.AutomaticResult{}, nil
		},
	})
	r.config.QueueCapacity = 1
	term := unitTerm(ctx, r, &cronStub{})
	term.recoverDue()
	if calls != 1 || queryCtx.Err() == nil || term.queue.size != 1 || r.Stats().QueueRejected != 1 {
		t.Fatalf("recovery paged, retained query context, or overflowed: calls=%d queued=%d stats=%+v", calls, term.queue.size, r.Stats())
	}
	done := make(chan struct{})
	go func() { defer close(done); term.worker() }()
	awaitSignal(t, triggered)
	term.queue.close()
	awaitSignal(t, done)
}

func TestInitialLoadFailureDoesNotStopIndependentRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	recovered := make(chan struct{})
	r := unitRuntime(triggerServiceStub{
		load: func(context.Context) ([]repository.Rule, error) { return nil, errors.New("database read failed") },
		due: func(context.Context, time.Time, int) ([]repository.DueJob, error) {
			return []repository.DueJob{{ID: 42}}, nil
		},
		trigger: func(ctx context.Context, id int64) (service.AutomaticResult, error) {
			if id != 42 || ctx.Err() != nil {
				t.Errorf("unexpected recovered job/context: %d %v", id, ctx.Err())
			}
			close(recovered)
			return service.AutomaticResult{}, nil
		},
	})
	c := &cronStub{}
	r.newCron = func() cronEngine { return c }
	done := make(chan struct{})
	go func() { defer close(done); r.runTerm(ctx) }()
	awaitSignal(t, recovered)
	cancel()
	awaitSignal(t, done)
	if c.starts != 0 {
		t.Fatal("failed first load started Cron")
	}
}

func TestRecoveryDiscardsPartialFailureAndDoesNotRescanItself(t *testing.T) {
	var calls int
	r := unitRuntime(triggerServiceStub{due: func(ctx context.Context, _ time.Time, limit int) ([]repository.DueJob, error) {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 5*time.Second || limit != 17 {
			t.Error("recovery ignored the configured limit or query budget")
		}
		return []repository.DueJob{{ID: 1}}, errors.New("query interrupted")
	}})
	r.config.RecoveryLimit = 17
	term := unitTerm(context.Background(), r, &cronStub{})
	term.recoverDue()
	if calls != 1 || term.queue.size != 0 || r.Stats().RecoveryErrors != 1 {
		t.Fatalf("partial failed query was applied/retried: calls=%d queue=%d stats=%+v", calls, term.queue.size, r.Stats())
	}
}

func TestExpiredQuerySuccessCannotInstallRulesOrOfferRecoveryWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := unitRuntime(triggerServiceStub{
		load: func(ctx context.Context) ([]repository.Rule, error) {
			<-ctx.Done()
			return []repository.Rule{{ID: 1, CronExpr: "* * * * * *"}}, nil
		},
		due: func(ctx context.Context, _ time.Time, _ int) ([]repository.DueJob, error) {
			<-ctx.Done()
			return []repository.DueJob{{ID: 1}}, nil
		},
	})
	r.timing.query = 10 * time.Millisecond
	c := &cronStub{}
	term := unitTerm(ctx, r, c)
	done := make(chan struct{})
	go func() { defer close(done); term.configLoop() }()
	t.Cleanup(func() { cancel(); awaitSignal(t, done) })
	term.signalReload()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for r.Stats().LoadErrors != 1 {
		select {
		case <-deadline.C:
			t.Fatal("expired configuration query was treated as successful")
		case <-ticker.C:
		}
	}
	term.recoverDue()
	cancel()
	awaitSignal(t, done)
	if c.starts != 0 || len(term.rules) != 0 || term.queue.size != 0 || r.Stats().RecoveryErrors != 1 {
		t.Fatalf("expired successful query applied results: starts=%d rules=%d queue=%d stats=%+v", c.starts, len(term.rules), term.queue.size, r.Stats())
	}
}

func TestWorkerFailureDoesNotGenerateItsOwnHint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{}, 2)
	var calls atomic.Int32
	r := unitRuntime(triggerServiceStub{trigger: func(context.Context, int64) (service.AutomaticResult, error) {
		calls.Add(1)
		finished <- struct{}{}
		return service.AutomaticResult{}, errors.New("database unavailable")
	}})
	term := unitTerm(ctx, r, &cronStub{})
	done := make(chan struct{})
	go func() { defer close(done); term.worker() }()
	t.Cleanup(func() { cancel(); term.queue.close(); awaitSignal(t, done) })
	term.offer(1)
	awaitSignal(t, finished)
	select {
	case <-finished:
		t.Fatal("failed trigger generated an unsolicited retry")
	case <-time.After(30 * time.Millisecond):
	}
	term.offer(1)
	awaitSignal(t, finished)
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for r.Stats().TriggerErrors != 2 {
		select {
		case <-deadline.C:
			t.Fatal("second failed operation did not finish")
		case <-ticker.C:
		}
	}
	cancel()
	term.queue.close()
	awaitSignal(t, done)
	if calls.Load() != 2 || r.Stats().TriggerErrors != 2 {
		t.Fatalf("independent new hint lost: calls=%d stats=%+v", calls.Load(), r.Stats())
	}
}
