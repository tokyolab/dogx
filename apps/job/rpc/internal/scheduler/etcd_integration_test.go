//go:build integration

package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/service"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdLossWaitsForOldTermBeforeLocalReelection(t *testing.T) {
	f := newEtcdFixture(t)
	for _, blockedWork := range []string{"configuration", "automatic-operation"} {
		t.Run(blockedWork, func(t *testing.T) {
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			releaseWork := func() { releaseOnce.Do(func() { close(release) }) }
			var loads, triggers atomic.Int32
			first := triggerServiceStub{
				load: func(ctx context.Context) ([]repository.Rule, error) {
					if loads.Add(1) > 1 {
						return nil, nil
					}
					if blockedWork == "configuration" {
						close(entered)
						<-ctx.Done()
						close(canceled)
						// A deliberately uncooperative dependency demonstrates that a
						// canceled old load cannot resurrect Cron or authorize a term.
						<-release
					}
					return []repository.Rule{{ID: 42, CronExpr: "0 0 0 1 JAN *"}}, nil
				},
				trigger: func(ctx context.Context, _ int64) (service.AutomaticResult, error) {
					triggers.Add(1)
					if blockedWork == "automatic-operation" {
						close(entered)
						<-ctx.Done()
						close(canceled)
						<-release
					}
					return service.AutomaticResult{}, nil
				},
			}
			prefix := "/test/cleanup/" + blockedWork
			a, err := New(f.replicaClient(t), first, Config{Prefix: prefix})
			if err != nil {
				t.Fatal(err)
			}
			candidates := make(chan context.Context, 1)
			a.coordination = observedCandidateCoordination{coordination: a.coordination, candidates: candidates}
			startRuntime(t, a)
			// Release before the registered runtime cleanup if an assertion fails.
			t.Cleanup(releaseWork)
			awaitSignal(t, entered)
			candidateCtx := <-candidates
			b, err := New(f.replicaClient(t), triggerServiceStub{}, Config{Prefix: prefix})
			if err != nil {
				t.Fatal(err)
			}
			startRuntime(t, b)
			revokeLeader(t, f.client, prefix)
			// LoadRules has its own five-second deadline. Only cancellation of
			// the actual candidate lifecycle proves the old term has lost authority.
			awaitSignal(t, candidateCtx.Done())
			awaitSignal(t, canceled)
			waitCondition(t, 10*time.Second, "other replica to take over", func() bool { return b.Stats().Terms == 1 })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			response, err := f.client.Get(ctx, makeKeys(prefix).election+"/", clientv3.WithPrefix())
			cancel()
			if err != nil || len(response.Kvs) != 1 || a.Stats().Terms != 1 {
				t.Fatalf("blocked old term reentered election: candidates=%v terms=%d error=%v", response, a.Stats().Terms, err)
			}
			releaseWork()
			waitCondition(t, 10*time.Second, "cleaned old replica to campaign again", func() bool {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				response, err := f.client.Get(ctx, makeKeys(prefix).election+"/", clientv3.WithPrefix())
				return err == nil && len(response.Kvs) == 2
			})
			revokeLeader(t, f.client, prefix)
			waitCondition(t, 10*time.Second, "fresh second local term", func() bool { return a.Stats().Terms == 2 })
			if blockedWork == "configuration" && triggers.Load() != 0 {
				t.Fatal("late canceled load offered automatic work")
			}
			if blockedWork == "automatic-operation" && triggers.Load() != 1 {
				t.Fatal("old queue was reused by the new term")
			}
		})
	}
}

type observedCandidateCoordination struct {
	coordination
	candidates chan context.Context
}

func (c observedCandidateCoordination) open(ctx context.Context, ttl int, prefix string) (electionSession, error) {
	session, err := c.coordination.open(ctx, ttl, prefix)
	if err == nil {
		select {
		case c.candidates <- ctx:
		default:
		}
	}
	return session, err
}

// The gap is between a real linearizable Get and the actual Watch RPC. Compacting
// etcd during this gap tests the official server's ErrCompacted response, without
// manufacturing a watch response or replacing the lease/clock implementation.
type watchGapCoordination struct {
	coordination
	once    sync.Once
	entered chan int64
	release chan struct{}
	watches atomic.Int32
}

func (c *watchGapCoordination) watch(ctx context.Context, key string, revision int64) clientv3.WatchChan {
	c.watches.Add(1)
	c.once.Do(func() {
		c.entered <- revision
		select {
		case <-c.release:
		case <-ctx.Done():
		}
	})
	return c.coordination.watch(ctx, key, revision)
}

func TestEtcdWatchCompactionRebuildsAndPersistentNotificationsRemainHints(t *testing.T) {
	f := newEtcdFixture(t)
	r, err := New(f.replicaClient(t), triggerServiceStub{}, Config{Prefix: "/test/watch-compaction"})
	if err != nil {
		t.Fatal(err)
	}
	gap := &watchGapCoordination{coordination: r.coordination, entered: make(chan int64, 1), release: make(chan struct{})}
	r.coordination = gap
	ctx, cancel := context.WithCancel(context.Background())
	term := unitTerm(ctx, r, &cronStub{})
	done := make(chan struct{})
	go func() { defer close(done); term.watchLoop() }()
	t.Cleanup(func() { cancel(); awaitSignal(t, done) })
	var revision int64
	select {
	case revision = <-gap.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("initial watch did not reach the revision handoff")
	}
	requestCtx, requestCancel := context.WithTimeout(ctx, 5*time.Second)
	defer requestCancel()
	var written *clientv3.PutResponse
	for range 3 {
		written, err = f.client.Put(requestCtx, "/test/unrelated", "advance")
		if err != nil {
			t.Fatal(err)
		}
	}
	if written.Header.Revision <= revision {
		t.Fatal("test failed to create a compacted watch gap")
	}
	if _, err := f.client.Compact(requestCtx, written.Header.Revision); err != nil {
		t.Fatal(err)
	}
	close(gap.release)
	awaitSignal(t, term.reload)
	waitCondition(t, 5*time.Second, "compacted watch restart", func() bool { return r.Stats().WatchRestarts > 0 && gap.watches.Load() > 1 })
	awaitSignal(t, term.reload)
	for range 2 {
		if err := r.Notify(ctx); err != nil {
			t.Fatal(err)
		}
		awaitSignal(t, term.reload)
	}
	response, err := f.client.Get(ctx, r.keys.changed)
	if err != nil || len(response.Kvs) != 1 || response.Kvs[0].Lease != 0 || string(response.Kvs[0].Value) != "reload" {
		t.Fatalf("notification is not a persistent fixed-value hint: response=%v error=%v", response, err)
	}
	if _, err := f.client.Delete(ctx, r.keys.changed); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, term.reload)
	if len(term.rules) != 0 || term.queue.size != 0 {
		t.Fatal("a notification directly changed configuration or queued a Job")
	}
}

func TestEtcdWatchRecoversAfterActualServerRestart(t *testing.T) {
	f := newEtcdFixture(t)
	replica := f.replicaClient(t)
	r, err := New(replica, triggerServiceStub{}, Config{Prefix: "/test/watch-restart"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	term := unitTerm(ctx, r, &cronStub{})
	done := make(chan struct{})
	go func() { defer close(done); term.watchLoop() }()
	t.Cleanup(func() { cancel(); awaitSignal(t, done) })
	awaitSignal(t, term.reload)
	f.stop(t)
	if err := r.Notify(context.Background()); err == nil || r.Stats().NotificationErrors != 1 {
		t.Fatal("notification reported success while the etcd process was stopped")
	}
	f.start(t)
	// Server readiness on the fixture connection does not end this client's
	// independent gRPC reconnect backoff. Confirm its linearizable read first;
	// the actual notification still gets exactly its production one-second call.
	readyCtx, readyCancel := context.WithTimeout(ctx, 10*time.Second)
	_, err = replica.Get(readyCtx, r.keys.changed)
	readyCancel()
	if err != nil {
		t.Fatalf("replica did not reconnect to restarted etcd: %v", err)
	}
	if err := r.Notify(ctx); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, term.reload)
}
