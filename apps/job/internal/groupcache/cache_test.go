package groupcache

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

// These credentials are synthetic test fixtures, never deployed secrets.
const testToken = "11111111111141118111111111111111"

type sourceStub struct {
	mu        sync.Mutex
	groups    []repository.ExecutorGroupConfig
	loadErr   error
	lookupErr error
	loads     int
	ids       int
	names     int
	load      func(context.Context) ([]repository.ExecutorGroupConfig, error)
	find      func(context.Context, lookupKey) (*repository.ExecutorGroupConfig, error)
}

func (s *sourceStub) LoadExecutionGroups(ctx context.Context) ([]repository.ExecutorGroupConfig, error) {
	s.mu.Lock()
	s.loads++
	load, err := s.load, s.loadErr
	groups := append([]repository.ExecutorGroupConfig{}, s.groups...)
	s.mu.Unlock()
	if load != nil {
		return load(ctx)
	}
	return groups, err
}

func (s *sourceStub) FindExecutionGroupByID(ctx context.Context, id int64) (*repository.ExecutorGroupConfig, error) {
	return s.lookup(ctx, lookupKey{id: id})
}

func (s *sourceStub) FindExecutionGroupByName(ctx context.Context, name string) (*repository.ExecutorGroupConfig, error) {
	return s.lookup(ctx, lookupKey{name: name})
}

func (s *sourceStub) lookup(ctx context.Context, key lookupKey) (*repository.ExecutorGroupConfig, error) {
	s.mu.Lock()
	if key.id != 0 {
		s.ids++
	} else {
		s.names++
	}
	find, err := s.find, s.lookupErr
	groups := append([]repository.ExecutorGroupConfig{}, s.groups...)
	s.mu.Unlock()
	if find != nil {
		return find(ctx, key)
	}
	if err != nil {
		return nil, err
	}
	for _, group := range groups {
		if key.id == group.ID || key.name == group.Name {
			return &group, nil
		}
	}
	return nil, repository.ErrExecutorGroupUnavailable
}

func testGroup(id int64, name string) repository.ExecutorGroupConfig {
	return repository.ExecutorGroupConfig{ID: id, Name: name, DiscoveryKey: "test/" + name, Token: testToken}
}

func newCache(t *testing.T, source Source) *Cache {
	t.Helper()
	cache, err := New(context.Background(), source, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not complete")
		var zero T
		return zero
	}
}

func TestNewRequiresCompleteValidInitialSnapshot(t *testing.T) {
	var nilSource *sourceStub
	for _, source := range []Source{nil, nilSource} {
		if cache, err := New(context.Background(), source, time.Second); err == nil || cache != nil {
			t.Fatal("nil source created a cache")
		}
	}
	if cache, err := New(context.Background(), &sourceStub{}, 0); err == nil || cache != nil {
		t.Fatal("unbounded query timeout was accepted")
	}
	cause := errors.New("private driver diagnostic " + testToken)
	cache, err := New(context.Background(), &sourceStub{loadErr: cause}, time.Second)
	if cache != nil || !errors.Is(err, cause) || strings.Contains(err.Error(), testToken) {
		t.Fatal("initial load failure lost its cause or exposed credentials")
	}
	group := testGroup(1, "workers")
	for _, test := range []struct {
		name   string
		modify func(*repository.ExecutorGroupConfig)
	}{
		{"invalid ID", func(g *repository.ExecutorGroupConfig) { g.ID = 0 }},
		{"empty name", func(g *repository.ExecutorGroupConfig) { g.Name = "" }},
		{"invalid token", func(g *repository.ExecutorGroupConfig) { g.Token = "" }},
		{"empty key", func(g *repository.ExecutorGroupConfig) { g.DiscoveryKey = "" }},
		{"key whitespace", func(g *repository.ExecutorGroupConfig) { g.DiscoveryKey += " " }},
		{"key NUL", func(g *repository.ExecutorGroupConfig) { g.DiscoveryKey += "\x00" }},
		{"key invalid encoding", func(g *repository.ExecutorGroupConfig) { g.DiscoveryKey = string([]byte{0xff}) }},
		{"key too long", func(g *repository.ExecutorGroupConfig) { g.DiscoveryKey = strings.Repeat("界", 256) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := group
			test.modify(&invalid)
			if cache, err := New(context.Background(), &sourceStub{groups: []repository.ExecutorGroupConfig{group, invalid}}, time.Second); err == nil || cache != nil {
				t.Fatal("partial or invalid snapshot was accepted")
			}
		})
	}
	for _, duplicate := range []repository.ExecutorGroupConfig{
		{ID: group.ID, Name: "other", DiscoveryKey: "other", Token: testToken},
		{ID: 2, Name: group.Name, DiscoveryKey: "other", Token: testToken},
		{ID: 2, Name: "other", DiscoveryKey: group.DiscoveryKey, Token: testToken},
	} {
		if cache, err := New(context.Background(), &sourceStub{groups: []repository.ExecutorGroupConfig{group, duplicate}}, time.Second); err == nil || cache != nil {
			t.Fatal("duplicate snapshot identity was accepted")
		}
	}
}

func TestHitsMissesAndFullReplacement(t *testing.T) {
	first, second := testGroup(1, "one"), testGroup(2, "two")
	source := &sourceStub{groups: []repository.ExecutorGroupConfig{first}}
	cache := newCache(t, source)
	got, err := cache.ByName(context.Background(), first.Name)
	if err != nil || got != first {
		t.Fatal("initial group was not available")
	}
	got.Token = "mutated caller copy"
	if got, err = cache.ByID(context.Background(), first.ID); err != nil || got != first {
		t.Fatal("caller modified cached credentials")
	}
	source.mu.Lock()
	source.groups = []repository.ExecutorGroupConfig{first, second}
	source.mu.Unlock()
	if got, err = cache.ByName(context.Background(), second.Name); err != nil || got != second {
		t.Fatal("new group did not become available on a cache miss")
	}
	if got, err = cache.ByID(context.Background(), second.ID); err != nil || got != second {
		t.Fatal("miss did not populate both indexes")
	}
	source.mu.Lock()
	if source.ids != 0 || source.names != 1 {
		t.Errorf("unexpected source lookup counts: IDs=%d names=%d", source.ids, source.names)
	}
	source.groups = []repository.ExecutorGroupConfig{second}
	source.loadErr = errors.New("temporary snapshot failure")
	source.mu.Unlock()
	if err = cache.Refresh(context.Background()); err == nil {
		t.Fatal("source failure was hidden")
	}
	if got, err = cache.ByID(context.Background(), first.ID); err != nil || got != first {
		t.Fatal("failed refresh removed the old group")
	}
	source.mu.Lock()
	source.loadErr = nil
	source.mu.Unlock()
	if err = cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err = cache.ByName(context.Background(), first.Name); !errors.Is(err, ErrNotFound) {
			t.Fatal("deleted group remained available")
		}
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.names != 3 {
		t.Errorf("absence was cached: name queries=%d", source.names)
	}
}

func TestLookupFailuresRemainTechnicalAndPrivate(t *testing.T) {
	cause := errors.New("query failed with credential " + testToken)
	source := &sourceStub{lookupErr: cause}
	cache := newCache(t, source)
	_, err := cache.ByName(context.Background(), "workers")
	if !errors.Is(err, cause) || errors.Is(err, ErrNotFound) || strings.Contains(err.Error(), testToken) {
		t.Fatal("lookup failure was misclassified or exposed credentials")
	}
	source.mu.Lock()
	source.lookupErr = nil
	source.find = func(context.Context, lookupKey) (*repository.ExecutorGroupConfig, error) { return nil, nil }
	source.mu.Unlock()
	if _, err = cache.ByID(context.Background(), 1); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatal("invalid source success was classified as absence")
	}
	source.mu.Lock()
	source.find = func(context.Context, lookupKey) (*repository.ExecutorGroupConfig, error) {
		group := testGroup(2, "unexpected")
		return &group, nil
	}
	source.mu.Unlock()
	if _, err = cache.ByID(context.Background(), 1); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatal("wrong source identity was accepted")
	}
}

// waitingContext signals that a follower has reached its cancellation-aware
// flight wait. This avoids timing sleeps when proving concurrent coalescing.
type waitingContext struct {
	context.Context
	waiting chan<- struct{}
	once    sync.Once
}

func (c *waitingContext) Done() <-chan struct{} {
	c.once.Do(func() { c.waiting <- struct{}{} })
	return c.Context.Done()
}

func TestConcurrentMissesShareLookupAndFollowerCancellation(t *testing.T) {
	group := testGroup(1, "workers")
	entered, release := make(chan context.Context, 1), make(chan struct{})
	source := &sourceStub{find: func(ctx context.Context, _ lookupKey) (*repository.ExecutorGroupConfig, error) {
		entered <- ctx
		<-release
		return &group, nil
	}}
	cache := newCache(t, source)
	leader := make(chan error, 1)
	go func() { _, err := cache.ByName(context.Background(), group.Name); leader <- err }()
	sourceCtx := await(t, entered)
	const followers = 24
	waiting := make(chan struct{}, followers)
	results := make(chan error, followers)
	followerCtx, cancelFollower := context.WithCancel(context.Background())
	for range followers {
		go func() {
			_, err := cache.ByName(&waitingContext{Context: followerCtx, waiting: waiting}, group.Name)
			results <- err
		}()
	}
	for range followers {
		await(t, waiting)
	}
	cancelFollower()
	for range followers {
		if err := await(t, results); !errors.Is(err, context.Canceled) {
			t.Fatalf("follower cancellation = %v", err)
		}
	}
	if sourceCtx.Err() != nil {
		t.Fatal("follower cancellation canceled the initiating lookup")
	}
	close(release)
	if err := await(t, leader); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.names != 1 {
		t.Errorf("concurrent miss queries=%d", source.names)
	}
}

func TestLateMissCannotRestoreDeletedGroupAfterRefresh(t *testing.T) {
	group := testGroup(1, "workers")
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var lookups atomic.Int32
	source := &sourceStub{find: func(context.Context, lookupKey) (*repository.ExecutorGroupConfig, error) {
		if lookups.Add(1) == 1 {
			entered <- struct{}{}
			<-release
			return &group, nil
		}
		return nil, repository.ErrExecutorGroupUnavailable
	}}
	cache := newCache(t, source)
	result := make(chan error, 1)
	go func() { _, err := cache.ByName(context.Background(), group.Name); result <- err }()
	await(t, entered)
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := await(t, result); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale lookup result = %v", err)
	}
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	if len(cache.byID) != 0 || len(cache.byName) != 0 || lookups.Load() != 2 {
		t.Fatal("late source result was published after full replacement")
	}
}

func TestReplacementCanSatisfyInFlightMiss(t *testing.T) {
	group := testGroup(1, "workers")
	entered, release := make(chan struct{}, 1), make(chan struct{})
	source := &sourceStub{find: func(context.Context, lookupKey) (*repository.ExecutorGroupConfig, error) {
		entered <- struct{}{}
		<-release
		return nil, repository.ErrExecutorGroupUnavailable
	}}
	cache := newCache(t, source)
	result := make(chan error, 1)
	go func() { _, err := cache.ByID(context.Background(), group.ID); result <- err }()
	await(t, entered)
	source.mu.Lock()
	source.groups = []repository.ExecutorGroupConfig{group}
	source.mu.Unlock()
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := await(t, result); err != nil {
		t.Fatal("old absence overrode a newly loaded group")
	}
}

func TestEverySourceCallHasQueryBudget(t *testing.T) {
	budget := 25 * time.Millisecond
	check := func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > budget {
			t.Error("query has no effective budget")
		}
		<-ctx.Done()
		return ctx.Err()
	}
	source := &sourceStub{}
	cache, err := New(context.Background(), source, budget)
	if err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	source.load = func(ctx context.Context) ([]repository.ExecutorGroupConfig, error) { return nil, check(ctx) }
	source.find = func(ctx context.Context, _ lookupKey) (*repository.ExecutorGroupConfig, error) {
		return nil, check(ctx)
	}
	source.mu.Unlock()
	for _, operation := range []func() error{
		func() error { return cache.Refresh(context.Background()) },
		func() error { _, err := cache.ByName(context.Background(), "workers"); return err },
		func() error { _, err := cache.ByID(context.Background(), 1); return err },
	} {
		if err = operation(); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrNotFound) {
			t.Fatalf("budget result = %v", err)
		}
	}
}

func TestRefreshLoopCancellationWaitsForActualSourceReturn(t *testing.T) {
	source := &sourceStub{}
	cache := newCache(t, source)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	source.mu.Lock()
	source.load = func(context.Context) ([]repository.ExecutorGroupConfig, error) {
		entered <- struct{}{}
		<-release
		return nil, nil
	}
	source.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time, 2)
	result := make(chan error, 1)
	go func() { result <- cache.run(ctx, ticks) }()
	ticks <- time.Now()
	await(t, entered)
	// A queued tick must not start a second query while the first is blocked.
	ticks <- time.Now()
	cancel()
	select {
	case err := <-result:
		t.Fatalf("loop returned before the source: %v", err)
	default:
	}
	blockedCtx, cancelBlocked := context.WithCancel(context.Background())
	cancelBlocked()
	if err := cache.Refresh(blockedCtx); !errors.Is(err, context.Canceled) {
		t.Fatal("waiting refresh ignored cancellation")
	}
	close(release)
	if err := await(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("loop result = %v", err)
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.loads != 2 {
		t.Errorf("loop overlapped or ran after cancellation: loads=%d", source.loads)
	}
}

func TestRunIsSingleUseAndHonorsAlreadyCanceledContext(t *testing.T) {
	cache := newCache(t, &sourceStub{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cache.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	if err := cache.Run(context.Background()); err == nil {
		t.Fatal("second Run was accepted")
	}
}

func TestRefreshObserversOnlySeeSuccessfulPublication(t *testing.T) {
	first, second := testGroup(1, "one"), testGroup(2, "two")
	source := &sourceStub{groups: []repository.ExecutorGroupConfig{first}}
	cache := newCache(t, source)
	if !cache.Contains(first) || cache.Contains(second) || cache.Contains(repository.ExecutorGroupConfig{}) {
		t.Fatal("membership does not describe the current snapshot")
	}
	changed := first
	changed.DiscoveryKey = "different"
	if cache.Contains(changed) {
		t.Fatal("membership accepted a different group configuration")
	}
	var calls int
	unsubscribe := cache.OnRefresh(func() {
		calls++
		// The callback may read the already-published cache without deadlocking.
		if cache.Contains(first) || !cache.Contains(second) {
			t.Error("observer ran before complete replacement")
		}
	})
	if calls != 0 {
		t.Fatal("registration invoked the observer")
	}
	source.mu.Lock()
	source.groups = []repository.ExecutorGroupConfig{second}
	source.loadErr = errors.New("synthetic refresh failure")
	source.mu.Unlock()
	if err := cache.Refresh(context.Background()); err == nil || calls != 0 || !cache.Contains(first) {
		t.Fatal("failed refresh notified observers or removed state")
	}
	// A miss may add a newly created group without running cleanup observers.
	if _, err := cache.ByID(context.Background(), second.ID); err != nil || !cache.Contains(second) || calls != 0 {
		t.Fatal("miss loading did not preserve observer semantics")
	}
	source.mu.Lock()
	source.loadErr = nil
	source.mu.Unlock()
	if err := cache.Refresh(context.Background()); err != nil || calls != 1 {
		t.Fatal("successful refresh did not synchronously notify the observer")
	}
	unsubscribe()
	unsubscribe()
	cache.OnRefresh(nil)()
	if err := cache.Refresh(context.Background()); err != nil || calls != 1 {
		t.Fatal("unregistered observer was invoked")
	}
}
