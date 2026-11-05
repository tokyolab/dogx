package execution

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/groupcache"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/zrpc"
)

func TestGroupClientsRequireCacheDiscoveryAndTimeout(t *testing.T) {
	_, cache, _ := newGroupClientFixture(t)
	for _, test := range []struct {
		cache   *groupcache.Cache
		etcd    discov.EtcdConf
		timeout time.Duration
	}{
		{nil, discov.EtcdConf{Hosts: []string{"127.0.0.1:2379"}}, time.Second},
		{cache, discov.EtcdConf{}, time.Second},
		{cache, discov.EtcdConf{Hosts: []string{" "}}, time.Second},
		{cache, discov.EtcdConf{Hosts: []string{"127.0.0.1:2379"}}, 0},
	} {
		if clients, err := NewGroupClients(test.cache, test.etcd, test.timeout); clients != nil || err == nil {
			t.Fatal("invalid group-client dependencies were accepted")
		}
	}
}

func TestGroupClientsCloseBeforeFirstUse(t *testing.T) {
	source, cache, group := newGroupClientFixture(t)
	clients := newTestGroupClients(t, cache)
	clients.create = func(zrpc.RpcClientConf, time.Duration) (*groupConnection, error) {
		t.Error("closed provider started construction")
		return nil, errors.New("unexpected construction")
	}
	if err := clients.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.Executor(context.Background(), group.ID); !errors.Is(err, errGroupClientsClosed) {
		t.Fatal("closed provider accepted its first borrow")
	}
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal("closing the provider disrupted future cache refreshes")
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.lookups != 0 {
		t.Fatal("closed provider queried the group source")
	}
}

func TestGroupClientsConcurrentReuseAndDeploymentConfiguration(t *testing.T) {
	source, cache, group := newGroupClientFixture(t)
	etcd := discov.EtcdConf{
		Hosts: []string{"127.0.0.1:2379"}, Key: "deployment-key-is-not-a-group-key",
		User: "synthetic-etcd-user", Pass: "synthetic-etcd-secret",
		CertFile: "synthetic-cert", CertKeyFile: "synthetic-key", CACertFile: "synthetic-ca",
	}
	clients, err := NewGroupClients(cache, etcd, 7*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clients.Close() })
	etcd.Hosts[0] = "caller-mutated-host"
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var creations, closes atomic.Int32
	client := &senderClientStub{}
	clients.create = func(config zrpc.RpcClientConf, timeout time.Duration) (*groupConnection, error) {
		creations.Add(1)
		if config.Etcd.Hosts[0] != "127.0.0.1:2379" || config.Etcd.Key != group.DiscoveryKey ||
			config.Etcd.User != etcd.User || config.Etcd.Pass != etcd.Pass || config.Etcd.CertFile != etcd.CertFile ||
			config.Etcd.CertKeyFile != etcd.CertKeyFile || config.Etcd.CACertFile != etcd.CACertFile ||
			config.App != "" || config.Token != "" || config.Target != "" || len(config.Endpoints) != 0 ||
			!config.NonBlock || timeout != 7*time.Second {
			t.Error("group client configuration lost deployment settings or used the wrong target/authentication")
		}
		entered <- struct{}{}
		<-release
		return &groupConnection{client: client, close: func() error { closes.Add(1); return nil }}, nil
	}
	const count = 20
	results := make(chan groupClientResult, count)
	for range count {
		go func() {
			target, err := clients.Executor(context.Background(), group.ID)
			results <- groupClientResult{target, err}
		}()
	}
	groupClientAwait(t, entered)
	close(release)
	for range count {
		result := groupClientAwait(t, results)
		if result.err != nil || result.target.Client != client || result.target.Group != group {
			t.Fatal("concurrent callers did not borrow the same group client")
		}
	}
	if creations.Load() != 1 || closes.Load() != 0 {
		t.Fatal("concurrent reuse created or closed extra clients")
	}
	source.mu.Lock()
	lookups := source.lookups
	source.mu.Unlock()
	if lookups != 0 {
		t.Fatal("cached group client borrowing queried the database")
	}
	if err := clients.Close(); err != nil || closes.Load() != 1 {
		t.Fatal("close did not release the sole owned connection")
	}
	if err := clients.Close(); err != nil || closes.Load() != 1 {
		t.Fatal("repeated close released the connection again")
	}
}

func TestGroupClientsRefreshRetainsValidAndRemovesBeforeClose(t *testing.T) {
	source, cache, group := newGroupClientFixture(t)
	clients := newTestGroupClients(t, cache)
	var closes atomic.Int32
	closeEntered, closeRelease := make(chan struct{}, 1), make(chan struct{})
	client := &senderClientStub{}
	clients.create = func(zrpc.RpcClientConf, time.Duration) (*groupConnection, error) {
		return &groupConnection{client: client, close: func() error {
			closes.Add(1)
			clients.mu.Lock()
			_, present := clients.clients[group.ID]
			clients.mu.Unlock()
			if present {
				t.Error("deleted connection was closed before removal from the client map")
			}
			closeEntered <- struct{}{}
			<-closeRelease
			return nil
		}}, nil
	}
	if _, err := clients.Executor(context.Background(), group.ID); err != nil {
		t.Fatal(err)
	}
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	source.groups = nil
	source.loadErr = errors.New("synthetic snapshot failure")
	source.mu.Unlock()
	if err := cache.Refresh(context.Background()); err == nil {
		t.Fatal("failed full refresh was accepted")
	}
	if target, err := clients.Executor(context.Background(), group.ID); err != nil || target.Client != client || closes.Load() != 0 {
		t.Fatal("successful unchanged or failed refresh disrupted a reusable client")
	}
	source.mu.Lock()
	source.loadErr = nil
	source.mu.Unlock()
	finished := make(chan error, 1)
	go func() { finished <- cache.Refresh(context.Background()) }()
	groupClientAwait(t, closeEntered)
	if _, err := clients.Executor(context.Background(), group.ID); !errors.Is(err, groupcache.ErrNotFound) {
		t.Fatal("deleted client remained borrowable while its close was running")
	}
	select {
	case <-finished:
		t.Fatal("refresh returned before its synchronous cleanup completed")
	default:
	}
	close(closeRelease)
	if err := groupClientAwait(t, finished); err != nil || closes.Load() != 1 {
		t.Fatal("successful deletion refresh did not finish exactly one close")
	}
}

func TestGroupClientsLateConstructionCannotUndoDeletionOrClose(t *testing.T) {
	for _, change := range []string{"delete", "close", "cancel"} {
		t.Run(change, func(t *testing.T) {
			source, cache, group := newGroupClientFixture(t)
			clients := newTestGroupClients(t, cache)
			entered, release := make(chan struct{}, 1), make(chan struct{})
			var closes atomic.Int32
			clients.create = func(zrpc.RpcClientConf, time.Duration) (*groupConnection, error) {
				entered <- struct{}{}
				<-release
				return &groupConnection{client: &senderClientStub{}, close: func() error { closes.Add(1); return nil }}, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finished := make(chan error, 1)
			go func() { _, err := clients.Executor(ctx, group.ID); finished <- err }()
			groupClientAwait(t, entered)
			var want error
			switch change {
			case "delete":
				source.mu.Lock()
				source.groups = nil
				source.mu.Unlock()
				if err := cache.Refresh(context.Background()); err != nil {
					t.Fatal(err)
				}
				want = groupcache.ErrNotFound
			case "close":
				if err := clients.Close(); err != nil {
					t.Fatal(err)
				}
				want = errGroupClientsClosed
			case "cancel":
				cancel()
				want = context.Canceled
			}
			select {
			case <-finished:
				t.Fatal("initiating caller released its processing slot before the factory returned")
			default:
			}
			close(release)
			if err := groupClientAwait(t, finished); !errors.Is(err, want) || closes.Load() != 1 {
				t.Fatalf("late construction error=%v close count=%d", err, closes.Load())
			}
			clients.mu.Lock()
			remaining := len(clients.clients)
			clients.mu.Unlock()
			if remaining != 0 {
				t.Fatal("late construction republished an obsolete connection")
			}
		})
	}
}

func TestGroupClientsFollowerCancellationDoesNotCancelCreation(t *testing.T) {
	_, cache, group := newGroupClientFixture(t)
	clients := newTestGroupClients(t, cache)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var creations atomic.Int32
	clients.create = func(zrpc.RpcClientConf, time.Duration) (*groupConnection, error) {
		creations.Add(1)
		entered <- struct{}{}
		<-release
		return &groupConnection{client: &senderClientStub{}, close: func() error { return nil }}, nil
	}
	first := make(chan error, 1)
	go func() { _, err := clients.Executor(context.Background(), group.ID); first <- err }()
	groupClientAwait(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiting := make(chan struct{}, 1)
	follower := make(chan error, 1)
	go func() {
		_, err := clients.Executor(&groupClientWaitingContext{Context: ctx, waiting: waiting}, group.ID)
		follower <- err
	}()
	groupClientAwait(t, waiting)
	cancel()
	if err := groupClientAwait(t, follower); !errors.Is(err, context.Canceled) || creations.Load() != 1 {
		t.Fatal("follower cancellation interrupted or duplicated construction")
	}
	close(release)
	if err := groupClientAwait(t, first); err != nil {
		t.Fatal("follower cancellation failed the initiating caller")
	}
}

func TestGroupClientsLateSourceLookupCannotCreateDeletedGroup(t *testing.T) {
	source, cache, group := newGroupClientFixture(t)
	source.mu.Lock()
	source.groups = nil
	source.mu.Unlock()
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var queries, creations atomic.Int32
	source.mu.Lock()
	source.find = func(context.Context, int64) (*repository.ExecutorGroupConfig, error) {
		if queries.Add(1) == 1 {
			entered <- struct{}{}
			<-release
			return &group, nil
		}
		return nil, repository.ErrExecutorGroupUnavailable
	}
	source.mu.Unlock()
	clients := newTestGroupClients(t, cache)
	clients.create = func(zrpc.RpcClientConf, time.Duration) (*groupConnection, error) {
		creations.Add(1)
		return &groupConnection{client: &senderClientStub{}, close: func() error { return nil }}, nil
	}
	finished := make(chan error, 1)
	go func() { _, err := clients.Executor(context.Background(), group.ID); finished <- err }()
	groupClientAwait(t, entered)
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := groupClientAwait(t, finished); !errors.Is(err, groupcache.ErrNotFound) || creations.Load() != 0 || queries.Load() != 2 {
		t.Fatal("late pre-deletion source data reached connection construction")
	}
}

func TestGroupClientsNewGroupLoadsWithoutWaitingForRefresh(t *testing.T) {
	source, cache, group := newGroupClientFixture(t)
	source.mu.Lock()
	source.groups = nil
	source.mu.Unlock()
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	clients := newTestGroupClients(t, cache)
	var creations, closes atomic.Int32
	clients.create = func(zrpc.RpcClientConf, time.Duration) (*groupConnection, error) {
		creations.Add(1)
		return &groupConnection{client: &senderClientStub{}, close: func() error { closes.Add(1); return nil }}, nil
	}
	source.mu.Lock()
	source.groups = []repository.ExecutorGroupConfig{group}
	source.mu.Unlock()
	if _, err := clients.Executor(context.Background(), group.ID); err != nil {
		t.Fatal("a group created after the complete snapshot could not be used")
	}
	clients.removeObsolete()
	if _, err := clients.Executor(context.Background(), group.ID); err != nil || creations.Load() != 1 || closes.Load() != 0 {
		t.Fatal("cleanup discarded a newly loaded current group")
	}
}

func TestGroupClientsFailedCreationIsPrivateAndNotCached(t *testing.T) {
	_, cache, group := newGroupClientFixture(t)
	clients := newTestGroupClients(t, cache)
	cause := errors.New("synthetic-private-discovery-detail")
	var calls int
	clients.create = func(zrpc.RpcClientConf, time.Duration) (*groupConnection, error) {
		calls++
		if calls == 1 {
			return nil, cause
		}
		return &groupConnection{client: &senderClientStub{}, close: func() error { return nil }}, nil
	}
	if _, err := clients.Executor(context.Background(), group.ID); !errors.Is(err, cause) || strings.Contains(err.Error(), cause.Error()) || calls != 1 {
		t.Fatal("creation failure was retried or exposed private diagnostics")
	}
	if _, err := clients.Executor(context.Background(), group.ID); err != nil || calls != 2 {
		t.Fatal("a failed construction poisoned later calls")
	}
}

type groupClientResult struct {
	target ExecutorTarget
	err    error
}

// groupClientWaitingContext reports the follower's cancellation-aware wait.
type groupClientWaitingContext struct {
	context.Context
	waiting chan<- struct{}
	once    sync.Once
}

func (c *groupClientWaitingContext) Done() <-chan struct{} {
	c.once.Do(func() { c.waiting <- struct{}{} })
	return c.Context.Done()
}

// groupClientSourceStub owns all group-source methods used by these tests.
type groupClientSourceStub struct {
	mu      sync.Mutex
	groups  []repository.ExecutorGroupConfig
	loadErr error
	lookups int
	find    func(context.Context, int64) (*repository.ExecutorGroupConfig, error)
}

func (s *groupClientSourceStub) LoadExecutionGroups(context.Context) ([]repository.ExecutorGroupConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]repository.ExecutorGroupConfig(nil), s.groups...), s.loadErr
}

func (s *groupClientSourceStub) FindExecutionGroupByID(ctx context.Context, id int64) (*repository.ExecutorGroupConfig, error) {
	s.mu.Lock()
	s.lookups++
	find := s.find
	groups := append([]repository.ExecutorGroupConfig(nil), s.groups...)
	s.mu.Unlock()
	if find != nil {
		return find(ctx, id)
	}
	for _, group := range groups {
		if group.ID == id {
			return &group, nil
		}
	}
	return nil, repository.ErrExecutorGroupUnavailable
}

func (s *groupClientSourceStub) FindExecutionGroupByName(context.Context, string) (*repository.ExecutorGroupConfig, error) {
	panic("group clients resolve the persisted group ID, never its name")
}

func newGroupClientFixture(t *testing.T) (*groupClientSourceStub, *groupcache.Cache, repository.ExecutorGroupConfig) {
	t.Helper()
	group := repository.ExecutorGroupConfig{ID: 1, Name: "test-workers", DiscoveryKey: "test/worker.rpc", Token: receiverTestToken}
	source := &groupClientSourceStub{groups: []repository.ExecutorGroupConfig{group}}
	cache, err := groupcache.New(context.Background(), source, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return source, cache, group
}

func newTestGroupClients(t *testing.T, cache *groupcache.Cache) *GroupClients {
	t.Helper()
	clients, err := NewGroupClients(cache, discov.EtcdConf{Hosts: []string{"127.0.0.1:2379"}}, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clients.Close() })
	return clients
}

func groupClientAwait[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("group client operation did not complete")
		var zero T
		return zero
	}
}
