package execution

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/groupcache"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
)

var errGroupClientsClosed = errors.New("executor group clients are closed")

// GroupClients owns one lazily created discovery connection per active group.
// Executor borrows connections; deletion may close them during an RPC. Successful
// complete group-cache refreshes remove obsolete clients before closing them,
// without an additional query, polling loop, or wait for in-flight RPCs.
//
// Methods support concurrent use. The owner stops refreshes and waits for dispatch
// workers to return before Close during ordinary shutdown. Close itself does not
// wait for ongoing construction or borrowed RPCs; a late construction cannot
// publish after Close and must close its connection before returning.
type GroupClients struct {
	cache       *groupcache.Cache
	config      zrpc.RpcClientConf
	rpcTimeout  time.Duration
	create      groupConnectionFactory
	unsubscribe func()
	closeOnce   sync.Once
	closeErr    error
	closedCh    chan struct{}

	mu      sync.Mutex
	closed  bool
	clients map[int64]*groupConnection
	flights map[int64]*groupCreation
}

// groupConnection owns its client and close operation after successful creation.
// Only the cache or the creating caller owns it at a time; removal transfers
// ownership out of the cache before close is called without holding its mutex.
type groupConnection struct {
	group  repository.ExecutorGroupConfig
	client protocol.ExecutorClient
	close  func() error
}

// groupConnectionFactory creates synchronously. It must return a complete owned
// connection on success, and release partial resources before returning an error.
// It cannot honor a caller Context because zrpc construction has no such argument.
type groupConnectionFactory func(zrpc.RpcClientConf, time.Duration) (*groupConnection, error)

// groupCreation coalesces construction for one group. Its initiating caller owns
// the synchronous factory call; followers may cancel only their own wait. err is
// immutable after done closes and a failed construction is not cached.
type groupCreation struct {
	done chan struct{}
	err  error
}

var _ ExecutorProvider = (*GroupClients)(nil)

// NewGroupClients binds deployment discovery settings to the existing group
// cache without opening connections. Each group's persisted DiscoveryKey replaces
// etcd.Key; Hosts and optional etcd credentials/TLS settings are deployment-owned.
// Group tokens belong to protocol messages, never native RPC authentication.
// rpcTimeout must be at least one millisecond and is passed to NewExecutorClient.
// Successful
// construction does not prove etcd or any executor is reachable.
func NewGroupClients(cache *groupcache.Cache, etcd discov.EtcdConf, rpcTimeout time.Duration) (*GroupClients, error) {
	if cache == nil {
		return nil, errors.New("executor group cache must not be nil")
	}
	if rpcTimeout < time.Millisecond {
		return nil, errors.New("executor RPC timeout must be at least one millisecond")
	}
	if len(etcd.Hosts) == 0 {
		return nil, errors.New("executor discovery hosts must not be empty")
	}
	for _, host := range etcd.Hosts {
		if host == "" || host != strings.TrimSpace(host) {
			return nil, errors.New("executor discovery host is invalid")
		}
	}
	var config zrpc.RpcClientConf
	if err := conf.FillDefault(&config); err != nil {
		return nil, &groupClientError{operation: "initialize executor client configuration", cause: err}
	}
	etcd.Hosts = append([]string(nil), etcd.Hosts...)
	etcd.Key = ""
	config.Etcd = etcd
	config.NonBlock = true
	clients := &GroupClients{
		cache: cache, config: config, rpcTimeout: rpcTimeout, create: createGroupConnection,
		clients: make(map[int64]*groupConnection), flights: make(map[int64]*groupCreation),
		closedCh: make(chan struct{}),
	}
	clients.unsubscribe = cache.OnRefresh(clients.removeObsolete)
	return clients, nil
}

func createGroupConnection(config zrpc.RpcClientConf, rpcTimeout time.Duration) (*groupConnection, error) {
	client, err := NewExecutorClient(config, rpcTimeout)
	if err != nil {
		return nil, err
	}
	return &groupConnection{client: protocol.NewExecutorClient(client.Conn()), close: client.Conn().Close}, nil
}

// Executor resolves id from the group cache and borrows its shared client. Cache
// misses and follower waits respect ctx. The first caller constructs the client
// synchronously: go-zero's initial discovery can outlast ctx even with nonblocking
// dialing. That caller retains its processing slot until construction actually
// returns, then checks ctx, closure and current membership before publication.
// Construction failure is returned without retry; a later call may try again.
func (p *GroupClients) Executor(ctx context.Context, id int64) (ExecutorTarget, error) {
	if ctx == nil {
		return ExecutorTarget{}, errors.New("executor lookup context must not be nil")
	}
	for {
		if err := ctx.Err(); err != nil {
			return ExecutorTarget{}, err
		}
		select {
		case <-p.closedCh:
			return ExecutorTarget{}, errGroupClientsClosed
		default:
		}
		group, err := p.cache.ByID(ctx, id)
		if err != nil {
			return ExecutorTarget{}, err
		}
		p.mu.Lock()
		if err := p.available(ctx, group); err != nil {
			p.mu.Unlock()
			return ExecutorTarget{}, err
		}
		if existing := p.clients[id]; existing != nil && existing.group == group {
			target := ExecutorTarget{Group: group, Client: existing.client}
			p.mu.Unlock()
			return target, nil
		}
		if pending := p.flights[id]; pending != nil {
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return ExecutorTarget{}, ctx.Err()
			case <-p.closedCh:
				return ExecutorTarget{}, errGroupClientsClosed
			case <-pending.done:
				if pending.err != nil {
					return ExecutorTarget{}, pending.err
				}
				continue
			}
		}
		pending := &groupCreation{done: make(chan struct{})}
		p.flights[id] = pending
		old := p.clients[id]
		delete(p.clients, id)
		p.mu.Unlock()
		p.closeRemoved(old)
		return p.construct(ctx, group, pending)
	}
}

// available requires p.mu. Cache never invokes observers with its own mutex
// held, so the lock order is always pool then cache. A refresh published after
// this check must acquire p.mu in cleanup and remove any just-installed client
// before Refresh returns; creation finishing after that cleanup fails this check.
func (p *GroupClients) available(ctx context.Context, group repository.ExecutorGroupConfig) error {
	if p.closed {
		return errGroupClientsClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.cache.Contains(group) {
		return groupcache.ErrNotFound
	}
	return nil
}

func (p *GroupClients) construct(ctx context.Context, group repository.ExecutorGroupConfig, pending *groupCreation) (ExecutorTarget, error) {
	config := p.config
	config.Etcd.Key = group.DiscoveryKey
	connection, err := p.create(config, p.rpcTimeout)
	if err != nil {
		err = &groupClientError{operation: "create executor group client", cause: err}
	} else if connection == nil || nilExecutorDependency(connection.client) || connection.close == nil {
		err = errors.New("executor client factory returned an incomplete connection")
	}
	p.mu.Lock()
	if err == nil {
		err = p.available(ctx, group)
	}
	if err == nil {
		connection.group = group
		p.clients[group.ID] = connection
	}
	p.mu.Unlock()
	if err != nil {
		p.closeRemoved(connection)
	}
	p.mu.Lock()
	pending.err = err
	delete(p.flights, group.ID)
	close(pending.done)
	p.mu.Unlock()
	if err != nil {
		return ExecutorTarget{}, err
	}
	return ExecutorTarget{Group: group, Client: connection.client}, nil
}

func (p *GroupClients) removeObsolete() {
	p.mu.Lock()
	var removed []*groupConnection
	for id, client := range p.clients {
		if !p.cache.Contains(client.group) {
			delete(p.clients, id)
			removed = append(removed, client)
		}
	}
	p.mu.Unlock()
	for _, client := range removed {
		p.closeRemoved(client)
	}
}

func (p *GroupClients) closeRemoved(client *groupConnection) {
	if client != nil && client.close != nil {
		if err := client.close(); err != nil {
			// Dependency errors can contain deployment credentials; only the
			// operation and the non-sensitive group identity belong in this log.
			logx.Errorw("close executor group client failed", logx.Field("executor_group_id", client.group.ID))
		}
	}
}

// Close permanently prevents new borrows and publication, unregisters refresh
// cleanup, removes cached clients, and closes them without waiting for RPCs.
// Concurrent repeated calls wait for this same close operation and share its
// result. An already-running refresh callback or factory retains responsibility
// for resources it removed or created; ordinary shutdown must stop those owners
// first when it needs proof that all connection cleanup has completed.
func (p *GroupClients) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.closedCh)
		removed := p.clients
		p.clients = make(map[int64]*groupConnection)
		p.mu.Unlock()
		p.unsubscribe()
		for _, client := range removed {
			if err := client.close(); err != nil {
				p.closeErr = errors.Join(p.closeErr, &groupClientError{operation: "close executor group client", cause: err})
			}
		}
	})
	return p.closeErr
}

// groupClientError preserves dependency identity without exposing connection
// settings or third-party diagnostics through normal error formatting.
type groupClientError struct {
	operation string
	cause     error
}

func (e *groupClientError) Error() string { return e.operation + " failed" }
func (e *groupClientError) Unwrap() error { return e.cause }
