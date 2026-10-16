// Package jobrpc isolates optional Job discovery from System startup.
package jobrpc

import (
	"context"
	"sync"
	"time"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type connection interface {
	grpc.ClientConnInterface
	Close() error
}

type initialization struct {
	done chan struct{}
	conn connection
	err  error
}

type lazyConnection struct {
	mu      sync.Mutex
	closed  chan struct{}
	factory func() (connection, error)
	conn    connection
	pending *initialization
	timeout time.Duration
}

type Client struct {
	jobclient.Job
	connection *lazyConnection
}

// New does not dial or start a discovery goroutine. Existing System-only
// deployments may omit JobRpc until this feature is deployed.
func New(config zrpc.RpcClientConf) *Client {
	config.NonBlock = true
	protectContent(zrpc.DontLogClientContentForMethod)
	client := newClient(func() (connection, error) {
		client, err := zrpc.NewClient(config)
		if err != nil {
			return nil, err
		}
		return client.Conn(), nil
	})
	if config.Timeout > 0 && config.Timeout <= int64((1<<63-1)/time.Millisecond) {
		client.connection.timeout = time.Duration(config.Timeout) * time.Millisecond
	}
	return client
}

func newClient(factory func() (connection, error)) *Client {
	conn := &lazyConnection{factory: factory, closed: make(chan struct{}), timeout: 5 * time.Second}
	return &Client{Job: job.NewJobClient(conn), connection: conn}
}

func (c *Client) Close() error { return c.connection.Close() }

func (c *lazyConnection) get(ctx context.Context) (connection, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	c.mu.Lock()
	select {
	case <-c.closed:
		c.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "job client is closed")
	default:
	}
	if c.conn != nil {
		conn := c.conn
		c.mu.Unlock()
		return conn, nil
	}
	if c.pending == nil {
		c.pending = &initialization{done: make(chan struct{})}
		// go-zero v1.10.3's discovery initial load retries using its shared
		// etcd client's context, not this request's context. Keep at most ONE
		// initializer for this process-owned client even when requests time out.
		// Close cannot interrupt that library operation; a late connection is
		// closed below, and process exit releases a permanently stalled load.
		go c.initialize(c.pending)
	}
	pending := c.pending
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case <-c.closed:
		return nil, status.Error(codes.Unavailable, "job client is closed")
	case <-pending.done:
		return pending.conn, pending.err
	}
}

func (c *lazyConnection) initialize(pending *initialization) {
	conn, err := c.factory()
	c.mu.Lock()
	select {
	case <-c.closed:
		c.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		pending.err = status.Error(codes.Unavailable, "job client is closed")
		close(pending.done)
		return
	default:
	}
	if err != nil {
		// Do not expose the raw configuration/discovery error to logs or HTTP
		// callers: third-party errors may include endpoints or credentials.
		logx.Error("initialize Job RPC client failed")
		pending.err = status.Error(codes.Unavailable, "job service is unavailable")
	} else {
		c.conn = conn
		pending.conn = conn
	}
	c.pending = nil
	close(pending.done)
	c.mu.Unlock()
}

func (c *lazyConnection) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	// The go-zero timeout interceptor runs only after discovery. Include the
	// initial wait here too, retaining a shorter caller deadline when present.
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	conn, err := c.get(ctx)
	if err != nil {
		return err
	}
	return conn.Invoke(ctx, method, args, reply, opts...)
}

func protectContent(register func(string)) {
	// Parameters are arbitrary business input, and Get returns them in full.
	for _, method := range []string{
		job.Job_CreateJob_FullMethodName,
		job.Job_UpdateJob_FullMethodName,
		job.Job_GetJob_FullMethodName,
		job.Job_TriggerJob_FullMethodName,
	} {
		register(method)
	}
}

func (c *lazyConnection) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	conn, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	return conn.NewStream(ctx, desc, method, opts...)
}

func (c *lazyConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.closed:
		return nil
	default:
		close(c.closed)
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
