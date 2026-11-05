package jobexecutor

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/rpchost"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
)

// NewServer creates the executor's separate RPC listener. The host must supply
// the same ServiceConf as its other servers, including logging and shutdown
// settings. Group authentication belongs to the adapter; native App/Token auth
// is rejected. Health is available without credentials and is not admission
// readiness. Reflection and application business services are not registered.
// DispatchTask uses its positive MethodTimeouts setting, or three seconds when
// omitted; deployments must align it with the scheduler's Dispatch.RPCTimeout.
// CancelInstance always uses one second. Duplicate method timeout entries and
// a nonpositive explicit DispatchTask timeout are rejected before construction.
//
// The returned server owns only inbound RPCs. The host must also stop admission,
// drain accepted work within its shared deadline, and release dependencies only
// after their users have exited. Constructing this server starts no work queue.
//
// Adapter is required but is not evaluated during construction. It is called
// once during service registration after Start begins. The host must finish
// dependency initialization before starting the server, as rpchost.Run does with
// its Initialize hook. The provider must promptly return an initialized adapter;
// it must not start background work or wait for initialization. A nil result is
// an assembly error and panics during Start, which rpchost.Run reports as a startup
// failure.
func NewServer(c zrpc.RpcServerConf, adapter func() *RPCAdapter) (*rpchost.Server, error) {
	if adapter == nil {
		return nil, errors.New("executor RPC adapter provider must not be nil")
	}
	if c.Auth {
		return nil, errors.New("executor server must not enable native RPC authentication")
	}
	var err error
	c, err = executorServerConfig(c)
	if err != nil {
		return nil, err
	}
	server, err := rpchost.NewServer(c, func(server *grpc.Server) {
		initialized := adapter()
		if initialized == nil {
			panic(errors.New("jobexecutor: RPC adapter provider returned nil after initialization"))
		}
		protocol.RegisterExecutorServer(server, initialized)
	})
	if err != nil {
		return nil, err
	}
	server.AddUnaryInterceptors(bizerror.UnaryServerInterceptor)
	return server, nil
}

func executorServerConfig(c zrpc.RpcServerConf) (zrpc.RpcServerConf, error) {
	dispatchTimeout := 3 * time.Second
	methods := make(map[string]struct{}, len(c.MethodTimeouts))
	for _, setting := range c.MethodTimeouts {
		if _, duplicate := methods[setting.FullMethod]; duplicate {
			return zrpc.RpcServerConf{}, fmt.Errorf("executor server has duplicate method timeout for %s", setting.FullMethod)
		}
		methods[setting.FullMethod] = struct{}{}
		if setting.FullMethod == protocol.Executor_DispatchTask_FullMethodName {
			if setting.Timeout <= 0 {
				return zrpc.RpcServerConf{}, errors.New("executor DispatchTask method timeout must be positive")
			}
			dispatchTimeout = setting.Timeout
		}
	}
	c.Health = true
	// A positive base timeout installs zrpc's timeout interceptor, which is also
	// required for MethodTimeouts to take effect when the supplied base is zero.
	c.Timeout = 2000
	// Copy slice-backed settings: sibling servers may share the host's config.
	c.Middlewares.StatConf.IgnoreContentMethods = slices.Clone(c.Middlewares.StatConf.IgnoreContentMethods)
	c.MethodTimeouts = slices.Clone(c.MethodTimeouts)
	for _, setting := range []zrpc.MethodTimeoutConf{
		{FullMethod: protocol.Executor_DispatchTask_FullMethodName, Timeout: dispatchTimeout},
		{FullMethod: protocol.Executor_CancelInstance_FullMethodName, Timeout: time.Second},
	} {
		if !slices.Contains(c.Middlewares.StatConf.IgnoreContentMethods, setting.FullMethod) {
			c.Middlewares.StatConf.IgnoreContentMethods = append(c.Middlewares.StatConf.IgnoreContentMethods, setting.FullMethod)
		}
		// Each method overrides the positive base timeout; dispatch retains its
		// deployment budget while cancellation keeps the agreed fixed limit.
		c.MethodTimeouts = slices.DeleteFunc(c.MethodTimeouts, func(existing zrpc.MethodTimeoutConf) bool {
			return existing.FullMethod == setting.FullMethod
		})
		c.MethodTimeouts = append(c.MethodTimeouts, setting)
	}
	return c, nil
}
