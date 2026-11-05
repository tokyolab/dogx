package execution

import (
	"errors"
	"slices"
	"time"

	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/rpchost"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
)

// NewServer creates the result-receiving listener independently of Job management
// RPCs. The host supplies the management server's ServiceConf to keep process
// settings consistent. Native App/Token auth is rejected: Receiver authenticates
// each business request while standard Health remains credential-free. Health
// does not assert group-cache, buffer or persistence readiness.
//
// The host owns receiver dependencies and must coordinate admission and draining
// with the returned server's bounded Shutdown. No management method or reflection
// service is registered, and construction does not start result consumers.
//
// Receiver is required but is not evaluated during construction. It is called
// once during service registration after Start begins. The host must finish
// dependency initialization before starting the server, as rpchost.Run does with
// its Initialize hook. The provider must return an initialized Receiver promptly;
// it must not start background work or wait for initialization. A nil result is
// an assembly error and panics during Start, which rpchost.Run reports as a startup
// failure.
func NewServer(c zrpc.RpcServerConf, receiver func() *Receiver) (*rpchost.Server, error) {
	if receiver == nil {
		return nil, errors.New("result receiver provider must not be nil")
	}
	if c.Auth {
		return nil, errors.New("result server must not enable native RPC authentication")
	}
	c = resultServerConfig(c)
	server, err := rpchost.NewServer(c, func(server *grpc.Server) {
		initialized := receiver()
		if initialized == nil {
			panic(errors.New("execution: result receiver provider returned nil after initialization"))
		}
		protocol.RegisterResultReceiverServer(server, initialized)
	})
	if err != nil {
		return nil, err
	}
	server.AddUnaryInterceptors(bizerror.UnaryServerInterceptor)
	return server, nil
}

func resultServerConfig(c zrpc.RpcServerConf) zrpc.RpcServerConf {
	c.Health = true
	c.Timeout = 2000
	c.Middlewares.StatConf.IgnoreContentMethods = slices.Clone(c.Middlewares.StatConf.IgnoreContentMethods)
	method := protocol.ResultReceiver_ReportTaskResult_FullMethodName
	if !slices.Contains(c.Middlewares.StatConf.IgnoreContentMethods, method) {
		c.Middlewares.StatConf.IgnoreContentMethods = append(c.Middlewares.StatConf.IgnoreContentMethods, method)
	}
	c.MethodTimeouts = slices.DeleteFunc(slices.Clone(c.MethodTimeouts), func(setting zrpc.MethodTimeoutConf) bool {
		return setting.FullMethod == method
	})
	c.MethodTimeouts = append(c.MethodTimeouts, zrpc.MethodTimeoutConf{FullMethod: method, Timeout: 2 * time.Second})
	return c
}
