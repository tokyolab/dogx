package execution

import (
	"errors"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/zeromicro/go-zero/zrpc"
)

// NewExecutorClient constructs a scheduler connection for dispatch or directed
// cancellation. Target selection and nonblocking dialing follow RpcClientConf;
// successful construction does not prove executor readiness. Group credentials
// belong to protocol requests, so native App/Token authentication is rejected.
// Body logging is disabled for both executor methods before client construction.
//
// rpcTimeout supplies the positive timeout for the connection's purpose,
// overriding RpcClientConf.Timeout so its default cannot change that budget.
// Cancellation callers must supply a Context bounded to one second; all calls
// must also respect their remaining operation and shutdown budgets. Callers must
// not extend those budgets with a per-call timeout override. The caller owns Conn().Close() and
// creates a protocol.ExecutorClient from Conn(). A group's discovery connection
// is reused; a directed cancellation connection is closed after that target's
// notification and its bounded retries. This function starts no retry loop or
// application-level group client cache.
func NewExecutorClient(c zrpc.RpcClientConf, rpcTimeout time.Duration) (zrpc.Client, error) {
	if rpcTimeout < time.Millisecond {
		return nil, errors.New("executor RPC timeout must be at least one millisecond")
	}
	if c.App != "" || c.Token != "" {
		return nil, errors.New("executor client must not use native RPC authentication")
	}
	// go-zero's client duration interceptor otherwise logs credential-bearing
	// requests on failure and both messages on slow successful calls.
	zrpc.DontLogClientContentForMethod(protocol.Executor_DispatchTask_FullMethodName)
	zrpc.DontLogClientContentForMethod(protocol.Executor_CancelInstance_FullMethodName)
	// A conf-loaded default must not override the chosen RPC budget.
	c.Timeout = rpcTimeout.Milliseconds()
	c.Middlewares.Timeout = true
	return zrpc.NewClient(c)
}
