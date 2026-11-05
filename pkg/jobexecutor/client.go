package jobexecutor

import (
	"errors"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/zeromicro/go-zero/zrpc"
)

// NewReportClient constructs the executor's shared result-reporting connection.
// Target selection follows RpcClientConf, but dialing is always nonblocking so
// an unavailable scheduler does not prevent the host's business RPC startup.
// Successful construction does not prove scheduler or result-buffer readiness.
// Native etcd target initialization still requires its discovery dependencies;
// nonblocking dialing does not bypass that cold-start requirement.
// Native App/Token authentication is rejected because group credentials belong
// to each protocol request. Request and response body logging is disabled for
// ReportTaskResult before constructing the client.
//
// The client uses the agreed two-second call timeout. Callers must also bound
// calls by the remaining shutdown grace period and must not extend this budget
// with a per-call timeout override. The caller owns Conn().Close(), creates a
// protocol.ResultReceiverClient from Conn(), and closes the connection only
// after its reporting work has stopped. This function starts no reporting loop.
func NewReportClient(c zrpc.RpcClientConf) (zrpc.Client, error) {
	if c.App != "" || c.Token != "" {
		return nil, errors.New("result client must not use native RPC authentication")
	}
	// go-zero's client duration interceptor otherwise logs the credential-bearing
	// request on failures and both messages on slow successful calls.
	zrpc.DontLogClientContentForMethod(protocol.ResultReceiver_ReportTaskResult_FullMethodName)
	c.NonBlock = true
	c.Timeout = (2 * time.Second).Milliseconds()
	c.Middlewares.Timeout = true
	return zrpc.NewClient(c)
}
