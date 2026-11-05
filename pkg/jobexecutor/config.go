package jobexecutor

import (
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/pkg/rpchost"
	"github.com/zeromicro/go-zero/zrpc"
)

// Config connects one executor group to its dedicated listener and the
// scheduler's result receiver. Group credentials belong to protocol requests,
// not native RPC authentication. Server and ReportClient use distinct etcd
// discovery targets; business RPC configuration remains owned by the host.
// Configuration decoding fills omitted Runtime fields; explicit values must
// be positive. Direct callers of NewRuntime can still use its zero-value defaults.
type Config struct {
	GroupName    string
	Token        string
	Server       zrpc.RpcServerConf
	ReportClient zrpc.RpcClientConf
	Runtime      RuntimeConfig
}

// Validate checks credentials, runtime limits and both communication endpoints
// without creating connections or changing the configuration. App/Token auth,
// direct endpoints and arbitrary targets are not part of executor discovery.
// Errors intentionally omit configuration values, including malformed ones.
func (c Config) Validate() error {
	if err := ValidateCredentials(c.GroupName, c.Token); err != nil {
		return err
	}
	if c.Runtime.Concurrency <= 0 || c.Runtime.QueueCapacity <= 0 || c.Runtime.ReportCapacity <= 0 || c.Runtime.ReportConcurrency <= 0 {
		return errors.New("configured executor runtime limits must be positive")
	}
	if c.Server.Auth {
		return errors.New("executor server must not enable native RPC authentication")
	}
	if err := rpchost.ValidateListener(c.Server.ListenOn); err != nil {
		return fmt.Errorf("executor server: %w", err)
	}
	if err := rpchost.ValidateDiscovery(c.Server.Etcd); err != nil {
		return fmt.Errorf("executor server: %w", err)
	}
	if c.ReportClient.App != "" || c.ReportClient.Token != "" {
		return errors.New("result client must not use native RPC authentication")
	}
	if len(c.ReportClient.Endpoints) != 0 || c.ReportClient.Target != "" {
		return errors.New("result client must use etcd discovery without Endpoints or Target")
	}
	if err := rpchost.ValidateDiscovery(c.ReportClient.Etcd); err != nil {
		return fmt.Errorf("result client: %w", err)
	}
	if rpchost.DiscoveryOverlap(c.Server.Etcd, c.ReportClient.Etcd) {
		return errors.New("executor server and result client must use different keys on the same etcd cluster")
	}
	return nil
}

// Prepare validates the executor and checks its separation from the host's
// business RPC endpoint, then copies the host-owned ServiceConf into Server.
// Call it after normalizing the host shutdown policy and before constructing
// services. Server.Name is required by the native decoder, but all its process
// settings come from the host after preparation. Failure changes neither input.
//
// Conflict checks are static and do not resolve DNS aliases or discover etcd
// cluster membership. Deployments must also verify those relationships. The
// host's native authentication and its other transport settings are preserved.
func (c *Config) Prepare(host zrpc.RpcServerConf) error {
	if c == nil {
		return errors.New("executor configuration is required")
	}
	if err := c.Validate(); err != nil {
		return err
	}
	if rpchost.ListenersOverlap(host.ListenOn, c.Server.ListenOn) {
		return errors.New("executor server ListenOn overlaps the host RPC listener")
	}
	if rpchost.DiscoveryOverlap(host.Etcd, c.Server.Etcd) {
		return errors.New("executor and host RPC servers must use different keys on the same etcd cluster")
	}
	if rpchost.DiscoveryOverlap(host.Etcd, c.ReportClient.Etcd) {
		return errors.New("result client must not target the host business RPC discovery key")
	}
	c.Server.ServiceConf = host.ServiceConf
	return nil
}
