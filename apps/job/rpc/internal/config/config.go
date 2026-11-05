package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/tokyolab/dogx/pkg/postgres"
	"github.com/tokyolab/dogx/pkg/rpchost"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	Postgres           postgres.Config
	Scheduler          SchedulerConf
	ExecutionRPC       zrpc.RpcServerConf
	ResultPersistence  ResultPersistenceConf
	Dispatch           DispatchConf
	InstanceCheck      InstanceCheckConf
	InstanceRecovery   InstanceRecoveryConf
	UnboundRecovery    UnboundRecoveryConf
	Stopping           StoppingConf
	CancelNotification CancelNotificationConf
}

// StoppingConf separates database cancellation workers from elected discovery.
// RunTimeout is a yield budget: a started batch keeps its per-SQL budget and must
// actually finish before its worker is released. Discovery never owns execution.
type StoppingConf struct {
	Capacity         int           `json:",default=256"`
	Concurrency      int           `json:",default=2"`
	BatchSize        int           `json:",default=256"`
	MaxBatches       int           `json:",default=4"`
	RunTimeout       time.Duration `json:",default=2s"`
	SQLTimeout       time.Duration `json:",default=1s"`
	RecoveryPageSize int           `json:",default=256"`
	RecoveryTimeout  time.Duration `json:",default=5s"`
}

func (c StoppingConf) Validate() error {
	if c.Capacity <= 0 || c.Concurrency <= 0 || c.BatchSize <= 0 || c.MaxBatches <= 0 || c.RunTimeout <= 0 || c.SQLTimeout <= 0 ||
		c.RecoveryPageSize <= 0 || c.RecoveryTimeout <= 0 {
		return errors.New("instance stopping settings must be positive")
	}
	return nil
}

// CancelNotificationConf controls best-effort notification independently of
// database cancellation. Instance and target capacities include in-flight work.
// InstanceTimeout begins at local admission and is never extended by merging.
type CancelNotificationConf struct {
	InstanceCapacity int           `json:",default=256"`
	PageSize         int           `json:",default=32"`
	TargetCapacity   int           `json:",default=256"`
	Concurrency      int           `json:",default=16"`
	QueryTimeout     time.Duration `json:",default=1s"`
	InstanceTimeout  time.Duration `json:",default=60s"`
}

func (c CancelNotificationConf) Validate() error {
	if c.InstanceCapacity <= 0 || c.PageSize <= 0 || c.PageSize > 32 || c.TargetCapacity <= 0 || c.Concurrency <= 0 || c.QueryTimeout <= 0 || c.InstanceTimeout <= 0 {
		return errors.New("cancel notification settings must be positive and address page size at most 32")
	}
	return nil
}

// UnboundRecoveryConf controls each node's serial observation scan independently
// of leadership and dispatch workers. ScanInterval starts after actual return;
// OperationTimeout also applies to each settlement read or independent commit.
// ObservationPeriod must match across nodes and remain consistent with dispatch
// budgets. Reaching it does not prove that the previous business attempt stopped.
type UnboundRecoveryConf struct {
	ScanInterval      time.Duration `json:",default=10s"`
	PageSize          int           `json:",default=128"`
	RunTimeout        time.Duration `json:",default=5s"`
	OperationTimeout  time.Duration `json:",default=1s"`
	ObservationPeriod time.Duration `json:",default=30s"`
}

func (c UnboundRecoveryConf) Validate() error {
	if c.ScanInterval <= 0 || c.PageSize <= 0 || c.RunTimeout <= 0 || c.OperationTimeout <= 0 || c.ObservationPeriod <= 0 ||
		c.ScanInterval > time.Duration((1<<63-1)/6*5) {
		return errors.New("unbound recovery limits must be positive and the jittered interval representable")
	}
	return nil
}

// InstanceCheckConf bounds local completion hints independently of result saves.
// Capacity includes waiting and active IDs. Timeout starts when a check begins;
// the fixed internal merge window and queue wait are outside that budget.
type InstanceCheckConf struct {
	Capacity    int           `json:",default=512"`
	Concurrency int           `json:",default=8"`
	Timeout     time.Duration `json:",default=2s"`
}

func (c InstanceCheckConf) Validate() error {
	if c.Capacity <= 0 || c.Concurrency <= 0 || c.Timeout <= 0 {
		return errors.New("instance check limits must be positive")
	}
	return nil
}

// InstanceRecoveryConf supplies the common leader discovery interval and the
// RUNNING branch's independent scan budgets. STOPPING retains its own page and
// pass settings. The four-minute start-age filter is a fixed recovery rule, not
// an execution deadline or a guarantee of prompt timeout processing.
type InstanceRecoveryConf struct {
	Interval   time.Duration `json:",default=2m"`
	PageSize   int           `json:",default=256"`
	RunTimeout time.Duration `json:",default=5s"`
	SQLTimeout time.Duration `json:",default=1s"`
}

func (c InstanceRecoveryConf) Validate() error {
	if c.Interval <= 0 || c.PageSize <= 0 || c.RunTimeout <= 0 || c.SQLTimeout <= 0 {
		return errors.New("instance recovery settings must be positive")
	}
	return nil
}

// DispatchConf controls process-wide dispatch supply and each admission attempt.
// WaitingCapacity includes reserved query pages and hints, but not work already
// taken by the fixed consumers. These limits are independent of leadership,
// Instance creation and executor business concurrency.
type DispatchConf struct {
	Concurrency     int           `json:",default=64"`
	WaitingCapacity int           `json:",default=256"`
	ScanInterval    time.Duration `json:",default=1s"`
	TotalTimeout    time.Duration `json:",default=10s"`
	RPCTimeout      time.Duration `json:",default=3s"`
	BusyDelay       time.Duration `json:",default=5s"`
}

func (c DispatchConf) Validate() error {
	if c.Concurrency <= 0 || c.WaitingCapacity <= 0 || c.ScanInterval <= 0 || c.TotalTimeout <= 0 ||
		c.RPCTimeout < time.Millisecond || c.BusyDelay <= 0 ||
		c.ScanInterval > time.Duration((1<<63-1)/6*5) || c.BusyDelay > time.Duration((1<<63-1)/6*5) {
		return errors.New("dispatch limits must be positive, RPC timeout at least one millisecond, and jittered durations representable")
	}
	return nil
}

// ResultPersistenceConf bounds accepted results waiting for a database outcome.
// Capacity includes queued, retrying and in-flight results. Concurrency limits
// independent saves, each of which has its own SaveTimeout budget.
type ResultPersistenceConf struct {
	Capacity    int           `json:",default=512"`
	Concurrency int           `json:",default=8"`
	SaveTimeout time.Duration `json:",default=2s"`
}

func (c ResultPersistenceConf) Validate() error {
	if c.Capacity <= 0 || c.Concurrency <= 0 || c.SaveTimeout <= 0 {
		return errors.New("result persistence capacity, concurrency and save timeout must be positive")
	}
	return nil
}

// PrepareExecution validates execution transport, dispatch, recovery and result persistence
// settings, then gives both RPC servers the host's process-wide ServiceConf.
// Call it after normalizing the host shutdown policy and before constructing
// either server. It leaves the configuration unchanged on error.
//
// Collision checks compare literal listeners and shared etcd endpoints only;
// they do not resolve DNS aliases or discover whether different endpoints belong
// to the same cluster. ExecutionRPC.Name is required by go-zero's decoder, but
// its process settings are replaced by the top-level ServiceConf on success.
func (c *Config) PrepareExecution() error {
	if c == nil {
		return errors.New("job configuration is required")
	}
	if err := c.ResultPersistence.Validate(); err != nil {
		return err
	}
	if err := c.Dispatch.Validate(); err != nil {
		return err
	}
	if err := c.InstanceCheck.Validate(); err != nil {
		return err
	}
	if err := c.InstanceRecovery.Validate(); err != nil {
		return err
	}
	if err := c.UnboundRecovery.Validate(); err != nil {
		return err
	}
	if err := c.Stopping.Validate(); err != nil {
		return err
	}
	if err := c.CancelNotification.Validate(); err != nil {
		return err
	}
	if c.ExecutionRPC.Auth {
		return errors.New("execution RPC must disable native Auth; executor groups authenticate result reports")
	}
	if err := rpchost.ValidateListener(c.ExecutionRPC.ListenOn); err != nil {
		return fmt.Errorf("execution RPC: %w", err)
	}
	if rpchost.ListenersOverlap(c.ListenOn, c.ExecutionRPC.ListenOn) {
		return errors.New("execution RPC ListenOn overlaps the management RPC listener")
	}
	if err := rpchost.ValidateDiscovery(c.ExecutionRPC.Etcd); err != nil {
		return fmt.Errorf("execution RPC: %w", err)
	}
	if rpchost.DiscoveryOverlap(c.Etcd, c.ExecutionRPC.Etcd) {
		return errors.New("execution and management RPC must use different keys on the same etcd cluster")
	}
	c.ExecutionRPC.ServiceConf = c.ServiceConf
	return nil
}

type SchedulerConf struct {
	Prefix              string
	QueueCapacity       int           `json:",default=1024"`
	CreationConcurrency int           `json:",default=32"`
	CreationBudget      time.Duration `json:",default=3s"`
	LeaseTTLSeconds     int           `json:",default=15"`
	RecoveryLimit       int           `json:",default=1024"`
	QueryTimeout        time.Duration `json:",default=5s"`
}

func (c SchedulerConf) Validate() error {
	if c.QueueCapacity <= 0 || c.CreationConcurrency <= 0 || c.CreationBudget <= 0 || c.LeaseTTLSeconds <= 0 || c.RecoveryLimit <= 0 || c.QueryTimeout <= 0 {
		return errors.New("scheduler capacities and budgets must be positive")
	}
	return nil
}
