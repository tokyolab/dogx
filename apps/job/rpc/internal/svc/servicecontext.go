package svc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/dispatch"
	"github.com/tokyolab/dogx/apps/job/internal/groupcache"
	"github.com/tokyolab/dogx/apps/job/internal/instancecheck"
	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/taskrecovery"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/apps/job/internal/trigger"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/config"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/execution"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/scheduler"
	"github.com/tokyolab/dogx/pkg/postgres"
	"github.com/zeromicro/go-zero/core/logx"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gorm.io/gorm"
)

// InstanceStopper records a manual stop and returns only committed facts.
// Callers publish post-commit hints only for a successful new decision. An error
// may mean uncertain commit and cannot authorize notification or claim rollback.
type InstanceStopper interface {
	RequestManual(context.Context, int64) (instancestop.Decision, error)
}

// ServiceContext assembles data access, trigger, and scheduler lifecycle
// dependencies for Job RPC. Instances created by NewServiceContext own their
// database connection, etcd client, group clients, scheduler, group cache, dispatch
// workers, unbound scans, result consumers, Instance checks and stop processing.
// Stop closes new admission and
// cancels discovery, while Drain gives accepted work the host's
// remaining shutdown budget. Wait observes actual completion;
// none of these operations closes dependencies. Callers must stop and drain
// inbound RPCs before Close and must not mutate dependencies while running.
type ServiceContext struct {
	Config              config.Config
	JobRepo             repository.JobDefinitionRepository
	InstanceRepo        repository.JobInstanceRepository
	TaskRepo            repository.JobTaskRepository
	GroupRepo           repository.ExecutorGroupRepository
	GroupCache          *groupcache.Cache
	Results             *taskresult.Queue
	InstanceChecks      *instancecheck.Queue
	InstanceStopper     InstanceStopper
	Stops               *instancestop.Queue
	CancelNotifications *execution.Notifier
	// AfterInstanceStopped hands a confirmed new decision to independent local
	// database and remote-notification queues. It performs no I/O or waiting;
	// rejection does not undo the stop. Rediscovery of an existing STOPPING
	// decision only repairs database convergence and never republishes this event.
	AfterInstanceStopped func(instancestop.Decision)
	ResultReceiver       *execution.Receiver
	Dispatch             *dispatch.Runtime
	UnboundRecovery      *taskrecovery.Runtime
	ExecutorClients      *execution.GroupClients
	Transactions         repository.TransactionRunner
	Trigger              ManualTrigger
	// NotifyJobChanged sends a configuration-reload hint only after a management
	// transaction's commit is confirmed. Nil means no notifier is installed.
	// Success does not confirm leader reload; failure does not roll back the
	// committed change. Periodic reconciliation provides recovery.
	NotifyJobChanged func(context.Context) error
	// Clock is an optional, concurrency-safe time source for management use
	// cases. It does not replace the scheduler Runtime's clock.
	Clock     func() time.Time
	cancel    context.CancelFunc
	done      chan struct{}
	etcd      *clientv3.Client
	sqlDB     *sql.DB
	stopOnce  sync.Once
	closeOnce sync.Once
	closeErr  error
}

// NewServiceContext validates configuration, opens independent connections, and
// loads the complete executor-group snapshot before starting the election and
// periodic group refresh loops. It does not wait for leadership. Result admission
// and its real consumers are ready before return. Ordinary first dispatch,
// retry supply, unbound observation and parent completion run on every node
// independently of election. Manual stopping and cancellation notifications are
// process-owned. Leader discovery resumes old RUNNING rounds and persisted stopping decisions;
// process-owned checks coordinate normal completion and automatic timeout.
// Diagnostics are safe to log.
// Initialization failures release acquired resources; on success, the returned
// instance's Close owns their cleanup.
func NewServiceContext(c config.Config) (*ServiceContext, error) {
	if err := c.Etcd.Validate(); err != nil {
		return nil, fmt.Errorf("job discovery: %w", err)
	}
	if err := c.Scheduler.Validate(); err != nil {
		return nil, err
	}
	if err := c.ResultPersistence.Validate(); err != nil {
		return nil, err
	}
	if err := c.Dispatch.Validate(); err != nil {
		return nil, err
	}
	if err := c.InstanceCheck.Validate(); err != nil {
		return nil, err
	}
	if err := c.InstanceRecovery.Validate(); err != nil {
		return nil, err
	}
	if err := c.UnboundRecovery.Validate(); err != nil {
		return nil, err
	}
	if err := c.Stopping.Validate(); err != nil {
		return nil, err
	}
	if err := c.CancelNotification.Validate(); err != nil {
		return nil, err
	}
	etcdConfig := clientv3.Config{Endpoints: c.Etcd.Hosts, Username: c.Etcd.User, Password: c.Etcd.Pass, DialTimeout: 5 * time.Second}
	if c.Etcd.HasTLS() {
		tlsConfig, err := (transport.TLSInfo{CertFile: c.Etcd.CertFile, KeyFile: c.Etcd.CertKeyFile, TrustedCAFile: c.Etcd.CACertFile, InsecureSkipVerify: c.Etcd.InsecureSkipVerify}).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("etcd TLS: %w", err)
		}
		etcdConfig.TLS = tlsConfig
	}
	etcd, err := clientv3.New(etcdConfig)
	if err != nil {
		return nil, err
	}
	db, sqlDB, err := postgres.Open(c.Postgres)
	if err != nil {
		_ = etcd.Close()
		return nil, err
	}
	result := &ServiceContext{Config: c, etcd: etcd, sqlDB: sqlDB}
	ready := false
	defer func() {
		// Ownership is established before assembling components. Initialization
		// errors or panics cannot strand a pool, connection or started consumer.
		if !ready {
			_ = result.Close()
		}
	}()
	jobs, err := repository.NewJobDefinitionRepository(db)
	if err != nil {
		return nil, err
	}
	transactions, err := repository.NewTransactionRunner(db)
	if err != nil {
		return nil, err
	}
	groups, err := repository.NewExecutorGroupRepository(db)
	if err != nil {
		return nil, err
	}
	groupCache, err := groupcache.New(context.Background(), groups, c.Scheduler.QueryTimeout)
	if err != nil {
		return nil, fmt.Errorf("initialize executor group cache: %w", err)
	}
	result.JobRepo, result.GroupRepo, result.GroupCache, result.Transactions = jobs, groups, groupCache, transactions
	clients, err := execution.NewGroupClients(groupCache, c.Etcd, c.Dispatch.RPCTimeout)
	if err != nil {
		return nil, fmt.Errorf("initialize executor clients: %w", err)
	}
	result.ExecutorClients = clients
	sender, err := execution.NewSender(clients, c.Dispatch.RPCTimeout)
	if err != nil {
		return nil, err
	}
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		return nil, err
	}
	tasks, err := repository.NewJobTaskRepository(db)
	if err != nil {
		return nil, err
	}
	result.InstanceRepo, result.TaskRepo = instances, tasks
	stopper, err := instancestop.NewController(transactions, instancestop.WithSQLTimeout(c.Stopping.SQLTimeout), instancestop.WithBatchSize(c.Stopping.BatchSize))
	if err != nil {
		return nil, err
	}
	result.InstanceStopper = stopper
	result.Stops, err = instancestop.NewQueue(stopper, instancestop.Config{
		Capacity: c.Stopping.Capacity, Concurrency: c.Stopping.Concurrency,
		RunTimeout: c.Stopping.RunTimeout, MaxBatches: c.Stopping.MaxBatches,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize instance stopping queue: %w", err)
	}
	canceller, err := execution.NewCanceller(groupCache)
	if err != nil {
		return nil, err
	}
	result.CancelNotifications, err = execution.NewNotifier(tasks, canceller, execution.NotificationConfig{
		InstanceCapacity: c.CancelNotification.InstanceCapacity, PageSize: c.CancelNotification.PageSize,
		TargetCapacity: c.CancelNotification.TargetCapacity, Concurrency: c.CancelNotification.Concurrency,
		QueryTimeout: c.CancelNotification.QueryTimeout, InstanceTimeout: c.CancelNotification.InstanceTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize cancel notifications: %w", err)
	}
	result.AfterInstanceStopped = result.afterInstanceStopped
	stopRecovery, err := instancestop.NewRecovery(instances, result.Stops.Hint, instancestop.RecoveryConfig{
		PageSize:   c.Stopping.RecoveryPageSize,
		RunTimeout: c.Stopping.RecoveryTimeout, SQLTimeout: c.Stopping.SQLTimeout,
	})
	if err != nil {
		return nil, err
	}
	checker, err := instancecheck.NewChecker(instances, instancecheck.WithStopping(stopper,
		result.afterInstanceStopped, func(id int64) { result.Stops.Hint(id) }))
	if err != nil {
		return nil, err
	}
	result.InstanceChecks, err = instancecheck.NewQueue(checker, instancecheck.Config{
		Capacity: c.InstanceCheck.Capacity, Concurrency: c.InstanceCheck.Concurrency, Timeout: c.InstanceCheck.Timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize instance checks: %w", err)
	}
	if err := result.InstanceChecks.Start(); err != nil {
		return nil, err
	}
	runningRecovery, err := instancecheck.NewRecovery(instances, result.InstanceChecks, instancecheck.RecoveryConfig{
		PageSize: c.InstanceRecovery.PageSize, RunTimeout: c.InstanceRecovery.RunTimeout,
		SQLTimeout: c.InstanceRecovery.SQLTimeout, MinAge: 4 * time.Minute,
	})
	if err != nil {
		return nil, err
	}
	hintCompletion := func(id int64) { result.InstanceChecks.Hint(id) }
	settler, err := taskrecovery.NewSettler(instances, tasks,
		taskrecovery.WithObservationPeriod(c.UnboundRecovery.ObservationPeriod),
		taskrecovery.WithOperationTimeout(c.UnboundRecovery.OperationTimeout),
		taskrecovery.WithInstanceHint(hintCompletion))
	if err != nil {
		return nil, err
	}
	result.UnboundRecovery, err = taskrecovery.NewRuntime(instances, tasks, settler, taskrecovery.Config{
		ScanInterval: c.UnboundRecovery.ScanInterval, PageSize: c.UnboundRecovery.PageSize,
		RunTimeout: c.UnboundRecovery.RunTimeout, OperationTimeout: c.UnboundRecovery.OperationTimeout,
		ObservationPeriod: c.UnboundRecovery.ObservationPeriod,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize unbound recovery: %w", err)
	}
	registrar, err := dispatch.NewRegistrar(transactions, dispatch.WithRegistrationHint(hintCompletion))
	if err != nil {
		return nil, err
	}
	result.Dispatch, err = dispatch.NewRuntime(instances, registrar, sender, tasks, dispatch.RuntimeConfig{
		Concurrency: c.Dispatch.Concurrency, WaitingCapacity: c.Dispatch.WaitingCapacity,
		ScanInterval: c.Dispatch.ScanInterval, QueryTimeout: c.Scheduler.QueryTimeout,
		TotalTimeout: c.Dispatch.TotalTimeout, BusyDelay: c.Dispatch.BusyDelay,
	}, hintCompletion)
	if err != nil {
		return nil, fmt.Errorf("initialize dispatch: %w", err)
	}
	// One process-scoped creator shares its limits between manual requests and
	// every elected term; leadership changes must not reset creation permits.
	creator, err := trigger.New(transactions, trigger.WithCreationLimits(c.Scheduler.CreationConcurrency, c.Scheduler.CreationBudget),
		trigger.WithInstanceHint(func(id int64) { result.Dispatch.Hint(id) }))
	if err != nil {
		return nil, err
	}
	runtime, err := scheduler.New(etcd, jobs, creator, scheduler.Config{Prefix: c.Scheduler.Prefix, QueueCapacity: c.Scheduler.QueueCapacity, CreationConcurrency: c.Scheduler.CreationConcurrency, LeaseTTLSeconds: c.Scheduler.LeaseTTLSeconds, RecoveryLimit: c.Scheduler.RecoveryLimit, QueryTimeout: c.Scheduler.QueryTimeout}, scheduler.WithInstanceRecovery(func(ctx context.Context) error {
		return runInstanceRecovery(ctx, c.InstanceRecovery.Interval, runningRecovery.Run, stopRecovery.Run)
	}))
	if err != nil {
		return nil, err
	}
	results, receiver, err := newResultReceiver(db, groupCache, c.ResultPersistence, hintCompletion)
	if err != nil {
		return nil, fmt.Errorf("initialize result receiver: %w", err)
	}
	result.Results, result.ResultReceiver, result.Trigger, result.NotifyJobChanged = results, receiver, creator, runtime.Notify
	if err := result.Stops.Start(); err != nil {
		return nil, err
	}
	if err := result.CancelNotifications.Start(); err != nil {
		return nil, err
	}
	if err := result.Dispatch.Start(); err != nil {
		return nil, err
	}
	if err := result.UnboundRecovery.Start(); err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	result.cancel, result.done = cancel, make(chan struct{})
	go func() {
		defer close(result.done)
		// Every node refreshes credentials regardless of election ownership.
		// Keep the database alive until both loops have actually returned.
		var workers sync.WaitGroup
		workers.Go(func() {
			if err := runtime.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
				logx.Errorf("job scheduler stopped: %v", err)
			}
		})
		workers.Go(func() {
			if err := groupCache.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
				logx.Errorf("executor group cache stopped: %v", err)
			}
		})
		workers.Wait()
	}()
	ready = true
	return result, nil
}

// Stop cancels unbound observation, closes dispatch, result, completion and stop
// admission, and cancels scheduler and group refresh without waiting. Accepted
// dispatches, result saves, Instance checks and stop work retain independent work
// Contexts until Drain's shared budget expires. It does not stop inbound RPCs or
// close connections.
// Concurrent and repeated calls are safe; Wait observes actual completion.
func (s *ServiceContext) Stop() {
	s.stopOnce.Do(func() {
		if s.UnboundRecovery != nil {
			s.UnboundRecovery.Stop()
		}
		if s.Dispatch != nil {
			s.Dispatch.Stop()
		}
		if s.Results != nil {
			s.Results.Stop()
		}
		if s.InstanceChecks != nil {
			// Hints are best-effort accelerators. Results committed after admission
			// closes remain durable for Instance recovery; shutdown never rolls them
			// back or runs parent completion inside a result persistence worker.
			s.InstanceChecks.Stop()
		}
		if s.Stops != nil {
			s.Stops.Stop()
		}
		if s.CancelNotifications != nil {
			s.CancelNotifications.Stop()
		}
		if s.cancel != nil {
			s.cancel()
		}
	})
}

// Drain closes new work and drains dispatch metadata, results, checks and stopping
// concurrently with producer cleanup, using the host's existing non-nil shutdown
// Context. All consumers receive that budget before waiting for scheduler or
// cache cleanup. Nil confirms all have finished; expiration does not authorize closing dependencies still in
// use. It must be paired with the host's inbound RPC completion barrier.
func (s *ServiceContext) Drain(ctx context.Context) error {
	s.Stop()
	// All accepted metadata paths need the shared deadline immediately; one
	// stalled dependency must not delay cancellation of the other component.
	var workers sync.WaitGroup
	errs := make(chan error, 7)
	workers.Go(func() { errs <- s.waitRuntime(ctx) })
	if s.UnboundRecovery != nil {
		workers.Go(func() { errs <- s.UnboundRecovery.Wait(ctx) })
	}
	if s.Results != nil {
		workers.Go(func() { errs <- s.Results.Drain(ctx) })
	}
	if s.Dispatch != nil {
		workers.Go(func() { errs <- s.Dispatch.Drain(ctx) })
	}
	if s.InstanceChecks != nil {
		workers.Go(func() { errs <- s.InstanceChecks.Drain(ctx) })
	}
	if s.Stops != nil {
		workers.Go(func() { errs <- s.Stops.Drain(ctx) })
	}
	if s.CancelNotifications != nil {
		workers.Go(func() { errs <- s.CancelNotifications.Drain(ctx) })
	}
	workers.Wait()
	close(errs)
	var result error
	for err := range errs {
		result = errors.Join(result, err)
	}
	return result
}

// Wait waits for discovery, unbound scans, dispatches, results, checks and stopping
// using a non-nil Context.
// It does not request cancellation, install another deadline, or close shared
// etcd/database dependencies. A Context error means only that this caller stopped
// waiting; producers or saves may still be using those dependencies. Later calls may
// continue waiting. Already completed or absent work returns nil, even when
// the caller's Context has expired. Concurrent calls are safe.
func (s *ServiceContext) Wait(ctx context.Context) error {
	runtimeErr := s.waitRuntime(ctx)
	if s.UnboundRecovery != nil {
		runtimeErr = errors.Join(runtimeErr, s.UnboundRecovery.Wait(ctx))
	}
	if s.Dispatch != nil {
		runtimeErr = errors.Join(runtimeErr, s.Dispatch.Wait(ctx))
	}
	if s.Results != nil {
		runtimeErr = errors.Join(runtimeErr, s.Results.Wait(ctx))
	}
	if s.InstanceChecks != nil {
		runtimeErr = errors.Join(runtimeErr, s.InstanceChecks.Wait(ctx))
	}
	if s.Stops != nil {
		runtimeErr = errors.Join(runtimeErr, s.Stops.Wait(ctx))
	}
	if s.CancelNotifications != nil {
		runtimeErr = errors.Join(runtimeErr, s.CancelNotifications.Wait(ctx))
	}
	return runtimeErr
}

func (s *ServiceContext) afterInstanceStopped(decision instancestop.Decision) {
	if !decision.New || decision.EndReason == nil {
		return
	}
	// Admission is independent: one queue's fullness cannot suppress the other.
	// A repeated request or a recovery hint never enters this new-decision path.
	s.Stops.Hint(decision.InstanceID)
	s.CancelNotifications.Hint(execution.Notification{InstanceID: decision.InstanceID,
		ExecutorGroupID: decision.ExecutorGroupID, Reason: *decision.EndReason})
}

func (s *ServiceContext) waitRuntime(ctx context.Context) error {
	if s.done == nil {
		return nil
	}
	select {
	case <-s.done:
		return nil
	default:
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close calls Stop and waits without a deadline for background loops and consumers
// before closing group connections, the shared etcd client and database. It may block
// indefinitely when work has not exited or unsaved results keep retrying. Use
// Drain first to apply the shared shutdown budget. It does not stop either RPC
// server: callers must first stop accepting requests and drain every inbound or
// background user of these dependencies. A prior Wait timeout does not authorize
// closing dependencies early or make this method bounded.
//
// Concurrent or repeated calls wait for the same cleanup and return the same
// combined close error. Hosts enforcing a shutdown deadline must use Stop and
// Drain before invoking Close after the inbound drain barrier has completed.
func (s *ServiceContext) Close() error {
	s.closeOnce.Do(func() {
		s.Stop()
		// Keep connections alive until producers and accepted saves actually return.
		_ = s.Wait(context.Background())
		if s.ExecutorClients != nil {
			s.closeErr = errors.Join(s.closeErr, s.ExecutorClients.Close())
		}
		if s.etcd != nil {
			s.closeErr = errors.Join(s.closeErr, s.etcd.Close())
		}
		if s.sqlDB != nil {
			s.closeErr = errors.Join(s.closeErr, s.sqlDB.Close())
		}
	})
	return s.closeErr
}

// newResultReceiver assembles real persistence before opening admission. It reuses
// the host's pool and initial group snapshot, independent of elected-term lifetime.
// hint is nonblocking and receives only confirmed persistence or obsolete results;
// it must enqueue completion rather than querying the parent inline.
func newResultReceiver(db *gorm.DB, groups *groupcache.Cache, c config.ResultPersistenceConf, hint func(int64)) (*taskresult.Queue, *execution.Receiver, error) {
	tasks, err := repository.NewJobTaskRepository(db)
	if err != nil {
		return nil, nil, err
	}
	saver, err := taskresult.NewSaver(tasks, taskresult.WithSaveTimeout(c.SaveTimeout), taskresult.WithInstanceHint(hint))
	if err != nil {
		return nil, nil, err
	}
	auth, err := execution.NewCachedGroupAuthenticator(groups)
	if err != nil {
		return nil, nil, err
	}
	queue, err := taskresult.NewQueue(saver, taskresult.QueueConfig{Capacity: c.Capacity, Concurrency: c.Concurrency})
	if err != nil {
		return nil, nil, err
	}
	receiver, err := execution.NewReceiver(auth, queue)
	if err != nil {
		queue.Stop()
		return nil, nil, err
	}
	if err := queue.Start(); err != nil {
		queue.Stop()
		return nil, nil, err
	}
	return queue, receiver, nil
}
