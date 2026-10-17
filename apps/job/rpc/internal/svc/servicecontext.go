package svc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/trigger"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/config"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/scheduler"
	"github.com/tokyolab/dogx/pkg/postgres"
	"github.com/zeromicro/go-zero/core/logx"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ServiceContext assembles data access, trigger, and scheduler lifecycle
// dependencies for Job RPC. Instances created by NewServiceContext own their
// database connection, etcd client, and scheduler background work. Callers must
// stop RPC requests before calling Close and must not mutate public dependencies
// while the service is running.
type ServiceContext struct {
	Config       config.Config
	JobRepo      repository.JobDefinitionRepository
	Transactions repository.TransactionRunner
	Trigger      ManualTrigger
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
	closeOnce sync.Once
	closeErr  error
}

// NewServiceContext validates configuration, opens independent connections, and
// starts the election loop without waiting for leadership. Success does not
// imply completed election, configuration loading, or external dependency
// readiness; background scheduler failures are logged. Initialization failures
// release acquired resources; on success, the returned instance's Close owns
// their cleanup.
func NewServiceContext(c config.Config) (*ServiceContext, error) {
	if err := c.Etcd.Validate(); err != nil {
		return nil, fmt.Errorf("job discovery: %w", err)
	}
	if err := c.Scheduler.Validate(); err != nil {
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
	jobs, err := repository.NewJobDefinitionRepository(db)
	if err != nil {
		_ = sqlDB.Close()
		_ = etcd.Close()
		return nil, err
	}
	transactions, err := repository.NewTransactionRunner(db)
	if err != nil {
		_ = sqlDB.Close()
		_ = etcd.Close()
		return nil, err
	}
	// One process-scoped creator shares its limits between manual requests and
	// every elected term; leadership changes must not reset creation permits.
	creator, err := trigger.New(transactions, trigger.WithCreationLimits(c.Scheduler.CreationConcurrency, c.Scheduler.CreationBudget))
	if err != nil {
		_ = sqlDB.Close()
		_ = etcd.Close()
		return nil, err
	}
	runtime, err := scheduler.New(etcd, jobs, creator, scheduler.Config{Prefix: c.Scheduler.Prefix, QueueCapacity: c.Scheduler.QueueCapacity, CreationConcurrency: c.Scheduler.CreationConcurrency, LeaseTTLSeconds: c.Scheduler.LeaseTTLSeconds, RecoveryLimit: c.Scheduler.RecoveryLimit, QueryTimeout: c.Scheduler.QueryTimeout})
	if err != nil {
		_ = sqlDB.Close()
		_ = etcd.Close()
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	result := &ServiceContext{Config: c, JobRepo: jobs, Transactions: transactions, Trigger: creator, NotifyJobChanged: runtime.Notify, cancel: cancel, done: make(chan struct{}), etcd: etcd, sqlDB: sqlDB}
	go func() {
		defer close(result.done)
		if err := runtime.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			logx.Errorf("job scheduler stopped: %v", err)
		}
	}()
	return result, nil
}

// Close cancels and waits for the scheduler loop before closing the shared etcd
// client and database connection. It does not stop the RPC server; callers must
// first stop accepting requests and finish requests using these dependencies.
// Concurrent or repeated calls wait for the same cleanup and return the same
// combined close error. There is no separate wait deadline: unfinished scheduler
// cancellation keeps Close waiting rather than releasing connections too early.
func (s *ServiceContext) Close() error {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		// Keep shared clients alive until the old term has released every callback and database operation.
		if s.done != nil {
			<-s.done
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
