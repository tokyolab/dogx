package svc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/database"
	"github.com/tokyolab/dogx/apps/job/internal/service"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/config"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/scheduler"
	"github.com/zeromicro/go-zero/core/logx"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ServiceContext struct {
	Config    config.Config
	Jobs      JobService
	Clock     func() time.Time
	cancel    context.CancelFunc
	done      chan struct{}
	etcd      *clientv3.Client
	sqlDB     *sql.DB
	closeOnce sync.Once
	closeErr  error
}

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
	db, sqlDB, err := database.OpenPostgres(c.Postgres)
	if err != nil {
		_ = etcd.Close()
		return nil, err
	}
	var runtime *scheduler.Runtime
	jobs, err := service.New(db, service.WithCreationLimits(c.Scheduler.CreationConcurrency, c.Scheduler.CreationBudget), service.WithNotifier(func(ctx context.Context) error { return runtime.Notify(ctx) }))
	if err != nil {
		_ = sqlDB.Close()
		_ = etcd.Close()
		return nil, err
	}
	runtime, err = scheduler.New(etcd, jobs, scheduler.Config{Prefix: c.Scheduler.Prefix, QueueCapacity: c.Scheduler.QueueCapacity, CreationConcurrency: c.Scheduler.CreationConcurrency, LeaseTTLSeconds: c.Scheduler.LeaseTTLSeconds, RecoveryLimit: c.Scheduler.RecoveryLimit, QueryTimeout: c.Scheduler.QueryTimeout})
	if err != nil {
		_ = sqlDB.Close()
		_ = etcd.Close()
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	result := &ServiceContext{Config: c, Jobs: jobs, cancel: cancel, done: make(chan struct{}), etcd: etcd, sqlDB: sqlDB}
	go func() {
		defer close(result.done)
		if err := runtime.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			logx.Errorf("job scheduler stopped: %v", err)
		}
	}()
	return result, nil
}

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
