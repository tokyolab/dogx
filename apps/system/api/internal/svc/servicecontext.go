package svc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/clientip"
	"github.com/tokyolab/dogx/apps/system/api/internal/config"
	"github.com/tokyolab/dogx/apps/system/api/internal/jobrpc"
	"github.com/tokyolab/dogx/apps/system/api/internal/middleware"
	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/authorization"
	"github.com/tokyolab/dogx/apps/system/internal/loginprotection"
	"github.com/tokyolab/dogx/apps/system/internal/rpclog"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/postgres"

	"github.com/casbin/casbin/v3"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type RedisPinger interface {
	PingCtx(ctx context.Context) bool
}

type ReadinessChecker interface {
	Check(ctx context.Context) error
}

type ServiceContext struct {
	ClientIP               *clientip.Resolver
	LoginRateLimit         rest.Middleware
	Config                 config.Config
	SystemRpc              systemclient.System
	JobRpc                 jobclient.Job
	Redis                  RedisPinger
	Sessions               authn.SessionReader
	SessionAuth            rest.Middleware
	Authorization          rest.Middleware
	AuthorizationEnforcer  *casbin.SyncedEnforcer
	AuthorizationReadiness ReadinessChecker

	policyWatcher   authorization.PolicyWatcher
	securityRuntime *loginprotection.Runtime
	policyReloader  *authorization.PolicyReloader
	cancelPolicy    context.CancelFunc
	sqlDB           *sql.DB
	jobClient       *jobrpc.Client
}

func NewServiceContext(c config.Config) (*ServiceContext, error) {
	rpclog.ProtectClientContent()
	resolver, err := clientip.New(c.Auth.TrustedProxies)
	if err != nil {
		return nil, fmt.Errorf("trusted proxies: %w", err)
	}
	systemRPC := systemclient.NewSystem(zrpc.MustNewClient(c.SystemRpc))
	redisClient, err := redis.NewRedis(c.RedisConf)
	if err != nil {
		return nil, err
	}
	sessions, err := authn.NewRedisSessionReader(redisClient, c.Auth.SessionKeyPrefix)
	if err != nil {
		return nil, err
	}

	database, sqlDB, err := postgres.Open(c.Postgres)
	if err != nil {
		return nil, err
	}
	closeDatabase := true
	defer func() {
		if closeDatabase {
			_ = sqlDB.Close()
		}
	}()

	adapter, err := authorization.NewGormAdapter(database)
	if err != nil {
		return nil, err
	}
	policyModel, err := authorization.NewModel()
	if err != nil {
		return nil, err
	}
	enforcer, err := casbin.NewSyncedEnforcer(policyModel)
	if err != nil {
		return nil, fmt.Errorf("initialize Casbin enforcer: %w", err)
	}
	enforcer.SetAdapter(adapter)
	// The API enforcer is read-only; system-rpc owns transactional policy writes.
	enforcer.EnableAutoSave(false)
	reloader, err := authorization.NewPolicyReloader(enforcer)
	if err != nil {
		return nil, err
	}
	if err := reloader.Reload(); err != nil {
		return nil, fmt.Errorf("initial authorization policy load: %w", err)
	}

	watcher, err := authorization.NewRedisPolicyWatcher(
		c.RedisConf,
		c.Authorization.PolicyChannel,
		func(string) {
			if err := reloader.Reload(); err != nil {
				logx.Errorf("reload authorization policy after Redis notification: %v", err)
			}
		},
	)
	if err != nil {
		return nil, fmt.Errorf("initialize authorization policy watcher: %w", err)
	}
	policyContext, cancelPolicy := context.WithCancel(context.Background())
	if err := reloader.Start(policyContext, c.Authorization.ReloadInterval, func(err error) {
		logx.Errorf("periodically reload authorization policy: %v", err)
	}); err != nil {
		cancelPolicy()
		watcher.Close()
		return nil, err
	}
	readiness, err := authorization.NewReadiness(sqlDB, reloader)
	if err != nil {
		cancelPolicy()
		watcher.Close()
		return nil, err
	}

	securityRuntime, err := loginprotection.NewLazyRuntime(c.RedisConf, func(ctx context.Context) (*system.LoginSecurityConfig, error) {
		return systemRPC.GetLoginSecurity(ctx, &system.GetLoginSecurityRequest{})
	})
	if err != nil {
		cancelPolicy()
		watcher.Close()
		reloader.Wait()
		return nil, err
	}
	closeDatabase = false
	jobClient := jobrpc.New(c.JobRpc)
	return &ServiceContext{
		ClientIP:               resolver,
		LoginRateLimit:         middleware.NewLoginRateLimitMiddleware(securityRuntime, redisClient, resolver, "login").Handle,
		securityRuntime:        securityRuntime,
		Config:                 c,
		SystemRpc:              systemRPC,
		JobRpc:                 jobClient,
		jobClient:              jobClient,
		Redis:                  redisClient,
		Sessions:               sessions,
		SessionAuth:            middleware.NewSessionAuthMiddleware(sessions).Handle,
		Authorization:          middleware.NewAuthorizationMiddleware(enforcer).Handle,
		AuthorizationEnforcer:  enforcer,
		AuthorizationReadiness: readiness,
		policyWatcher:          watcher,
		policyReloader:         reloader,
		cancelPolicy:           cancelPolicy,
		sqlDB:                  sqlDB,
	}, nil
}

func (s *ServiceContext) Close() error {
	if s.jobClient != nil {
		if err := s.jobClient.Close(); err != nil {
			logx.Errorf("close Job RPC client: %v", err)
		}
	}
	if s.securityRuntime != nil {
		s.securityRuntime.Close()
	}
	// Cancel the periodic loop and request watcher shutdown first.
	// redis-watcher's Close does not wait for callbacks to exit.
	if s.cancelPolicy != nil {
		s.cancelPolicy()
	}
	if s.policyWatcher != nil {
		s.policyWatcher.Close()
	}
	if s.policyReloader != nil {
		s.policyReloader.Wait()
	}
	// Wait covers the periodic loop and current reload, not watcher callbacks
	// that have yet to enter Reload. This ordering therefore does not prove that
	// every policy callback has exited.
	if s.sqlDB == nil {
		return nil
	}
	return s.sqlDB.Close()
}
