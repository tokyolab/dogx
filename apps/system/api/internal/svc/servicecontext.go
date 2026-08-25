package svc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/api/internal/config"
	"github.com/tokyolab/dogx/apps/system/api/internal/middleware"
	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/authorization"
	systemdb "github.com/tokyolab/dogx/apps/system/internal/database"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

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
	Config                 config.Config
	SystemRpc              systemclient.System
	Redis                  RedisPinger
	Sessions               authn.SessionReader
	SessionAuth            rest.Middleware
	Authorization          rest.Middleware
	AuthorizationEnforcer  *casbin.SyncedEnforcer
	AuthorizationReadiness ReadinessChecker

	policyWatcher  authorization.PolicyWatcher
	policyReloader *authorization.PolicyReloader
	cancelPolicy   context.CancelFunc
	sqlDB          *sql.DB
}

func NewServiceContext(c config.Config) (*ServiceContext, error) {
	systemRPC := systemclient.NewSystem(zrpc.MustNewClient(c.SystemRpc))
	redisClient, err := redis.NewRedis(c.RedisConf)
	if err != nil {
		return nil, err
	}
	sessions, err := authn.NewRedisSessionReader(redisClient, c.Auth.SessionKeyPrefix)
	if err != nil {
		return nil, err
	}

	database, sqlDB, err := systemdb.OpenPostgres(c.Postgres)
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

	closeDatabase = false
	return &ServiceContext{
		Config:                 c,
		SystemRpc:              systemRPC,
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
	if s.cancelPolicy != nil {
		s.cancelPolicy()
	}
	if s.policyWatcher != nil {
		s.policyWatcher.Close()
	}
	if s.policyReloader != nil {
		s.policyReloader.Wait()
	}
	if s.sqlDB == nil {
		return nil
	}
	return s.sqlDB.Close()
}
