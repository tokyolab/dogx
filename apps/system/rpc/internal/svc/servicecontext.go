package svc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/authorization"
	"github.com/tokyolab/dogx/apps/system/internal/dictcache"
	"github.com/tokyolab/dogx/apps/system/internal/loginprotection"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/config"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/health"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/postgres"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"google.golang.org/grpc"
	"gorm.io/gorm"
)

type ReadinessChecker interface {
	Check(ctx context.Context) error
}

type RolePolicyService interface {
	ReplaceRoleAPIs(ctx context.Context, roleID int64, apiIDs []int64) (authorization.ReplaceResult, error)
	ListRoleAPIIDs(ctx context.Context, roleID int64) ([]int64, error)
	DeleteRole(
		ctx context.Context,
		roleID int64,
	) (authorization.DeleteRoleResult, error)
}

// RegisterProcessors registers System-owned processors after domain dependencies
// are initialized and before execution starts. It is called once, synchronously.
// Nil leaves the registry empty; unknown processor names are refused at dispatch.
// It must not start background work or retain the mutable registry. A failure
// aborts startup and closes the dependencies initialized by ServiceContext.
type RegisterProcessors func(*ServiceContext, *jobexecutor.Registry) error

// ServiceContext owns the RPC process's database, security runtime, policy
// publisher and embedded executor. The host stops admission, drains both inbound
// servers and accepted business work, then calls Close using one shared budget.
// Dependencies must not be mutated while serving.
type ServiceContext struct {
	SecurityRepo       repository.SecurityConfigRepository
	Security           SecurityRuntime
	LoginFailures      loginprotection.FailureStore
	DictionaryRepo     repository.DictionaryRepository
	DictionaryItemRepo repository.DictionaryItemRepository
	DictionaryCache    dictcache.Store
	Config             config.Config
	DB                 *gorm.DB
	Redis              *redis.Redis
	UserRepo           repository.UserRepository
	DepartmentRepo     repository.DepartmentRepository
	RoleRepo           repository.RoleRepository
	MenuRepo           repository.MenuRepository
	RoleMenuRepo       repository.RoleMenuRepository
	APIRepo            repository.APIRepository
	LoginLogRepo       repository.LoginLogRepository
	Passwords          authn.PasswordHasher
	Tokens             authn.CredentialIssuer
	RefreshTokens      authn.CredentialRefresher
	Sessions           authn.SessionStore
	RolePolicies       RolePolicyService
	Readiness          ReadinessChecker
	Executor           *jobexecutor.Runtime
	ExecutorAdapter    *jobexecutor.RPCAdapter

	policyPublisher authorization.PolicyWatcher
	securityRuntime *loginprotection.Runtime
	sqlDB           *sql.DB
	reportConn      *grpc.ClientConn
	closeOnce       sync.Once
	closeErr        error
}

// NewServiceContext initializes domain dependencies and the embedded executor.
// The registration callback may inject these dependencies into System processors;
// it cannot access Job persistence. The returned executor is ready before either
// server starts. Partial initialization failure releases owned resources. Native
// dependency initialization can still block; the host bounds the overall wait.
func NewServiceContext(c config.Config, register RegisterProcessors) (*ServiceContext, error) {
	if err := c.PrepareExecutor(); err != nil {
		return nil, err
	}
	database, sqlDB, err := postgres.Open(c.Postgres)
	if err != nil {
		return nil, err
	}

	redisClient, err := redis.NewRedis(c.RedisConf)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize redis: %w", err)
	}
	policyPublisher, err := authorization.NewRedisPolicyPublisher(
		c.RedisConf,
		c.Authorization.PolicyChannel,
	)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize authorization policy publisher: %w", err)
	}
	closePublisher := true
	defer func() {
		if closePublisher {
			policyPublisher.Close()
		}
	}()
	rolePolicies, err := authorization.NewRolePolicyService(database, policyPublisher)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize role policy service: %w", err)
	}

	sessionStore, err := authn.NewRedisSessionStore(
		redisClient,
		c.Authentication.SessionKeyPrefix,
		c.Authentication.UserSessionsKeyPrefix,
	)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize session store: %w", err)
	}
	passwords := authn.NewBcrypt()
	userRepo, err := repository.NewUserRepository(database)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize user repository: %w", err)
	}
	departmentRepo, err := repository.NewDepartmentRepository(database)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize department repository: %w", err)
	}
	roleRepo, err := repository.NewRoleRepository(database)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize role repository: %w", err)
	}
	apiRepo, err := repository.NewAPIRepository(database)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize API repository: %w", err)
	}
	menuRepo, err := repository.NewMenuRepository(database)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize menu repository: %w", err)
	}
	roleMenuRepo, err := repository.NewRoleMenuRepository(database)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize role menu repository: %w", err)
	}
	loginLogRepo, err := repository.NewLoginLogRepository(database)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize login log repository: %w", err)
	}
	tokenIssuer, err := authn.NewTokenIssuer(authn.TokenConfig{
		AccessSecret:  c.Authentication.AccessSecret,
		AccessExpire:  c.Authentication.AccessExpire,
		RefreshExpire: c.Authentication.RefreshExpire,
		Issuer:        c.Authentication.Issuer,
	}, sessionStore, roleRepo)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize token issuer: %w", err)
	}

	dictionaryRepo, err := repository.NewDictionaryRepository(database)
	if err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	dictionaryItemRepo, err := repository.NewDictionaryItemRepository(database)
	if err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	securityRepo, err := repository.NewSecurityConfigRepository(database)
	if err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	securityRuntime, err := loginprotection.NewRuntime(c.RedisConf, func(ctx context.Context) (*system.LoginSecurityConfig, error) {
		return LoadLoginSecurity(ctx, securityRepo)
	})
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize login protection: %w", err)
	}
	closePublisher = false
	s := &ServiceContext{
		SecurityRepo:       securityRepo,
		Security:           securityRuntime,
		LoginFailures:      loginprotection.NewFailureStore(redisClient),
		securityRuntime:    securityRuntime,
		DictionaryRepo:     dictionaryRepo,
		DictionaryItemRepo: dictionaryItemRepo,
		DictionaryCache:    dictcache.New(redisClient),
		Config:             c,
		DB:                 database,
		Redis:              redisClient,
		UserRepo:           userRepo,
		DepartmentRepo:     departmentRepo,
		RoleRepo:           roleRepo,
		MenuRepo:           menuRepo,
		RoleMenuRepo:       roleMenuRepo,
		APIRepo:            apiRepo,
		LoginLogRepo:       loginLogRepo,
		Passwords:          passwords,
		Tokens:             tokenIssuer,
		RefreshTokens:      tokenIssuer,
		Sessions:           sessionStore,
		RolePolicies:       rolePolicies,
		Readiness:          health.NewReadiness(sqlDB, redisClient),
		policyPublisher:    policyPublisher,
		sqlDB:              sqlDB,
	}
	if err := s.initializeExecutor(register); err != nil {
		return nil, fmt.Errorf("initialize job executor: %w", err)
	}
	return s, nil
}

func (s *ServiceContext) initializeExecutor(register RegisterProcessors) (err error) {
	ready := false
	defer func() {
		if !ready {
			// The host does not call Close after Initialize fails, including a
			// registration panic. Release dependencies here and let panic propagate.
			err = errors.Join(err, s.Close())
		}
	}()
	registry := jobexecutor.NewRegistry()
	if register != nil {
		if err := register(s, registry); err != nil {
			return err
		}
	}
	identity, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("create executor process identity: %w", err)
	}
	c := s.Config.JobExecutor
	client, err := jobexecutor.NewReportClient(c.ReportClient)
	if err != nil {
		return err
	}
	s.reportConn = client.Conn()
	s.Executor, err = jobexecutor.NewRuntime(registry, protocol.NewResultReceiverClient(s.reportConn),
		c.GroupName, c.Token, identity.String(), c.Runtime)
	if err != nil {
		return err
	}
	auth, err := jobexecutor.NewAuthenticator(c.GroupName, c.Token)
	if err != nil {
		return err
	}
	s.ExecutorAdapter, err = jobexecutor.NewRPCAdapter(auth, registry, identity.String(), s.Executor)
	if err != nil {
		return err
	}
	if err := s.Executor.Start(); err != nil {
		return err
	}
	ready = true
	return nil
}

// Stop closes new executor admission without waiting or closing dependencies.
// Business and reporting already accepted by the executor continue during Drain.
// The host separately stops inbound business RPCs. Repeated calls are safe.
func (s *ServiceContext) Stop() {
	if s.Executor != nil {
		s.Executor.Stop()
	}
}

// Drain immediately applies the host's shared budget to accepted executor work.
// Private security refresh is joined later by Close, so a blocked refresh cannot
// delay cancellation of business or reporting when the shared deadline expires.
func (s *ServiceContext) Drain(ctx context.Context) error {
	if s.Executor != nil {
		return s.Executor.Drain(ctx)
	}
	return nil
}

// Wait observes actual executor business and reporting completion. It neither
// stops admission nor joins inbound RPCs or the private security refresh loop.
// Cancellation bounds only this wait, not the lifetime of borrowed dependencies.
func (s *ServiceContext) Wait(ctx context.Context) error {
	if s.Executor != nil {
		return s.Executor.Wait(ctx)
	}
	return nil
}

// Close stops admission and waits for actual executor completion before closing
// reporting, security refresh, policy publishing and the database. Inbound RPC
// handlers must already have finished. Concurrent calls share one cleanup and
// its result. Close has no independent deadline; the host must not call it after
// failed draining, and exits if cleanup exceeds the remaining shared budget.
func (s *ServiceContext) Close() error {
	s.closeOnce.Do(func() {
		s.Stop()
		_ = s.Wait(context.Background())
		if s.reportConn != nil {
			s.closeErr = s.reportConn.Close()
		}
		if s.securityRuntime != nil {
			s.securityRuntime.Close()
		}
		if s.policyPublisher != nil {
			s.policyPublisher.Close()
		}
		if s.sqlDB != nil {
			s.closeErr = errors.Join(s.closeErr, s.sqlDB.Close())
		}
	})
	return s.closeErr
}
