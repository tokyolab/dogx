package svc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/authorization"
	systemdb "github.com/tokyolab/dogx/apps/system/internal/database"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/config"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/health"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/gorm"
)

type ReadinessChecker interface {
	Check(ctx context.Context) error
}

type RolePolicyWriter interface {
	ReplaceRoleAPIs(ctx context.Context, roleID int64, apiIDs []int64) (authorization.ReplaceResult, error)
}

type ServiceContext struct {
	Config        config.Config
	DB            *gorm.DB
	Redis         *redis.Redis
	UserRepo      repository.UserRepository
	LoginLogRepo  repository.LoginLogRepository
	Passwords     authn.PasswordHasher
	Tokens        authn.CredentialIssuer
	RefreshTokens authn.CredentialRefresher
	Sessions      authn.SessionStore
	RolePolicies  RolePolicyWriter
	Readiness     ReadinessChecker

	policyPublisher authorization.PolicyWatcher
	sqlDB           *sql.DB
}

func NewServiceContext(c config.Config) (*ServiceContext, error) {
	database, sqlDB, err := systemdb.OpenPostgres(c.Postgres)
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
	passwords := authn.NewArgon2id()
	roleRepo, err := repository.NewRoleRepository(database)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize role repository: %w", err)
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

	closePublisher = false
	return &ServiceContext{
		Config:          c,
		DB:              database,
		Redis:           redisClient,
		UserRepo:        repository.NewUserRepository(database),
		LoginLogRepo:    repository.NewLoginLogRepository(database),
		Passwords:       passwords,
		Tokens:          tokenIssuer,
		RefreshTokens:   tokenIssuer,
		Sessions:        sessionStore,
		RolePolicies:    rolePolicies,
		Readiness:       health.NewReadiness(sqlDB, redisClient),
		policyPublisher: policyPublisher,
		sqlDB:           sqlDB,
	}, nil
}

func (s *ServiceContext) Close() error {
	if s.policyPublisher != nil {
		s.policyPublisher.Close()
	}
	if s.sqlDB == nil {
		return nil
	}

	return s.sqlDB.Close()
}
