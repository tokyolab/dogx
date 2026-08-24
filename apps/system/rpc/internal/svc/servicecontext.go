package svc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
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
	Readiness     ReadinessChecker

	sqlDB *sql.DB
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

	sessionStore, err := authn.NewRedisSessionStore(
		redisClient,
		c.Authentication.SessionKeyPrefix,
		c.Authentication.UserSessionsKeyPrefix,
	)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize session store: %w", err)
	}
	tokenIssuer, err := authn.NewTokenIssuer(authn.TokenConfig{
		AccessSecret:  c.Authentication.AccessSecret,
		AccessExpire:  c.Authentication.AccessExpire,
		RefreshExpire: c.Authentication.RefreshExpire,
		Issuer:        c.Authentication.Issuer,
	}, sessionStore)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("initialize token issuer: %w", err)
	}

	passwords := authn.NewArgon2id()

	return &ServiceContext{
		Config:        c,
		DB:            database,
		Redis:         redisClient,
		UserRepo:      repository.NewUserRepository(database),
		LoginLogRepo:  repository.NewLoginLogRepository(database),
		Passwords:     passwords,
		Tokens:        tokenIssuer,
		RefreshTokens: tokenIssuer,
		Sessions:      sessionStore,
		Readiness:     health.NewReadiness(sqlDB, redisClient),
		sqlDB:         sqlDB,
	}, nil
}

func (s *ServiceContext) Close() error {
	if s.sqlDB == nil {
		return nil
	}

	return s.sqlDB.Close()
}
