package svc

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/loginprotection"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
)

type SecurityRuntime interface {
	loginprotection.ConfigProvider
	Reload(context.Context) error
	Notify(context.Context) error
}

func LoadLoginSecurity(ctx context.Context, repo repository.SecurityConfigRepository) (*system.LoginSecurityConfig, error) {
	cfg, err := repo.Get(ctx)
	if err != nil {
		return nil, err
	}
	result := &system.LoginSecurityConfig{
		RateLimitEnabled: cfg.LoginRateLimitEnabled, RateLimitWindowSeconds: cfg.LoginRateLimitWindowSeconds,
		RateLimitMaxRequests: cfg.LoginRateLimitMaxRequests, FailureLockEnabled: cfg.LoginFailureLockEnabled,
		FailureWindowSeconds: cfg.LoginFailureWindowSeconds, FailureThreshold: cfg.LoginFailureThreshold,
		LockDurationSeconds: cfg.LoginLockDurationSeconds,
	}
	return result, loginprotection.Validate(result)
}
