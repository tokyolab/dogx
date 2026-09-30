package loginprotection

import (
	"errors"

	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
)

type ConfigProvider interface {
	Current() (*system.LoginSecurityConfig, error)
}

func Validate(cfg *system.LoginSecurityConfig) error {
	if cfg == nil || cfg.RateLimitWindowSeconds < 1 || cfg.RateLimitWindowSeconds > 3600 ||
		cfg.RateLimitMaxRequests < 1 || cfg.RateLimitMaxRequests > 10000 ||
		cfg.FailureWindowSeconds < 60 || cfg.FailureWindowSeconds > 86400 ||
		cfg.FailureThreshold < 1 || cfg.FailureThreshold > 100 ||
		cfg.LockDurationSeconds < 60 || cfg.LockDurationSeconds > 86400 {
		return errors.New("invalid login protection configuration")
	}
	return nil
}
