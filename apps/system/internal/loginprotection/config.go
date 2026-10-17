package loginprotection

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
)

// ConfigProvider supplies validated security configuration to login checks.
// Load failures must not be hidden by defaulting protection to disabled.
type ConfigProvider interface {
	// Current returns a non-nil configuration snapshot on success, or an error
	// if unavailable, loading fails, or the request is canceled. Runtime returns
	// an independent copy, so caller mutations do not affect other requests.
	Current(context.Context) (*system.LoginSecurityConfig, error)
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
