package repository

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"gorm.io/gorm"
)

type SecurityConfigRepository interface {
	Get(context.Context) (*model.SecurityConfig, error)
	UpdateLogin(context.Context, *model.SecurityConfig) error
}

type securityConfigRepository struct{ db *gorm.DB }

func NewSecurityConfigRepository(db *gorm.DB) (SecurityConfigRepository, error) {
	if db == nil {
		return nil, errors.New("security config database is nil")
	}
	return &securityConfigRepository{db: db}, nil
}

func (r *securityConfigRepository) Get(ctx context.Context) (*model.SecurityConfig, error) {
	var cfg model.SecurityConfig
	err := r.db.WithContext(ctx).First(&cfg, 1).Error
	return &cfg, err
}

func (r *securityConfigRepository) UpdateLogin(ctx context.Context, cfg *model.SecurityConfig) error {
	if cfg == nil {
		return errors.New("security config is nil")
	}
	// A map preserves false switches and limits updates to this category only.
	result := r.db.WithContext(ctx).Model(&model.SecurityConfig{}).Where("id = ?", 1).Updates(map[string]any{
		"login_rate_limit_enabled":        cfg.LoginRateLimitEnabled,
		"login_rate_limit_window_seconds": cfg.LoginRateLimitWindowSeconds,
		"login_rate_limit_max_requests":   cfg.LoginRateLimitMaxRequests,
		"login_failure_lock_enabled":      cfg.LoginFailureLockEnabled,
		"login_failure_window_seconds":    cfg.LoginFailureWindowSeconds,
		"login_failure_threshold":         cfg.LoginFailureThreshold,
		"login_lock_duration_seconds":     cfg.LoginLockDurationSeconds,
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return gorm.ErrRecordNotFound
	}
	return nil
}
