//go:build integration

package repository

import (
	"context"
	"errors"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"gorm.io/gorm"
	"testing"
)

func TestSecurityConfigRepository(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	ctx := context.Background()
	repo, err := NewSecurityConfigRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := repo.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ID != 1 || !cfg.LoginRateLimitEnabled || !cfg.LoginFailureLockEnabled || cfg.LoginRateLimitWindowSeconds != 60 || cfg.LoginRateLimitMaxRequests != 30 || cfg.LoginFailureWindowSeconds != 900 || cfg.LoginFailureThreshold != 5 || cfg.LoginLockDurationSeconds != 900 {
		t.Fatalf("bad defaults %+v", cfg)
	}
	before := cfg.CreatedAt
	cfg.ID = 999
	cfg.LoginRateLimitEnabled = false
	cfg.LoginFailureLockEnabled = false
	cfg.LoginFailureThreshold = 7
	if err := repo.UpdateLogin(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Get(ctx)
	if err != nil || got.ID != 1 || got.LoginRateLimitEnabled || got.LoginFailureLockEnabled || got.LoginFailureThreshold != 7 || !got.CreatedAt.Equal(before) {
		t.Fatalf("false mapping %+v %v", got, err)
	}
	if err := repo.UpdateLogin(ctx, cfg); err != nil {
		t.Fatal("unchanged save", err)
	}
	if err := db.Exec("INSERT INTO sys_security_config(id) VALUES (2)").Error; err == nil {
		t.Fatal("second config row allowed")
	}
	// Business ranges belong to API/RPC validation; direct SQL is not range-limited.
	if err := db.Exec(`UPDATE sys_security_config SET
		login_rate_limit_window_seconds=0, login_rate_limit_max_requests=0,
		login_failure_window_seconds=0, login_failure_threshold=0,
		login_lock_duration_seconds=0 WHERE id=1`).Error; err != nil {
		t.Fatalf("business range checks remain in the database: %v", err)
	}
	if err := db.Exec("UPDATE sys_security_config SET login_failure_threshold=NULL WHERE id=1").Error; err == nil {
		t.Fatal("required configuration allowed NULL")
	}
	if err := repo.UpdateLogin(ctx, nil); err == nil {
		t.Fatal("nil update accepted")
	}
	if err := db.Delete(&model.SecurityConfig{}, 1).Error; err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateLogin(ctx, cfg); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal(err)
	}
	if _, err := repo.Get(ctx); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal(err)
	}
	if _, err := NewSecurityConfigRepository(nil); err == nil {
		t.Fatal("nil database accepted")
	}
}
