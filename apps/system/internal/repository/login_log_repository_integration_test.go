//go:build integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/model"
)

func TestLoginLogRepositoryCreatesAuditRecord(t *testing.T) {
	userRepo, gormDB := newPostgreSQLUserRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	user := &model.User{
		Username:     "LoginAuditUser",
		PasswordHash: "hashed-password",
		Nickname:     "Login Audit User",
		Status:       model.RecordStatusEnabled,
	}
	if err := userRepo.Create(ctx, user); err != nil {
		t.Fatalf("create login audit user: %v", err)
	}

	repository := NewLoginLogRepository(gormDB)
	loginLog := &model.LoginLog{
		UserID:        &user.ID,
		Username:      user.Username,
		Success:       false,
		FailureReason: model.LoginFailureInvalidCredentials,
		IPAddress:     "192.0.2.1",
		UserAgent:     "DogX Integration Test",
	}
	if err := repository.Create(ctx, loginLog); err != nil {
		t.Fatalf("create login log: %v", err)
	}
	if loginLog.ID == 0 || loginLog.CreatedAt.IsZero() {
		t.Fatalf("login log identity or timestamp was not populated: %+v", loginLog)
	}

	var stored model.LoginLog
	if err := gormDB.WithContext(ctx).First(&stored, loginLog.ID).Error; err != nil {
		t.Fatalf("load login log: %v", err)
	}
	if stored.UserID == nil || *stored.UserID != user.ID || stored.Success ||
		stored.FailureReason != model.LoginFailureInvalidCredentials ||
		stored.IPAddress != "192.0.2.1" {
		t.Fatalf("unexpected stored login log: %+v", stored)
	}
}
