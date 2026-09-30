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

	repository, err := NewLoginLogRepository(gormDB)
	if err != nil {
		t.Fatalf("create login log repository: %v", err)
	}
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

func TestLoginLogRepositoryListsAuditRecords(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	r, err := NewLoginLogRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	entries := []model.LoginLog{
		{Username: "Alice", Success: true},
		{Username: "ALICE", Success: false, FailureReason: model.LoginFailureInvalidCredentials},
		{Username: "bob", Success: false, FailureReason: model.LoginFailureAccountDisabled},
		// Failed attempts can contain arbitrary submitted names, unlike valid users.
		{Username: "percent%_!", Success: false, FailureReason: model.LoginFailureInvalidCredentials},
	}
	for i := range entries {
		if err := r.Create(ctx, &entries[i]); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name  string
		query LoginLogListQuery
		total int64
		ids   []int64
	}{
		{"all", LoginLogListQuery{Limit: 20}, 4, []int64{entries[3].ID, entries[2].ID, entries[1].ID, entries[0].ID}},
		{"page", LoginLogListQuery{Limit: 2, Offset: 1}, 4, []int64{entries[2].ID, entries[1].ID}},
		{"case insensitive", LoginLogListQuery{Username: " aLiCe ", Limit: 20}, 2, []int64{entries[1].ID, entries[0].ID}},
		{"prefix does not match", LoginLogListQuery{Username: "ali", Limit: 20}, 0, nil},
		{"substring does not match", LoginLogListQuery{Username: "lic", Limit: 20}, 0, nil},
		{"suffix does not match", LoginLogListQuery{Username: "ice", Limit: 20}, 0, nil},
		{"success", LoginLogListQuery{Success: loginLogBool(true), Limit: 20}, 1, []int64{entries[0].ID}},
		{"failure", LoginLogListQuery{Success: loginLogBool(false), Limit: 20}, 3, []int64{entries[3].ID, entries[2].ID, entries[1].ID}},
		{"combined", LoginLogListQuery{Username: "alice", Success: loginLogBool(false), Limit: 20}, 1, []int64{entries[1].ID}},
		{"literal wildcard", LoginLogListQuery{Username: "percent%_!", Limit: 20}, 1, []int64{entries[3].ID}},
		{"wildcard is not a pattern", LoginLogListQuery{Username: "%", Limit: 20}, 0, nil},
		{"empty", LoginLogListQuery{Username: "missing", Limit: 20}, 0, nil},
		{"beyond last page", LoginLogListQuery{Offset: 4, Limit: 20}, 4, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs, total, err := r.List(ctx, tc.query)
			if err != nil {
				t.Fatal(err)
			}
			if logs == nil || total != tc.total || len(logs) != len(tc.ids) {
				t.Fatalf("result: %+v total %d", logs, total)
			}
			for i, id := range tc.ids {
				if logs[i].ID != id {
					t.Fatalf("unexpected order: %+v", logs)
				}
			}
		})
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := r.List(cancelled, LoginLogListQuery{Limit: 20}); err == nil {
		t.Fatal("cancelled query succeeded")
	}
}

func loginLogBool(value bool) *bool { return &value }
