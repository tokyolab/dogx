//go:build integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/model"
)

func TestRoleRepositoryReturnsOnlyEnabledActiveUserRoles(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	user := model.User{Username: "role-user", PasswordHash: "hash", Nickname: "Role User", Status: model.RecordStatusEnabled}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	roles := []model.Role{
		{Code: "enabled", Name: "Enabled", Status: model.RecordStatusEnabled},
		{Code: "disabled", Name: "Disabled", Status: model.RecordStatusDisabled},
		{Code: "deleted", Name: "Deleted", Status: model.RecordStatusEnabled},
	}
	if err := db.Create(&roles).Error; err != nil {
		t.Fatalf("create roles: %v", err)
	}
	if err := db.Delete(&roles[2]).Error; err != nil {
		t.Fatalf("soft delete role: %v", err)
	}
	for _, role := range roles {
		if err := db.Create(&model.UserRole{UserID: user.ID, RoleID: role.ID}).Error; err != nil {
			t.Fatalf("assign role %d: %v", role.ID, err)
		}
	}

	repository, err := NewRoleRepository(db)
	if err != nil {
		t.Fatalf("create role repository: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	roleIDs, err := repository.ListEnabledRoleIDs(ctx, user.ID)
	if err != nil {
		t.Fatalf("list enabled user roles: %v", err)
	}
	if len(roleIDs) != 1 || roleIDs[0] != roles[0].ID {
		t.Fatalf("unexpected enabled role ids: %v", roleIDs)
	}
}
