//go:build integration

package repository

import (
	"context"
	"errors"
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

func TestRoleRepositoryListsPagesAndEscapesKeywordWildcards(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	roles := []model.Role{
		{Code: "paged_role_later", Name: "Paged Later", Sort: 20, Status: model.RecordStatusEnabled},
		{Code: "paged_role_first", Name: "Paged First", Sort: 10, Status: model.RecordStatusDisabled},
		{Code: "deleted_paged_role", Name: "Deleted Paged Role", Sort: 5, Status: model.RecordStatusEnabled},
		{Code: "literal_percent_role", Name: "Literal % Role", Sort: 30, Status: model.RecordStatusEnabled},
	}
	if err := db.Create(&roles).Error; err != nil {
		t.Fatalf("create role query fixtures: %v", err)
	}
	if err := db.Delete(&roles[2]).Error; err != nil {
		t.Fatalf("soft delete role query fixture: %v", err)
	}

	repository, err := NewRoleRepository(db)
	if err != nil {
		t.Fatalf("create role repository: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	firstPage, total, err := repository.List(ctx, RoleListQuery{
		Keyword: "paged_role",
		Offset:  0,
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("list first role page: %v", err)
	}
	if total != 2 || len(firstPage) != 1 || firstPage[0].ID != roles[1].ID {
		t.Fatalf("unexpected first role page: total=%d roles=%+v", total, firstPage)
	}
	secondPage, total, err := repository.List(ctx, RoleListQuery{
		Keyword: "paged_role",
		Offset:  1,
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("list second role page: %v", err)
	}
	if total != 2 || len(secondPage) != 1 || secondPage[0].ID != roles[0].ID {
		t.Fatalf("unexpected second role page: total=%d roles=%+v", total, secondPage)
	}

	literalMatch, total, err := repository.List(ctx, RoleListQuery{
		Keyword: "%",
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("list role by literal wildcard: %v", err)
	}
	if total != 1 || len(literalMatch) != 1 || literalMatch[0].ID != roles[3].ID {
		t.Fatalf("LIKE wildcard was not escaped: total=%d roles=%+v", total, literalMatch)
	}

	disabled, err := repository.FindByID(ctx, roles[1].ID)
	if err != nil || disabled.Status != model.RecordStatusDisabled {
		t.Fatalf("find disabled active role: role=%+v error=%v", disabled, err)
	}
	if _, err := repository.FindByID(ctx, roles[2].ID); !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("soft-deleted role error = %v, want %v", err, ErrRoleNotFound)
	}
}

func TestRoleRepositoryCreatesUpdatesAndProtectsSystemRole(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	repository, err := NewRoleRepository(db)
	if err != nil {
		t.Fatalf("create role repository: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	role := &model.Role{
		Code:        "content_editor",
		Name:        "Content Editor",
		Description: "Creates content",
		Sort:        20,
		Status:      model.RecordStatusEnabled,
	}
	if err := repository.Create(ctx, role); err != nil {
		t.Fatalf("create role: %v", err)
	}
	if role.ID <= 0 || role.IsSystem || role.CreatedAt.IsZero() || role.UpdatedAt.IsZero() {
		t.Fatalf("unexpected created role: %+v", role)
	}

	duplicate := &model.Role{
		Code:   "CONTENT_EDITOR",
		Name:   "Duplicate",
		Status: model.RecordStatusEnabled,
	}
	if err := repository.Create(ctx, duplicate); !errors.Is(err, ErrRoleCodeExists) {
		t.Fatalf("case-insensitive duplicate role code error = %v, want %v", err, ErrRoleCodeExists)
	}

	if err := repository.Update(ctx, role.ID, RoleUpdate{
		Code:        "content_reviewer",
		Name:        "Content Reviewer",
		Description: "Reviews content",
		Sort:        10,
	}); err != nil {
		t.Fatalf("update role: %v", err)
	}
	updated, err := repository.FindByID(ctx, role.ID)
	if err != nil {
		t.Fatalf("load updated role: %v", err)
	}
	if updated.Code != "content_reviewer" || updated.Name != "Content Reviewer" ||
		updated.Description != "Reviews content" || updated.Sort != 10 {
		t.Fatalf("unexpected updated role: %+v", updated)
	}

	var systemRole model.Role
	if err := db.WithContext(ctx).Where("code = ?", "super_admin").First(&systemRole).Error; err != nil {
		t.Fatalf("load system role: %v", err)
	}
	if !systemRole.IsSystem {
		t.Fatalf("seeded system role is not protected: %+v", systemRole)
	}
	if err := repository.Update(ctx, systemRole.ID, RoleUpdate{
		Code: "renamed_super_admin",
		Name: systemRole.Name,
		Sort: systemRole.Sort,
	}); !errors.Is(err, ErrSystemRoleProtected) {
		t.Fatalf("system role code update error = %v, want %v", err, ErrSystemRoleProtected)
	}
	if err := repository.Update(ctx, systemRole.ID, RoleUpdate{
		Code:        systemRole.Code,
		Name:        "平台超级管理员",
		Description: systemRole.Description,
		Sort:        systemRole.Sort,
	}); err != nil {
		t.Fatalf("update allowed system role metadata: %v", err)
	}
}
