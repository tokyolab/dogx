//go:build integration

package repository

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/migration"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/testutil"

	"gorm.io/gorm"
)

func TestUserRepositoryCreatesAndFindsUserInPostgreSQL(t *testing.T) {
	repository, _ := newPostgreSQLUserRepository(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	email := "dogx-user@example.com"
	phone := "13900000000"
	user := model.User{
		Username:     "DogXUser",
		PasswordHash: "hashed-password",
		Nickname:     "DogX User",
		Email:        &email,
		Phone:        &phone,
		Status:       model.RecordStatusEnabled,
		Remark:       "repository integration test",
	}
	if err := repository.Create(ctx, &user); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if user.ID == 0 {
		t.Fatal("created user ID was not populated")
	}
	if user.CreatedAt.IsZero() || user.UpdatedAt.IsZero() {
		t.Fatalf("GORM timestamps were not populated: created_at=%v updated_at=%v", user.CreatedAt, user.UpdatedAt)
	}

	byID, err := repository.FindByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("find user by ID: %v", err)
	}
	if byID.Username != user.Username || byID.Nickname != user.Nickname {
		t.Fatalf("unexpected user loaded by ID: %+v", byID)
	}

	byUsername, err := repository.FindByUsername(ctx, strings.ToUpper(user.Username))
	if err != nil {
		t.Fatalf("find user by case-insensitive username: %v", err)
	}
	if byUsername.ID != user.ID {
		t.Fatalf("unexpected user loaded by username: got ID %d, want %d", byUsername.ID, user.ID)
	}

	lastLoginAt := time.Date(2026, time.August, 24, 10, 30, 0, 0, time.UTC)
	if err := repository.UpdateLastLoginAt(ctx, user.ID, lastLoginAt); err != nil {
		t.Fatalf("update last login time: %v", err)
	}
	if err := repository.UpdatePasswordHash(ctx, user.ID, "updated-password-hash"); err != nil {
		t.Fatalf("update password hash: %v", err)
	}
	updated, err := repository.FindByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("load updated user: %v", err)
	}
	if updated.LastLoginAt == nil || !updated.LastLoginAt.Equal(lastLoginAt) ||
		updated.PasswordHash != "updated-password-hash" {
		t.Fatalf("unexpected updated user: %+v", updated)
	}
}

func TestUserRepositoryHonorsActiveUsernameUniquenessAndSoftDelete(t *testing.T) {
	repository, gormDB := newPostgreSQLUserRepository(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	user := model.User{
		Username:     "SoftDeleteUser",
		PasswordHash: "hashed-password",
		Nickname:     "Soft Delete User",
		Status:       model.RecordStatusEnabled,
	}
	if err := repository.Create(ctx, &user); err != nil {
		t.Fatalf("create original user: %v", err)
	}

	duplicate := model.User{
		Username:     strings.ToLower(user.Username),
		PasswordHash: "another-hash",
		Nickname:     "Duplicate User",
		Status:       model.RecordStatusEnabled,
	}
	if err := repository.Create(ctx, &duplicate); err == nil {
		t.Fatal("case-insensitive active username uniqueness was not enforced")
	}

	if err := gormDB.WithContext(ctx).Delete(&user).Error; err != nil {
		t.Fatalf("soft delete user: %v", err)
	}
	if !user.DeletedAt.Valid {
		t.Fatal("GORM did not populate deleted_at during soft delete")
	}

	if _, err := repository.FindByID(ctx, user.ID); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("soft-deleted user should be hidden by ID, got %v", err)
	}
	if _, err := repository.FindByUsername(ctx, user.Username); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("soft-deleted user should be hidden by username, got %v", err)
	}

	replacement := model.User{
		Username:     strings.ToUpper(user.Username),
		PasswordHash: "replacement-hash",
		Nickname:     "Replacement User",
		Status:       model.RecordStatusEnabled,
	}
	if err := repository.Create(ctx, &replacement); err != nil {
		t.Fatalf("reuse username after soft delete: %v", err)
	}
	if replacement.ID == user.ID {
		t.Fatalf("replacement user reused deleted identity %d", replacement.ID)
	}
}

func newPostgreSQLUserRepository(t testing.TB) (UserRepository, *gorm.DB) {
	t.Helper()

	gormDB, sqlDB := testutil.OpenPostgres(t)
	provider, err := migration.NewProvider(sqlDB)
	if err != nil {
		t.Fatalf("create migration provider: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	repository, err := NewUserRepository(gormDB)
	if err != nil {
		t.Fatalf("create user repository: %v", err)
	}
	return repository, gormDB
}
