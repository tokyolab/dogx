//go:build integration

package repository

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/migration"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/testutil"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
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

type userQueryCounter struct {
	logger.Interface
	queries atomic.Int32
}

func (c *userQueryCounter) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	c.queries.Add(1)
	c.Interface.Trace(ctx, begin, fc, err)
}

func TestUserManagementPersistsRolesAndClearedProfileWithoutNPlusOne(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	ctx := context.Background()
	counter := &userQueryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewUserRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	role := model.Role{Code: "user_reader", Name: "Reader", Status: 1}
	if err := db.Create(&role).Error; err != nil {
		t.Fatal(err)
	}
	email, phone := "alice@example.com", "123456"
	alice := model.User{Username: "Alice", Nickname: "Alice", PasswordHash: "hash", Status: 1, Email: &email, Phone: &phone, Remark: "note"}
	if err := repo.CreateWithRoles(ctx, &alice, []int64{role.ID, role.ID}); err != nil {
		t.Fatal(err)
	}
	bob := model.User{Username: "Bob", Nickname: "Bob", PasswordHash: "hash", Status: 0}
	if err := repo.CreateWithRoles(ctx, &bob, []int64{role.ID}); err != nil {
		t.Fatal(err)
	}
	counter.queries.Store(0)
	rows, total, err := repo.List(ctx, UserListQuery{Limit: 20})
	if err != nil || total != 2 || len(rows) != 2 || len(rows[0].Roles) != 1 || len(rows[1].Roles) != 1 || rows[0].User.ID != bob.ID || rows[1].User.PasswordHash != "" {
		t.Fatalf("list: %+v total=%d err=%v", rows, total, err)
	}
	if counter.queries.Load() != 3 {
		t.Fatalf("user list issued %d queries; expected count + page + batch roles", counter.queries.Load())
	}
	zero := model.RecordStatusDisabled
	filtered, count, err := repo.List(ctx, UserListQuery{Limit: 1, Status: &zero, Keyword: "bOB"})
	if err != nil || count != 1 || len(filtered) != 1 || filtered[0].User.ID != bob.ID {
		t.Fatalf("filter: %+v %d %v", filtered, count, err)
	}
	if err := repo.UpdateProfile(ctx, alice.ID, UserProfileUpdate{Nickname: "新昵称"}); err != nil {
		t.Fatal(err)
	}
	updated, err := repo.FindByID(ctx, alice.ID)
	if err != nil || updated.Nickname != "新昵称" || updated.Email != nil || updated.Phone != nil || updated.Remark != "" || updated.Username != "Alice" || updated.PasswordHash != "hash" {
		t.Fatalf("profile clearing: %+v %v", updated, err)
	}
	duplicate := model.User{Username: "alice", Nickname: "Duplicate", PasswordHash: "hash", Status: 1}
	if err := repo.CreateWithRoles(ctx, &duplicate, nil); !errors.Is(err, ErrUsernameExists) {
		t.Fatalf("duplicate: %v", err)
	}
	if err := repo.UpdateProfile(ctx, 999999, UserProfileUpdate{Nickname: "missing"}); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func TestUserListPagesExcludeSoftDeletedUsers(t *testing.T) {
	repo, db := newPostgreSQLUserRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	users := []model.User{
		{Username: "pagefirst", Nickname: "First", PasswordHash: "hash", Status: 1},
		{Username: "pagesecond", Nickname: "Second", PasswordHash: "hash", Status: 0},
		{Username: "pagethird", Nickname: "Third", PasswordHash: "hash", Status: 1},
		{Username: "pagedeleted", Nickname: "Deleted", PasswordHash: "hash", Status: 1},
	}
	if err := db.WithContext(ctx).Create(&users).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.WithContext(ctx).Delete(&users[3]).Error; err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		query UserListQuery
		ids   []int64
		total int64
	}{
		{"first page", UserListQuery{Limit: 2}, []int64{users[2].ID, users[1].ID}, 3},
		{"second page", UserListQuery{Limit: 2, Offset: 2}, []int64{users[0].ID}, 3},
		{"past last page", UserListQuery{Limit: 2, Offset: 4}, nil, 3},
		{"only deleted user matches", UserListQuery{Limit: 2, Keyword: "pagedeleted"}, nil, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows, total, err := repo.List(ctx, test.query)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]int64, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.User.ID)
			}
			if total != test.total || !slices.Equal(ids, test.ids) {
				t.Fatalf("unexpected page: ids=%v total=%d, want ids=%v total=%d", ids, total, test.ids, test.total)
			}
		})
	}
}

func TestUserListSearchTreatsWildcardsLiterally(t *testing.T) {
	repo, db := newPostgreSQLUserRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	users := []model.User{
		{Username: "percent", Nickname: "Percent%", PasswordHash: "hash", Status: 1},
		{Username: "percentdecoy", Nickname: "PercentX", PasswordHash: "hash", Status: 1},
		{Username: "underscore", Nickname: "Under_score", PasswordHash: "hash", Status: 1},
		{Username: "underscoredecoy", Nickname: "UnderXscore", PasswordHash: "hash", Status: 1},
		{Username: "bang", Nickname: "Bang!mark", PasswordHash: "hash", Status: 1},
		{Username: "bangdecoy", Nickname: "Bangmark", PasswordHash: "hash", Status: 1},
		{Username: "MixedUser", Nickname: "Username Match", PasswordHash: "hash", Status: 1},
		{Username: "nickname", Nickname: "Mixed Nickname", PasswordHash: "hash", Status: 1},
	}
	if err := db.WithContext(ctx).Create(&users).Error; err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		keyword string
		ids     []int64
	}{
		{"percent is not a wildcard", "percent%", []int64{users[0].ID}},
		{"underscore is not a wildcard", "under_score", []int64{users[2].ID}},
		{"escape character stays literal", "bang!mark", []int64{users[4].ID}},
		{"case insensitive username and nickname", "mIxEd", []int64{users[7].ID, users[6].ID}},
		{"no matching users", "nobody-matches", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows, total, err := repo.List(ctx, UserListQuery{Limit: 20, Keyword: test.keyword})
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]int64, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.User.ID)
			}
			if total != int64(len(test.ids)) || !slices.Equal(ids, test.ids) {
				t.Fatalf("unexpected search result: ids=%v total=%d, want ids=%v", ids, total, test.ids)
			}
		})
	}
}

func TestUserRoleAssignmentRejectsSuperAdminGrantAndProtectsItsRoles(t *testing.T) {
	repo, db := newPostgreSQLUserRepository(t)
	ctx := context.Background()
	var super model.Role
	if err := db.Where("code = ?", model.SuperAdminRoleCode).First(&super).Error; err != nil {
		t.Fatal(err)
	}
	role := model.Role{Code: "assignable", Name: "Assignable", Status: 1}
	if err := db.Create(&role).Error; err != nil {
		t.Fatal(err)
	}
	user := model.User{Username: "protected", Nickname: "Protected", PasswordHash: "hash", Status: 1}
	if err := repo.CreateWithRoles(ctx, &user, []int64{super.ID}); !errors.Is(err, ErrSuperAdminNotAssignable) {
		t.Fatalf("super grant: %v", err)
	}
	if _, err := repo.FindByUsername(ctx, user.Username); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("rejected create left a user: %v", err)
	}
	if err := repo.CreateWithRoles(ctx, &user, []int64{role.ID}); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&role).Update("status", 0).Error; err != nil {
		t.Fatal(err)
	}
	if err := repo.ReplaceRoles(ctx, user.ID, []int64{role.ID}); err != nil {
		t.Fatalf("existing disabled role should be retained: %v", err)
	}
	if err := repo.ReplaceRoles(ctx, user.ID, []int64{super.ID}); !errors.Is(err, ErrSuperAdminNotAssignable) {
		t.Fatalf("explicit super grant: %v", err)
	}
	if err := repo.ReplaceRoles(ctx, user.ID, nil); err != nil {
		t.Fatal(err)
	}
	record, err := repo.FindWithRoles(ctx, user.ID)
	if err != nil || len(record.Roles) != 0 {
		t.Fatalf("ordinary user roles were not cleared: %+v %v", record, err)
	}
	if err := repo.ReplaceRoles(ctx, user.ID, []int64{role.ID}); !errors.Is(err, ErrUserRoleUnavailable) {
		t.Fatalf("new disabled grant accepted: %v", err)
	}
	if err := repo.ReplaceRoles(ctx, user.ID, []int64{999999}); !errors.Is(err, ErrUserRoleUnavailable) {
		t.Fatalf("missing role accepted: %v", err)
	}
	if err := db.Create(&model.UserRole{UserID: user.ID, RoleID: super.ID}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&role).Update("status", 1).Error; err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		ids  []int64
	}{
		{"ordinary role", []int64{role.ID}},
		{"explicit super role", []int64{super.ID}},
		{"empty roles", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := repo.ReplaceRoles(ctx, user.ID, test.ids); !errors.Is(err, ErrSuperAdminProtected) {
				t.Fatalf("super administrator role change accepted: %v", err)
			}
			record, err := repo.FindWithRoles(ctx, user.ID)
			if err != nil || len(record.Roles) != 1 || record.Roles[0].ID != super.ID {
				t.Fatalf("protected roles changed: %+v %v", record, err)
			}
		})
	}
	if err := db.Model(&role).Update("status", 0).Error; err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateStatus(ctx, user.ID, 0); !errors.Is(err, ErrSuperAdminProtected) {
		t.Fatalf("initialized super administrator disabled: %v", err)
	}
	if err := repo.Delete(ctx, user.ID); !errors.Is(err, ErrSuperAdminProtected) {
		t.Fatalf("initialized super administrator deleted: %v", err)
	}
	roleRepo, err := NewRoleRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	options, total, err := roleRepo.List(ctx, RoleListQuery{Limit: 200, AssignableOnly: true})
	if err != nil || total != 0 || len(options) != 0 {
		t.Fatalf("super or disabled role leaked into options: %+v %v", options, err)
	}
}

func TestUserRepositoryUpdatesStatusWithoutChangingProfileOrRoles(t *testing.T) {
	repo, db := newPostgreSQLUserRepository(t)
	ctx := context.Background()
	role := model.Role{Code: "user_status_reader", Name: "Reader", Status: 1}
	if err := db.Create(&role).Error; err != nil {
		t.Fatal(err)
	}
	user := model.User{Username: "status-user", Nickname: "Status User", PasswordHash: "hash", Status: 1, Remark: "keep"}
	if err := repo.CreateWithRoles(ctx, &user, []int64{role.ID}); err != nil {
		t.Fatal(err)
	}
	for _, state := range []model.RecordStatus{0, 0, 1} {
		if err := repo.UpdateStatus(ctx, user.ID, state); err != nil {
			t.Fatalf("update status to %d: %v", state, err)
		}
		updated, err := repo.FindByID(ctx, user.ID)
		if err != nil {
			t.Fatal(err)
		}
		if updated.Status != state || updated.Username != user.Username ||
			updated.Nickname != user.Nickname || updated.PasswordHash != user.PasswordHash || updated.Remark != user.Remark {
			t.Fatalf("status update changed unrelated fields: %+v", updated)
		}
		record, err := repo.FindWithRoles(ctx, user.ID)
		if err != nil || len(record.Roles) != 1 || record.Roles[0].ID != role.ID {
			t.Fatalf("status update changed role assignments: %+v %v", record, err)
		}
	}
	if err := repo.UpdateStatus(ctx, user.ID+1000, 0); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("missing user status update: %v", err)
	}
	if err := repo.Delete(ctx, user.ID); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateStatus(ctx, user.ID, 1); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("soft-deleted user status update: %v", err)
	}
}

func TestUserRepositoryStatusUpdatePreservesReadAndWriteErrors(t *testing.T) {
	repo, db := newPostgreSQLUserRepository(t)
	ctx := context.Background()
	user := model.User{Username: "status-failure", Nickname: "Status Failure", PasswordHash: "hash", Status: 1}
	if err := repo.Create(ctx, &user); err != nil {
		t.Fatal(err)
	}
	forced := errors.New("forced status persistence failure")
	fail := func(tx *gorm.DB) {
		if tx.Statement.Table == "sys_user" {
			_ = tx.AddError(forced)
		}
	}
	if err := db.Callback().Query().Before("gorm:query").Register("test:user-status-read-failure", fail); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Query().Remove("test:user-status-read-failure") })
	if err := repo.UpdateStatus(ctx, user.ID, 0); !errors.Is(err, forced) {
		t.Fatalf("read error lost: %v", err)
	}
	if err := db.Callback().Query().Remove("test:user-status-read-failure"); err != nil {
		t.Fatal(err)
	}
	if err := db.Callback().Update().Before("gorm:update").Register("test:user-status-write-failure", fail); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Update().Remove("test:user-status-write-failure") })
	if err := repo.UpdateStatus(ctx, user.ID, 0); !errors.Is(err, forced) {
		t.Fatalf("write error lost: %v", err)
	}
	unchanged, err := repo.FindByID(ctx, user.ID)
	if err != nil || unchanged.Status != model.RecordStatusEnabled {
		t.Fatalf("failed status update changed user: %+v %v", unchanged, err)
	}
}

func TestUserDeletionCleansAssociationsAndRollsBackOnFailure(t *testing.T) {
	repo, db := newPostgreSQLUserRepository(t)
	ctx := context.Background()
	role := model.Role{Code: "delete_user_reader", Name: "Reader", Status: 1}
	if err := db.Create(&role).Error; err != nil {
		t.Fatal(err)
	}
	user := model.User{Username: "delete-user", Nickname: "Delete", PasswordHash: "hash", Status: 1}
	if err := repo.CreateWithRoles(ctx, &user, []int64{role.ID}); err != nil {
		t.Fatal(err)
	}
	forced := errors.New("forced user deletion failure")
	if err := db.Callback().Delete().Before("gorm:delete").Register("test:user-delete-failure", func(tx *gorm.DB) {
		if tx.Statement.Table == "sys_user" {
			_ = tx.AddError(forced)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Delete().Remove("test:user-delete-failure") })
	if err := repo.Delete(ctx, user.ID); !errors.Is(err, forced) {
		t.Fatalf("expected rollback: %v", err)
	}
	record, err := repo.FindWithRoles(ctx, user.ID)
	if err != nil || len(record.Roles) != 1 {
		t.Fatalf("association delete escaped rollback: %+v %v", record, err)
	}
	if err := db.Callback().Delete().Remove("test:user-delete-failure"); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, user.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FindWithRoles(ctx, user.ID); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("deleted user visible: %v", err)
	}
	var count int64
	if err := db.Model(&model.UserRole{}).Where("user_id = ?", user.ID).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("remaining associations: %d %v", count, err)
	}
	var deleted model.User
	if err := db.Unscoped().First(&deleted, user.ID).Error; err != nil || !deleted.DeletedAt.Valid {
		t.Fatalf("user was not soft-deleted: %v", err)
	}
}

func TestUserRoleReplacementRollsBackBothDeleteAndInsert(t *testing.T) {
	repo, db := newPostgreSQLUserRepository(t)
	ctx := context.Background()
	roles := []model.Role{
		{Code: "old_user_role", Name: "Old", Status: 1},
		{Code: "new_user_role", Name: "New", Status: 1},
	}
	if err := db.Create(&roles).Error; err != nil {
		t.Fatal(err)
	}
	user := model.User{Username: "role-rollback", Nickname: "Rollback", PasswordHash: "hash", Status: 1}
	if err := repo.CreateWithRoles(ctx, &user, []int64{roles[0].ID}); err != nil {
		t.Fatal(err)
	}
	forced := errors.New("forced association insert failure")
	if err := db.Callback().Create().After("gorm:create").Register("test:user-role-rollback", func(tx *gorm.DB) {
		if tx.Statement.Table == "sys_user_role" {
			_ = tx.AddError(forced)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Create().Remove("test:user-role-rollback") })
	if err := repo.ReplaceRoles(ctx, user.ID, []int64{roles[1].ID}); !errors.Is(err, forced) {
		t.Fatalf("expected replacement failure: %v", err)
	}
	record, err := repo.FindWithRoles(ctx, user.ID)
	if err != nil || len(record.Roles) != 1 || record.Roles[0].ID != roles[0].ID {
		t.Fatalf("replacement escaped rollback: %+v %v", record, err)
	}
	if err := db.Callback().Create().Remove("test:user-role-rollback"); err != nil {
		t.Fatal(err)
	}
	if err := repo.ReplaceRoles(ctx, user.ID, []int64{roles[1].ID}); err != nil {
		t.Fatal(err)
	}
	record, err = repo.FindWithRoles(ctx, user.ID)
	if err != nil || len(record.Roles) != 1 || record.Roles[0].ID != roles[1].ID {
		t.Fatalf("successful replacement did not persist: %+v %v", record, err)
	}
}
