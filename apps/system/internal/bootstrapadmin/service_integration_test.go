//go:build integration

package bootstrapadmin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/migration"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/testutil"
	"gorm.io/gorm"
)

func TestCreateInitialAdministratorAssignsSeededSuperAdminRoleAtomically(t *testing.T) {
	db := newBootstrapDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	user, err := CreateInitialAdministrator(ctx, db, &passwordHasherStub{hash: "encoded-hash"}, Input{
		Username: "admin",
		Password: "secure-password",
		Nickname: "Administrator",
	})
	if err != nil {
		t.Fatalf("create initial administrator: %v", err)
	}
	var assignmentCount int64
	if err := db.Model(&model.UserRole{}).
		Joins("JOIN sys_role ON sys_role.id = sys_user_role.role_id").
		Where("sys_user_role.user_id = ? AND sys_role.code = ?", user.ID, InitialRoleCode).
		Count(&assignmentCount).Error; err != nil {
		t.Fatalf("query initial role assignment: %v", err)
	}
	if assignmentCount != 1 {
		t.Fatalf("unexpected initial role assignment count: %d", assignmentCount)
	}

	for _, state := range []string{"enabled", "disabled", "soft-deleted"} {
		t.Run(state, func(t *testing.T) {
			userStatus := model.RecordStatusEnabled
			var deletedAt *time.Time
			if state == "disabled" {
				userStatus = model.RecordStatusDisabled
			}
			if state == "soft-deleted" {
				deleted := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
				deletedAt = &deleted
			}
			if err := db.Unscoped().Model(&model.User{}).Where("id = ?", user.ID).
				Updates(map[string]any{"status": userStatus, "deleted_at": deletedAt}).Error; err != nil {
				t.Fatal(err)
			}
			created, err := CreateInitialAdministrator(ctx, db, &passwordHasherStub{hash: "hash"},
				Input{Username: "another-admin", Password: "secure-password"})
			if !errors.Is(err, ErrAdministratorExists) || created != nil {
				t.Fatalf("repeat initialization accepted: %+v %v", created, err)
			}
		})
	}
	var count int64
	if err := db.Unscoped().Model(&model.User{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("repeat initialization left extra users: %d %v", count, err)
	}
}

func TestConcurrentInitializationCreatesExactlyOneAdministrator(t *testing.T) {
	db := newBootstrapDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Both transactions must observe the seeded role before either attempts
	// its primary-key lock, reproducing concurrent first-time initialization.
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	if err := db.Callback().Query().After("gorm:query").Register("test:initial-role-read", func(tx *gorm.DB) {
		if tx.Statement.Table != "sys_role" {
			return
		}
		if _, locked := tx.Statement.Clauses["FOR"]; locked {
			return
		}
		arrived <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			_ = tx.AddError(ctx.Err())
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Query().Remove("test:initial-role-read") })
	results := make(chan error, 2)
	for _, username := range []string{"first-admin", "second-admin"} {
		go func() {
			_, err := CreateInitialAdministrator(ctx, db, &passwordHasherStub{hash: "hash"},
				Input{Username: username, Password: "secure-password"})
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-arrived:
		case <-ctx.Done():
			t.Fatal("initialization did not reach the concurrent role reads")
		}
	}
	close(release)
	var succeeded, rejected int
	for range 2 {
		select {
		case err := <-results:
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, ErrAdministratorExists):
				rejected++
			default:
				t.Fatalf("unexpected initialization error: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("initialization did not finish")
		}
	}
	if succeeded != 1 || rejected != 1 {
		t.Fatalf("initialization outcomes: succeeded=%d rejected=%d", succeeded, rejected)
	}
	var users, assignments int64
	if err := db.Model(&model.User{}).Count(&users).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.UserRole{}).Count(&assignments).Error; err != nil {
		t.Fatal(err)
	}
	if users != 1 || assignments != 1 {
		t.Fatalf("initialization created users=%d assignments=%d", users, assignments)
	}
}

func TestInitializationFailureRollsBackUserAndAllowsRetry(t *testing.T) {
	db := newBootstrapDatabase(t)
	ctx := context.Background()
	forced := errors.New("assignment failed")
	if err := db.Callback().Create().Before("gorm:create").Register("test:initial-assignment-failure", func(tx *gorm.DB) {
		if tx.Statement.Table == "sys_user_role" {
			_ = tx.AddError(forced)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Create().Remove("test:initial-assignment-failure") })
	input := Input{Username: "retry-admin", Password: "secure-password"}
	if user, err := CreateInitialAdministrator(ctx, db, &passwordHasherStub{hash: "hash"}, input); !errors.Is(err, forced) || user != nil {
		t.Fatalf("failed initialization returned user=%+v err=%v", user, err)
	}
	var users int64
	if err := db.Model(&model.User{}).Count(&users).Error; err != nil || users != 0 {
		t.Fatalf("failed initialization retained user: %d %v", users, err)
	}
	if err := db.Callback().Create().Remove("test:initial-assignment-failure"); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateInitialAdministrator(ctx, db, &passwordHasherStub{hash: "hash"}, input); err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
}

func newBootstrapDatabase(t *testing.T) *gorm.DB {
	t.Helper()
	db, sqlDB := testutil.OpenPostgres(t)
	provider, err := migration.NewProvider(sqlDB)
	if err != nil {
		t.Fatalf("create migration provider: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	return db
}
