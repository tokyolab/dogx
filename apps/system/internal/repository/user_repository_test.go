package repository

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"gorm.io/gorm"
)

func TestMapUserError(t *testing.T) {
	if err := mapUserError(gorm.ErrRecordNotFound); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("expected ErrUserNotFound, got %v", err)
	}

	databaseErr := errors.New("database unavailable")
	if err := mapUserError(databaseErr); !errors.Is(err, databaseErr) {
		t.Fatalf("expected wrapped database error, got %v", err)
	}
}

func TestUserWriteErrorsPreserveBusinessMeaning(t *testing.T) {
	for constraint, want := range map[string]error{
		"uk_sys_user_username_active": ErrUsernameExists,
		"uk_sys_user_email_active":    ErrUserEmailExists,
		"uk_sys_user_phone_active":    ErrUserPhoneExists,
	} {
		if got := mapUserWriteError(&pgconn.PgError{Code: "23505", ConstraintName: constraint}); !errors.Is(got, want) {
			t.Fatalf("%s: %v", constraint, got)
		}
	}
	unknown := &pgconn.PgError{Code: "23505", ConstraintName: "other_constraint"}
	if !errors.Is(mapUserWriteError(unknown), unknown) {
		t.Fatal("unknown error lost")
	}
	if mapUserWriteError(nil) != nil {
		t.Fatal("nil error was changed")
	}
}

func TestNewUserRepositoryRejectsNilDatabase(t *testing.T) {
	if _, err := NewUserRepository(nil); err == nil {
		t.Fatal("expected nil user repository database to be rejected")
	}
}

func TestInitializedSuperAdminProtectionDoesNotDependOnStatusOrID(t *testing.T) {
	for _, state := range []model.RecordStatus{model.RecordStatusEnabled, model.RecordStatusDisabled} {
		record := &UserRecord{
			User:  model.User{Base: model.Base{ID: 42}, Status: state},
			Roles: []model.Role{{Code: model.SuperAdminRoleCode, Status: state}},
		}
		if err := protectSuperAdmin(record); !errors.Is(err, ErrSuperAdminProtected) {
			t.Fatalf("initialized administrator was not protected at status %d: %v", state, err)
		}
	}
	if err := protectSuperAdmin(&UserRecord{User: model.User{Base: model.Base{ID: 1}}}); err != nil {
		t.Fatalf("ordinary user ID 1 incorrectly protected: %v", err)
	}
}
