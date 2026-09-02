package repository

import (
	"errors"
	"testing"

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

func TestNewUserRepositoryRejectsNilDatabase(t *testing.T) {
	if _, err := NewUserRepository(nil); err == nil {
		t.Fatal("expected nil user repository database to be rejected")
	}
}
