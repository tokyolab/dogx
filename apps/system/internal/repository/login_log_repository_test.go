package repository

import (
	"context"
	"testing"
)

func TestLoginLogRepositoryRejectsNilLog(t *testing.T) {
	repository := &loginLogRepository{}
	if err := repository.Create(context.Background(), nil); err == nil {
		t.Fatal("expected nil login log to be rejected")
	}
}

func TestNewLoginLogRepositoryRejectsNilDatabase(t *testing.T) {
	if _, err := NewLoginLogRepository(nil); err == nil {
		t.Fatal("expected nil login log repository database to be rejected")
	}
}
