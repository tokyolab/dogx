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

func TestLoginLogListRejectsInvalidQuery(t *testing.T) {
	r := &loginLogRepository{}
	for _, q := range []LoginLogListQuery{{}, {Limit: -1}, {Limit: 20, Offset: -1}} {
		if _, _, err := r.List(context.Background(), q); err == nil {
			t.Fatal("invalid pagination accepted")
		}
	}
	if _, _, err := r.List(nil, LoginLogListQuery{Limit: 20}); err == nil {
		t.Fatal("nil context accepted")
	}
}
