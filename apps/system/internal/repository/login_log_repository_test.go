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
