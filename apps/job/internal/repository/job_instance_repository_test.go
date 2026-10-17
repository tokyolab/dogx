package repository

import (
	"context"
	"testing"

	"gorm.io/gorm"
)

func TestInstanceRepositoryRejectsInvalidInputsBeforeDatabaseAccess(t *testing.T) {
	repo, err := NewJobInstanceRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if id, created, err := repo.Insert(ctx, nil); err == nil || id != 0 || created {
		t.Fatalf("nil instance id=%d created=%v err=%v", id, created, err)
	}
	for _, batch := range []struct{ cursor, limit int }{{-1, 1}, {0, 0}, {0, 1025}} {
		if _, err := repo.ListWaitingIDs(ctx, int64(batch.cursor), batch.limit); err == nil {
			t.Fatalf("accepted invalid batch %+v", batch)
		}
	}
	if items, err := repo.LoadWaiting(ctx, nil); err != nil || items == nil || len(items) != 0 {
		t.Fatalf("empty items=%v err=%v", items, err)
	}
	if _, err := repo.LoadWaiting(ctx, make([]int64, 1025)); err == nil {
		t.Fatal("accepted oversized waiting batch")
	}
}
