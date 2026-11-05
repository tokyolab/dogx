package repository

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
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
	for _, batch := range []struct{ cursor, upper, limit int }{{-1, 1, 1}, {0, -1, 1}, {2, 1, 1}, {0, 1, 0}, {0, 1, 129}} {
		if ids, err := repo.ListWaitingIDs(ctx, int64(batch.cursor), int64(batch.upper), batch.limit); err == nil || ids != nil {
			t.Fatalf("accepted invalid batch %+v", batch)
		}
	}
	for _, position := range []int64{0, 17} {
		if ids, err := repo.ListWaitingIDs(ctx, position, position, 128); err != nil || ids == nil || len(ids) != 0 {
			t.Fatalf("empty interval ids=%v err=%v", ids, err)
		}
	}
	if items, err := repo.LoadWaiting(ctx, nil); err != nil || items == nil || len(items) != 0 {
		t.Fatalf("empty items=%v err=%v", items, err)
	}
	if _, err := repo.LoadWaiting(ctx, make([]int64, 1025)); err == nil {
		t.Fatal("accepted oversized waiting batch")
	}
}

func TestExecutionLockErrorMapsOnlyNowaitConflict(t *testing.T) {
	for _, code := range []string{"55P03", "57014", "40P01", "23505"} {
		driverErr := &pgconn.PgError{Code: code}
		err := executionInstanceLockError(fmt.Errorf("driver: %w", driverErr))
		if errors.Is(err, ErrInstanceLocked) != (code == "55P03") {
			t.Fatalf("SQLSTATE %s has unexpected conflict classification: %v", code, err)
		}
		var retained *pgconn.PgError
		if !errors.As(err, &retained) || retained != driverErr {
			t.Fatalf("SQLSTATE %s lost its driver cause", code)
		}
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, errors.New("transport unavailable")} {
		err := executionInstanceLockError(cause)
		if !errors.Is(err, cause) || errors.Is(err, ErrInstanceLocked) {
			t.Fatalf("technical failure became a lock conflict or lost its cause: %v", err)
		}
	}
}
