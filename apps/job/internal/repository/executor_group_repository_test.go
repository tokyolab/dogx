package repository

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestExecutorGroupWriteErrorPreservesUnrelatedFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		code string
		key  string
		want error
	}{
		{"name", "23505", "uq_job_executor_group_name", ErrExecutorGroupNameExists},
		{"key", "23505", "uq_job_executor_group_discovery_key", ErrExecutorGroupDiscoveryKeyExists},
		{"other unique", "23505", "job_executor_group_pkey", nil},
		{"other SQLSTATE", "23502", "uq_job_executor_group_name", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cause := &pgconn.PgError{Code: tc.code, ConstraintName: tc.key}
			err := mapExecutorGroupWriteError(fmt.Errorf("driver: %w", cause))
			if tc.want != nil {
				if !errors.Is(err, tc.want) {
					t.Fatalf("error = %v, want %v", err, tc.want)
				}
			} else if !errors.Is(err, cause) {
				t.Fatalf("database cause was discarded: %v", err)
			}
		})
	}
}

func TestExecutorGroupRepositoryRejectsInvalidInputsBeforeQuery(t *testing.T) {
	repo := &executorGroupRepository{}
	if id, err := repo.Create(context.Background(), nil); id != 0 || err == nil {
		t.Fatalf("nil create: id = %d, error = %v", id, err)
	}
	for _, f := range []ExecutorGroupFilter{
		{Page: 0, PageSize: 1}, {Page: 1, PageSize: 0}, {Page: 1, PageSize: 201},
		{Page: math.MaxInt64, PageSize: 200},
	} {
		if _, _, err := repo.List(context.Background(), f); err == nil {
			t.Fatalf("accepted pagination %+v", f)
		}
	}
}
