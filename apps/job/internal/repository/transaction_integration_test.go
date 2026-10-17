//go:build integration

package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
)

func TestTransactionRunnerCommitsOrRollsBackAllRepositories(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		name := "commit"
		if rollback {
			name = "rollback"
		}
		t.Run(name, func(t *testing.T) {
			db, _ := testutil.OpenMigratedPostgres(t)
			group := model.ExecutorGroup{Name: "transaction group"}
			if err := db.Create(&group).Error; err != nil {
				t.Fatal(err)
			}
			run, err := NewTransactionRunner(db)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			cause := errors.New("abort business transaction")
			err = run(ctx, func(r Repositories) error {
				if err := r.Groups.LockAvailable(ctx, group.ID); err != nil {
					return err
				}
				definition := &model.JobDefinition{Name: "transaction job", ExecutorGroupID: group.ID,
					ProcessorName: "processor", CronExpr: "0 * * * * *", ExecutionMode: model.ExecutionModeNormal,
					RetryIntervalSeconds: 10, TimeoutSeconds: 900}
				id, err := r.Jobs.Create(ctx, definition)
				if err != nil {
					return err
				}
				requestID := uuid.MustParse("17922155-2547-4500-8000-000000000001")
				operatorID := int64(1)
				_, created, err := r.Instances.Insert(ctx, &model.JobInstance{JobID: id, JobName: definition.Name,
					ExecutorGroupID: group.ID, TriggerType: model.TriggerManual, RequestID: &requestID,
					TriggeredBy: &operatorID, ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1}})
				if err != nil {
					return err
				}
				if !created {
					t.Fatal("instance was not inserted")
				}
				// All reads must see this transaction's uncommitted writes.
				if row, err := r.Jobs.Get(ctx, id); err != nil || row == nil {
					t.Fatalf("transaction job=%+v error=%v", row, err)
				}
				if active, err := r.Instances.HasActive(ctx, id); err != nil || !active {
					t.Fatalf("transaction active=%t error=%v", active, err)
				}
				if rollback {
					return cause
				}
				return nil
			})
			if (rollback && !errors.Is(err, cause)) || (!rollback && err != nil) {
				t.Fatalf("transaction error=%v", err)
			}
			want := int64(1)
			if rollback {
				want = 0
			}
			for _, record := range []any{&model.JobDefinition{}, &model.JobInstance{}} {
				var count int64
				if err := db.Model(record).Count(&count).Error; err != nil || count != want {
					t.Fatalf("%T count=%d want=%d error=%v", record, count, want, err)
				}
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if err := run(canceled, func(Repositories) error { t.Fatal("canceled transaction entered callback"); return nil }); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
		})
	}
}
