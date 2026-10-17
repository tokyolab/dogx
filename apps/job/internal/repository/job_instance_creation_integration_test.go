//go:build integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
)

func TestInstanceRepositoryCreateIgnoresExistingIdentityAndExecutionFields(t *testing.T) {
	db, job := repositoryFixture(t)
	repo, err := NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	var databaseTime time.Time
	if err := db.Raw("SELECT CURRENT_TIMESTAMP").Scan(&databaseTime).Error; err != nil {
		t.Fatal(err)
	}
	previousTime := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	text := "must not be inserted"
	instance := repositoryInstance(job)
	instance.ID, instance.Status = 987654321, model.InstanceSucceeded
	instance.CreatedAt, instance.UpdatedAt = previousTime, previousTime
	instance.StartedAt, instance.DeadlineAt, instance.FinishedAt = &previousTime, &previousTime, &previousTime
	instance.EndReason, instance.ResultSummary = &text, &text
	ctx := context.Background()
	id, created, err := repo.Insert(ctx, instance)
	if err != nil || !created || id <= 0 || id == 987654321 || instance.ID != id || instance.Status != model.InstanceWaiting {
		t.Fatalf("insert id=%d created=%v input=%+v err=%v", id, created, instance, err)
	}
	var stored model.JobInstance
	if err := db.First(&stored, id).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.InstanceWaiting || stored.StartedAt != nil || stored.DeadlineAt != nil || stored.FinishedAt != nil || stored.EndReason != nil || stored.ResultSummary != nil {
		t.Fatalf("execution fields escaped trigger insert: %+v", stored)
	}
	if !stored.CreatedAt.Equal(databaseTime) || !stored.UpdatedAt.Equal(databaseTime) {
		t.Fatalf("audit defaults created=%v updated=%v want=%v", stored.CreatedAt, stored.UpdatedAt, databaseTime)
	}
	if duplicate, created, err := repo.Insert(ctx, instance); err != nil || created || duplicate != 0 {
		t.Fatalf("same object duplicate id=%d created=%v err=%v", duplicate, created, err)
	}
	next := instance.ScheduledAt.Add(time.Minute)
	instance.ScheduledAt = &next
	nextID, created, err := repo.Insert(ctx, instance)
	if err != nil || !created || nextID == id || instance.ID != nextID {
		t.Fatalf("reused object new identity id=%d created=%v err=%v", nextID, created, err)
	}
}
