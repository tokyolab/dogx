package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
)

func TestShardRepositoryRejectsInvalidQueriesAndUnownedWrites(t *testing.T) {
	repo, err := NewJobTaskRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, now := context.Background(), time.Now()
	for _, id := range []int64{-1, 0} {
		if item, err := repo.FindPreparation(ctx, id); item != nil || err == nil {
			t.Fatalf("invalid preparation read: %+v %v", item, err)
		}
		if item, err := repo.FindPreparationResult(ctx, id); item != nil || err == nil {
			t.Fatalf("invalid result read: %+v %v", item, err)
		}
		if exists, err := repo.HasShards(ctx, id); exists || err == nil {
			t.Fatalf("invalid shard read: %t %v", exists, err)
		}
	}
	for _, input := range []struct {
		id    int64
		at    time.Time
		limit int
	}{{0, now, 1}, {1, time.Time{}, 1}, {1, now, 0}, {1, now, 129}} {
		if items, err := repo.ListInstanceDispatchCandidates(ctx, input.id, input.at, input.limit); items != nil || err == nil {
			t.Fatalf("invalid targeted candidates: %+v %v", items, err)
		}
	}
	for _, stage := range []model.TaskType{0, model.TaskTypeOrdinary, model.TaskTypePrepare, model.TaskTypeShard, model.TaskTypePost} {
		if id, err := repo.InsertInitial(ctx, 1, stage, now); id != 0 || err == nil {
			t.Fatalf("invalid stage or missing transaction accepted: %d %v", id, err)
		}
	}
	if err := repo.InsertShards(ctx, 1, []model.TaskData{model.TaskData(`null`)}, now); err == nil {
		t.Fatal("shards were accepted outside a transaction")
	}
}

func TestShardRepositoryValidatesCompleteSetBeforeInsert(t *testing.T) {
	conn := &resultDriverConn{}
	tx := resultDriverDatabase(t, conn).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	repo, err := NewJobTaskRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	for _, items := range [][]model.TaskData{nil, {}, make([]model.TaskData, 501), {nil}, {model.TaskData(`{`)}, {model.TaskData(`1`), nil}} {
		if err := repo.InsertShards(context.Background(), 1, items, time.Now()); err == nil || conn.execs != 0 || conn.queries != 0 || conn.commits != 0 {
			t.Fatalf("invalid list wrote a prefix or committed: %v execs=%d queries=%d commits=%d", err, conn.execs, conn.queries, conn.commits)
		}
	}
	for _, stage := range []model.TaskType{model.TaskTypeOrdinary, model.TaskTypePrepare, model.TaskTypeShard, model.TaskTypePost} {
		write := AttemptRegistration{TaskID: 1, InstanceID: 2, TaskType: stage, ExpectedStatus: model.TaskWaiting,
			ExpectedAttemptNo: 1, MaxAttempts: 1, RegisteredAt: time.Now()}
		if task, err := repo.RegisterAttempt(context.Background(), write); task != nil || err == nil || conn.queries != 0 {
			t.Fatalf("waiting nonzero attempt accepted: %+v %v", task, err)
		}
		if stage == model.TaskTypeOrdinary || stage == model.TaskTypePrepare {
			write.ExpectedAttemptNo = 0
			if task, err := repo.RegisterAttempt(context.Background(), write); task != nil || err == nil || conn.queries != 0 {
				t.Fatalf("initial stage accepted through candidate registration: %+v %v", task, err)
			}
		}
	}
}

func TestShardRepositoryPreservesReadFailures(t *testing.T) {
	failure := errors.New("synthetic shard read failure")
	conn := &resultDriverConn{queryErr: failure}
	repo, err := NewJobTaskRepository(resultDriverDatabase(t, conn))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if item, err := repo.FindPreparation(ctx, 1); item != nil || !errors.Is(err, failure) {
		t.Fatalf("preparation read failure became absence: %+v %v", item, err)
	}
	if item, err := repo.FindPreparationResult(ctx, 1); item != nil || !errors.Is(err, failure) {
		t.Fatalf("preparation result failure became absence: %+v %v", item, err)
	}
	if exists, err := repo.HasShards(ctx, 1); exists || !errors.Is(err, failure) {
		t.Fatalf("shard read failure became absence: %t %v", exists, err)
	}
	if items, err := repo.ListInstanceDispatchCandidates(ctx, 1, time.Now(), 1); items != nil || !errors.Is(err, failure) {
		t.Fatalf("targeted read failure became empty success: %+v %v", items, err)
	}
}
