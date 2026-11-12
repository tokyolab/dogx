package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestPostRepositoryValidatesBeforeDatabaseAccess(t *testing.T) {
	repo, err := NewJobTaskRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, now := context.Background(), time.Now()
	for _, id := range []int64{0, -1} {
		if stages, err := repo.FindStages(ctx, id); stages != nil || err == nil {
			t.Fatalf("invalid stage read: %+v %v", stages, err)
		}
		if exists, err := repo.HasUnfinishedShards(ctx, id); exists || err == nil {
			t.Fatalf("invalid unfinished shard read: %t %v", exists, err)
		}
		if counts, err := repo.CountShards(ctx, id); counts != (ShardCounts{}) || err == nil {
			t.Fatalf("invalid shard count read: %+v %v", counts, err)
		}
		if post, err := repo.FindPost(ctx, id); post != nil || err == nil {
			t.Fatalf("invalid Post read: %+v %v", post, err)
		}
	}
	for _, counts := range []ShardCounts{{}, {Total: 1, Succeeded: 1}, {Total: 500, Failed: 500}} {
		if id, err := repo.InsertPost(ctx, 1, counts, now); id != 0 || err == nil {
			t.Fatalf("nontransactional Post creation: id=%d err=%v", id, err)
		}
	}
	conn := &resultDriverConn{}
	tx := resultDriverDatabase(t, conn).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	repo, err = NewJobTaskRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	for _, counts := range []ShardCounts{
		{Total: -1}, {Succeeded: -1}, {Failed: -1}, {Canceled: -1}, {Total: 501, Succeeded: 501},
		{Total: 1}, {Total: 1, Succeeded: 2}, {Total: 1, Succeeded: 1, Failed: 1},
		{Total: 1, Succeeded: 1, Canceled: 1}, {Total: 1, Succeeded: 1<<63 - 1, Failed: 1<<63 - 1},
	} {
		if id, err := repo.InsertPost(ctx, 1, counts, now); id != 0 || err == nil {
			t.Fatalf("invalid Post counts accepted: counts=%+v id=%d err=%v", counts, id, err)
		}
	}
	for _, input := range []struct {
		id int64
		at time.Time
	}{{0, now}, {-1, now}, {1, time.Time{}}} {
		if id, err := repo.InsertPost(ctx, input.id, ShardCounts{}, input.at); id != 0 || err == nil {
			t.Fatalf("invalid Post identity/time accepted: id=%d err=%v", id, err)
		}
	}
	if conn.queries != 0 || conn.execs != 0 || conn.commits != 0 {
		t.Fatalf("invalid Post creation touched the database: queries=%d execs=%d commits=%d", conn.queries, conn.execs, conn.commits)
	}
}

func TestPostRepositoryPreservesReadAndWriteFailure(t *testing.T) {
	failure := errors.New("synthetic Post repository failure")
	conn := &resultDriverConn{queryErr: failure}
	db := resultDriverDatabase(t, conn)
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if stages, err := repo.FindStages(ctx, 1); stages != nil || !errors.Is(err, failure) {
		t.Fatalf("stage failure became absence: %+v %v", stages, err)
	}
	if exists, err := repo.HasUnfinishedShards(ctx, 1); exists || !errors.Is(err, failure) {
		t.Fatalf("unfinished failure became completion: %t %v", exists, err)
	}
	if counts, err := repo.CountShards(ctx, 1); counts != (ShardCounts{}) || !errors.Is(err, failure) {
		t.Fatalf("aggregate failure became zero shards: %+v %v", counts, err)
	}
	if post, err := repo.FindPost(ctx, 1); post != nil || !errors.Is(err, failure) {
		t.Fatalf("Post failure became absence: %+v %v", post, err)
	}
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	repo, err = NewJobTaskRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := repo.InsertPost(ctx, 1, ShardCounts{}, time.Now()); id != 0 || !errors.Is(err, failure) || conn.commits != 0 {
		t.Fatalf("failed Post insert reported creation or committed: id=%d err=%v commits=%d", id, err, conn.commits)
	}
}

func TestPostRepositoryDistinguishesMissingStagesFromMissingAggregateAnswer(t *testing.T) {
	conn := &resultDriverConn{}
	repo, err := NewJobTaskRepository(resultDriverDatabase(t, conn))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	stages, err := repo.FindStages(ctx, 1)
	if err != nil || stages == nil || stages.Preparation != nil || stages.Post != nil {
		t.Fatalf("empty stage query: %+v %v", stages, err)
	}
	if post, err := repo.FindPost(ctx, 1); post != nil || err != nil {
		t.Fatalf("absent Post: %+v %v", post, err)
	}
	if exists, err := repo.HasUnfinishedShards(ctx, 1); exists || err != nil {
		t.Fatalf("absent unfinished shard: %t %v", exists, err)
	}
	// PostgreSQL's ungrouped COUNT always returns one row, even for an empty set.
	// A driver returning no aggregate row is not evidence of zero shards.
	if counts, err := repo.CountShards(ctx, 1); counts != (ShardCounts{}) || err == nil {
		t.Fatalf("absent aggregate answer became zero-shard proof: %+v %v", counts, err)
	}
}
