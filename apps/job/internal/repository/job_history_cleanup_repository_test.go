package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestHistoryCleanupRejectsInvalidBoundsAndImplicitTransactions(t *testing.T) {
	instances, err := NewJobInstanceRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := NewJobTaskRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	before := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	for _, input := range []struct {
		before time.Time
		after  *HistoryCandidate
		limit  int
	}{
		{time.Time{}, nil, 100}, {before, nil, 0}, {before, nil, -1}, {before, nil, 101},
		{before, &HistoryCandidate{FinishedAt: before.Add(-time.Second)}, 100},
		{before, &HistoryCandidate{ID: -1, FinishedAt: before.Add(-time.Second)}, 100},
		{before, &HistoryCandidate{ID: 1}, 100},
		{before, &HistoryCandidate{ID: 1, FinishedAt: before}, 100},
		{before, &HistoryCandidate{ID: 1, FinishedAt: before.Add(time.Microsecond)}, 100},
	} {
		if page, err := instances.ListExpiredHistory(context.Background(), input.before, input.after, input.limit); err == nil || page != nil {
			t.Fatalf("invalid bounds reached storage: input=%+v page=%v error=%v", input, page, err)
		}
	}
	for _, id := range []int64{-1, 0, 1} {
		if row, err := instances.LockForHistoryCleanup(context.Background(), id); row != nil || err == nil {
			t.Fatalf("invalid or implicit lock: id=%d row=%+v error=%v", id, row, err)
		}
		if deleted, err := instances.DeleteHistory(context.Background(), id, before); deleted || err == nil {
			t.Fatalf("invalid or implicit parent delete: id=%d deleted=%t error=%v", id, deleted, err)
		}
		if count, err := tasks.DeleteHistoryForInstance(context.Background(), id); count != 0 || err == nil {
			t.Fatalf("invalid or implicit child delete: id=%d count=%d error=%v", id, count, err)
		}
	}
	if deleted, err := instances.DeleteHistory(context.Background(), 1, time.Time{}); deleted || err == nil {
		t.Fatalf("zero cutoff accepted: deleted=%t error=%v", deleted, err)
	}
}

func TestHistoryCandidatesPreserveQueryFailureAndCursor(t *testing.T) {
	cause := errors.New("synthetic history query failure")
	conn := &resultDriverConn{queryErr: cause}
	repo, err := NewJobInstanceRepository(resultDriverDatabase(t, conn))
	if err != nil {
		t.Fatal(err)
	}
	before := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	after := HistoryCandidate{ID: 10, FinishedAt: before.Add(-time.Hour)}
	original := after
	page, err := repo.ListExpiredHistory(context.Background(), before, &after, 100)
	if !errors.Is(err, cause) || page != nil || after != original || conn.queries != 1 || conn.execs != 0 || conn.commits != 0 {
		t.Fatalf("query failure changed facts: page=%v cursor=%+v queries=%d writes=%d commits=%d error=%v",
			page, after, conn.queries, conn.execs, conn.commits, err)
	}
}
