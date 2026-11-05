package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestRunningCandidatesRejectInvalidBoundsBeforeDatabaseUse(t *testing.T) {
	repo, err := NewJobInstanceRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	bound := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	for _, input := range []struct {
		name   string
		before time.Time
		after  *RunningCandidate
		limit  int
	}{
		{"zero bound", time.Time{}, nil, 256},
		{"zero limit", bound, nil, 0},
		{"negative limit", bound, nil, -1},
		{"zero cursor ID", bound, &RunningCandidate{StartedAt: bound.Add(-time.Second)}, 256},
		{"negative cursor ID", bound, &RunningCandidate{ID: -1, StartedAt: bound.Add(-time.Second)}, 256},
		{"zero cursor time", bound, &RunningCandidate{ID: 1}, 256},
		{"cursor at bound", bound, &RunningCandidate{ID: 1, StartedAt: bound}, 256},
		{"cursor after bound", bound, &RunningCandidate{ID: 1, StartedAt: bound.Add(time.Microsecond)}, 256},
	} {
		t.Run(input.name, func(t *testing.T) {
			items, err := repo.ListRunningCandidates(context.Background(), input.before, input.after, input.limit)
			if err == nil || items != nil {
				t.Fatalf("invalid candidate input reached storage: items=%v error=%v", items, err)
			}
		})
	}
}

func TestRunningCandidateFailuresPreserveCauseAndDoNotAdvanceInput(t *testing.T) {
	cause := errors.New("synthetic running candidate query failure")
	conn := &resultDriverConn{queryErr: cause}
	repo, err := NewJobInstanceRepository(resultDriverDatabase(t, conn))
	if err != nil {
		t.Fatal(err)
	}
	before := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	after := RunningCandidate{ID: 1, StartedAt: before.Add(-time.Minute)}
	original := after
	items, err := repo.ListRunningCandidates(context.Background(), before, &after, 256)
	if items != nil || !errors.Is(err, cause) || after != original || conn.queries != 1 || conn.execs != 0 || conn.commits != 0 {
		t.Fatalf("failed query changed facts or lost its cause: items=%v after=%v query=%d exec=%d commit=%d error=%v",
			items, after, conn.queries, conn.execs, conn.commits, err)
	}
}

func TestRunningCandidatePageLimitRemainsDeploymentControlled(t *testing.T) {
	conn := &resultDriverConn{}
	repo, err := NewJobInstanceRepository(resultDriverDatabase(t, conn))
	if err != nil {
		t.Fatal(err)
	}
	// 256 is a deployment initial value, not a new repository hard limit.
	items, err := repo.ListRunningCandidates(context.Background(), time.Now(), nil, 257)
	if err != nil || items == nil || len(items) != 0 || conn.queries != 1 || conn.execs != 0 {
		t.Fatalf("positive caller-bounded page was rejected or became a write: items=%v error=%v", items, err)
	}
}
