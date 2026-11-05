package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestUnboundCandidateQueryValidatesBeforeDatabaseAccess(t *testing.T) {
	repo, err := NewJobTaskRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-30 * time.Second)
	for _, input := range []struct {
		name   string
		now    time.Time
		cutoff time.Time
		cursor *UnboundCursor
		limit  int
	}{
		{"missing scan time", time.Time{}, cutoff, nil, 1},
		{"missing observation cutoff", now, time.Time{}, nil, 1},
		{"equal cutoff", now, now, nil, 1},
		{"future cutoff", now, now.Add(time.Second), nil, 1},
		{"zero limit", now, cutoff, nil, 0},
		{"negative limit", now, cutoff, nil, -1},
		{"missing cursor time", now, cutoff, &UnboundCursor{TaskID: 1}, 1},
		{"zero cursor ID", now, cutoff, &UnboundCursor{AttemptRegisteredAt: cutoff}, 1},
		{"negative cursor ID", now, cutoff, &UnboundCursor{AttemptRegisteredAt: cutoff, TaskID: -1}, 1},
		{"cursor beyond cutoff", now, cutoff, &UnboundCursor{AttemptRegisteredAt: cutoff.Add(time.Microsecond), TaskID: 1}, 1},
	} {
		t.Run(input.name, func(t *testing.T) {
			items, err := repo.ListUnboundCandidates(context.Background(), input.now, input.cutoff, input.cursor, input.limit)
			if items != nil || err == nil {
				t.Fatalf("invalid unbound candidate query: items=%v err=%v", items, err)
			}
		})
	}
}

func TestUnboundCandidateQueryPreservesDatabaseFailureAndAcceptsConfiguredPageSize(t *testing.T) {
	broken := errors.New("synthetic unbound candidate database failure")
	conn := &resultDriverConn{queryErr: broken}
	repo, err := NewJobTaskRepository(resultDriverDatabase(t, conn))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-30 * time.Second)
	cursor := &UnboundCursor{AttemptRegisteredAt: cutoff, TaskID: 1}
	items, err := repo.ListUnboundCandidates(context.Background(), now, cutoff, cursor, 129)
	if items != nil || !errors.Is(err, broken) {
		t.Fatalf("query failure was hidden or configured page size rejected: items=%v err=%v", items, err)
	}
}
