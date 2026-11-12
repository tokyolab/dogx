package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
)

func TestRetryRepositoryValidatesBeforeDatabaseAccess(t *testing.T) {
	repo, err := NewJobTaskRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 5, 1, 2, 3, 0, time.UTC)
	for _, input := range []struct {
		now    time.Time
		cursor *DispatchCursor
		limit  int
	}{
		{time.Time{}, nil, 1}, {now, nil, 0}, {now, nil, 129},
		{now, &DispatchCursor{TaskID: 1}, 1},
		{now, &DispatchCursor{TaskID: 0, NextDispatchAt: now}, 1},
		{now, &DispatchCursor{TaskID: 1, NextDispatchAt: now.Add(time.Second)}, 1},
	} {
		if items, err := repo.ListDispatchCandidates(context.Background(), input.now, input.cursor, input.limit); items != nil || err == nil {
			t.Fatalf("invalid retry query: items=%v err=%v", items, err)
		}
	}
	valid := AttemptRegistration{TaskType: model.TaskTypeOrdinary, ExpectedStatus: model.TaskRetryWaiting, TaskID: 1, InstanceID: 2, ExpectedAttemptNo: 1, MaxAttempts: 4, RegisteredAt: now}
	for _, mutate := range []func(*AttemptRegistration){
		func(w *AttemptRegistration) { w.TaskID = 0 },
		func(w *AttemptRegistration) { w.InstanceID = -1 },
		func(w *AttemptRegistration) { w.ExpectedAttemptNo = 0 },
		func(w *AttemptRegistration) { w.MaxAttempts = 0 },
		func(w *AttemptRegistration) { w.RegisteredAt = time.Time{} },
	} {
		write := valid
		mutate(&write)
		if task, err := repo.RegisterAttempt(context.Background(), write); task != nil || err == nil {
			t.Fatalf("invalid retry registration: task=%+v err=%v", task, err)
		}
	}
	if task, err := repo.RegisterAttempt(context.Background(), valid); task != nil || err == nil {
		t.Fatalf("nontransactional registration was accepted: task=%+v err=%v", task, err)
	}
}

func TestRetryRepositoryKeepsDatabaseFailureAndOuterCommitOwnership(t *testing.T) {
	broken := errors.New("synthetic retry database failure")
	conn := &resultDriverConn{queryErr: broken}
	db := resultDriverDatabase(t, conn)
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	if items, err := repo.ListDispatchCandidates(context.Background(), time.Now(), nil, 128); items != nil || !errors.Is(err, broken) {
		t.Fatalf("query failure became an empty page: items=%v err=%v", items, err)
	}
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	bound, err := NewJobTaskRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	write := AttemptRegistration{TaskType: model.TaskTypeOrdinary, ExpectedStatus: model.TaskRetryWaiting, TaskID: 1, InstanceID: 2, ExpectedAttemptNo: 1, MaxAttempts: 4, RegisteredAt: time.Now()}
	if task, err := bound.RegisterAttempt(context.Background(), write); task != nil || !errors.Is(err, broken) || conn.commits != 0 {
		t.Fatalf("registration hid failure or committed its caller's transaction: task=%+v err=%v commits=%d", task, err, conn.commits)
	}
}
