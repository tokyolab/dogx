package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
)

func TestStopRepositoriesRejectInvalidInputAndNontransactionalHandles(t *testing.T) {
	instances, err := NewJobInstanceRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := NewJobTaskRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	for _, id := range []int64{-1, 0, 1} {
		for _, nowait := range []bool{false, true} {
			if row, err := instances.LockForStop(ctx, id, nowait); row != nil || err == nil {
				t.Fatalf("invalid or nontransactional lock: row=%+v err=%v", row, err)
			}
		}
		if changed, err := instances.RecordStop(ctx, id, model.InstanceRunning, model.EndReasonManualStop, "stopped", now); changed || err == nil {
			t.Fatalf("invalid or nontransactional decision: changed=%v err=%v", changed, err)
		}
		if changed, err := instances.FinishStop(ctx, id, model.EndReasonManualStop); changed || err == nil {
			t.Fatalf("invalid or nontransactional finish: changed=%v err=%v", changed, err)
		}
		for _, limit := range []int{-1, 0, 257} {
			if rows, err := tasks.ListUnfinishedIDs(ctx, id, limit); rows != nil || err == nil {
				t.Fatalf("invalid or nontransactional child query: rows=%v err=%v", rows, err)
			}
		}
	}
	for _, expected := range []model.InstanceStatus{0, model.InstanceStopping, model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled, 99} {
		if changed, err := instances.RecordStop(ctx, 1, expected, model.EndReasonManualStop, "stopped", now); changed || err == nil {
			t.Fatalf("invalid stop source %d: changed=%v err=%v", expected, changed, err)
		}
	}
	for _, reason := range []string{"", "invalid", model.EndReasonSuccess} {
		if changed, err := instances.RecordStop(ctx, 1, model.InstanceRunning, reason, "stopped", now); changed || err == nil {
			t.Fatal("invalid stop reason was accepted")
		}
		if changed, err := instances.FinishStop(ctx, 1, reason); changed || err == nil {
			t.Fatal("invalid final stop reason was accepted")
		}
	}
	if changed, err := instances.RecordStop(ctx, 1, model.InstanceWaiting, model.EndReasonInstanceTimeout, "expired", now); changed || err == nil {
		t.Fatal("a waiting Instance was accepted as timed out")
	}
	if changed, err := instances.RecordStop(ctx, 1, model.InstanceRunning, model.EndReasonManualStop, "stopped", time.Time{}); changed || err == nil {
		t.Fatal("missing decision time was accepted")
	}
	valid := TaskCancellation{InstanceID: 1, TaskIDs: []int64{2}, Reason: model.EndReasonManualStop, Summary: "stopped", ProcessedAt: now}
	for _, mutate := range []func(*TaskCancellation){
		func(w *TaskCancellation) { w.InstanceID = 0 },
		func(w *TaskCancellation) { w.TaskIDs = nil },
		func(w *TaskCancellation) { w.TaskIDs = []int64{2, 0} },
		func(w *TaskCancellation) { w.TaskIDs = []int64{-1} },
		func(w *TaskCancellation) { w.Reason = "invalid" },
		func(w *TaskCancellation) { w.ProcessedAt = time.Time{} },
		func(_ *TaskCancellation) {},
	} {
		write := valid
		mutate(&write)
		if count, err := tasks.CancelUnfinished(ctx, write); count != 0 || err == nil {
			t.Fatalf("invalid or nontransactional child cancellation: count=%d err=%v", count, err)
		}
	}
}

func TestStopRepositoriesPreserveErrorsAndOuterTransactionOwnership(t *testing.T) {
	broken := errors.New("synthetic stop database failure")
	conn := &resultDriverConn{queryErr: broken}
	tx := resultDriverDatabase(t, conn).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	instances, err := NewJobInstanceRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := NewJobTaskRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	ctx, now := context.Background(), time.Now()
	if row, err := instances.LockForStop(ctx, 1, true); row != nil || !errors.Is(err, broken) || errors.Is(err, ErrInstanceLocked) {
		t.Fatalf("lock technical failure became normal contention: row=%+v err=%v", row, err)
	}
	if rows, err := tasks.ListUnfinishedIDs(ctx, 1, 257); rows != nil || !errors.Is(err, broken) {
		t.Fatalf("query failure hidden or configured limit rejected: rows=%v err=%v", rows, err)
	}
	if changed, err := instances.RecordStop(ctx, 1, model.InstanceRunning, model.EndReasonManualStop, "stopped", now); !changed || err != nil {
		t.Fatalf("transactional decision failed: changed=%v err=%v", changed, err)
	}
	if count, err := tasks.CancelUnfinished(ctx, TaskCancellation{InstanceID: 1, TaskIDs: []int64{2}, Reason: model.EndReasonManualStop, ProcessedAt: now}); count != 1 || err != nil {
		t.Fatalf("transactional cancellation failed: count=%d err=%v", count, err)
	}
	if changed, err := instances.FinishStop(ctx, 1, model.EndReasonManualStop); !changed || err != nil {
		t.Fatalf("transactional completion failed: changed=%v err=%v", changed, err)
	}
	if conn.execs != 3 || conn.commits != 0 {
		t.Fatal("stop repository committed its caller's transaction")
	}
}

func TestStopLockClassifiesOnlyNowaitContention(t *testing.T) {
	for _, nowait := range []bool{false, true} {
		driverErr := &pgconn.PgError{Code: "55P03", Message: "synthetic lock failure"}
		conn := &resultDriverConn{queryErr: driverErr}
		tx := resultDriverDatabase(t, conn).Begin()
		if tx.Error != nil {
			t.Fatal(tx.Error)
		}
		repo, err := NewJobInstanceRepository(tx)
		if err != nil {
			t.Fatal(err)
		}
		row, err := repo.LockForStop(context.Background(), 1, nowait)
		_ = tx.Rollback()
		if row != nil || !errors.Is(err, driverErr) || errors.Is(err, ErrInstanceLocked) != nowait {
			t.Fatalf("wrong stop lock classification for nowait=%v: row=%+v err=%v", nowait, row, err)
		}
	}
}
