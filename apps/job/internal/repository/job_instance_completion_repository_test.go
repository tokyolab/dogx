package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
)

func TestInstanceCompletionRepositoryRejectsInvalidInputs(t *testing.T) {
	repo, err := NewJobInstanceRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{0, -1} {
		if exists, err := repo.Exists(context.Background(), id); exists || err == nil {
			t.Fatalf("invalid existence lookup: exists=%v err=%v", exists, err)
		}
		if item, err := repo.FindCompletion(context.Background(), id); item != nil || err == nil {
			t.Fatalf("invalid completion lookup: item=%+v err=%v", item, err)
		}
		if item, err := repo.FindByID(context.Background(), id); item != nil || err == nil {
			t.Fatalf("invalid management lookup: item=%+v err=%v", item, err)
		}
		if changed, err := repo.Complete(context.Background(), id, model.InstanceSucceeded, nil); changed || err == nil {
			t.Fatalf("invalid completion ID: changed=%v err=%v", changed, err)
		}
	}
	for _, target := range []model.InstanceStatus{0, model.InstanceWaiting, model.InstanceRunning, model.InstanceCanceled, model.InstanceStopping, 99} {
		if changed, err := repo.Complete(context.Background(), 1, target, nil); changed || err == nil {
			t.Fatalf("accepted completion target %d: changed=%v err=%v", target, changed, err)
		}
	}
}

func TestInstanceCompletionRepositoryRequiresConfirmedCommit(t *testing.T) {
	broken := errors.New("synthetic completion commit failure")
	for _, skipDefault := range []bool{false, true} {
		for _, commitErr := range []error{nil, broken} {
			conn := &resultDriverConn{commitErr: commitErr}
			db := resultDriverDatabase(t, conn).Session(&gorm.Session{SkipDefaultTransaction: skipDefault})
			repo, err := NewJobInstanceRepository(db)
			if err != nil {
				t.Fatal(err)
			}
			changed, err := repo.Complete(context.Background(), 1, model.InstanceSucceeded, nil)
			if commitErr == nil {
				if !changed || err != nil {
					t.Fatalf("confirmed completion: changed=%v err=%v", changed, err)
				}
			} else if changed || !errors.Is(err, broken) {
				t.Fatalf("unconfirmed completion: changed=%v err=%v", changed, err)
			}
			if conn.execs != 1 || conn.queries != 0 || conn.commits != 1 {
				t.Fatalf("completion lost its transaction or added a read: execs=%d queries=%d commits=%d", conn.execs, conn.queries, conn.commits)
			}
		}
	}
}

func TestInstanceCompletionRepositoryRejectsOuterTransaction(t *testing.T) {
	conn := &resultDriverConn{}
	tx := resultDriverDatabase(t, conn).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	repo, err := NewJobInstanceRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := repo.Complete(context.Background(), 1, model.InstanceFailed, nil); changed || err == nil || conn.execs != 0 || conn.commits != 0 {
		t.Fatalf("outer transaction looked committed: changed=%v err=%v execs=%d commits=%d", changed, err, conn.execs, conn.commits)
	}
}

func TestExecutionQueriesPreserveDatabaseErrors(t *testing.T) {
	broken := errors.New("synthetic execution read failure")
	db := resultDriverDatabase(t, &resultDriverConn{queryErr: broken})
	instances, err := NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	if item, err := instances.FindCompletion(context.Background(), 1); item != nil || !errors.Is(err, broken) {
		t.Fatalf("completion query failure became missing data: item=%+v err=%v", item, err)
	}
	if item, err := instances.FindByID(context.Background(), 1); item != nil || !errors.Is(err, broken) || errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("management query failure became not found: item=%+v err=%v", item, err)
	}
	if items, total, err := tasks.ListByInstance(context.Background(), 1, 1, 200); items != nil || total != 0 || !errors.Is(err, broken) {
		t.Fatalf("task query failure became empty page: items=%+v total=%d err=%v", items, total, err)
	}
}

func TestTaskSummaryRepositoryRejectsInvalidPagination(t *testing.T) {
	repo, err := NewJobTaskRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []struct{ instance, page, size int64 }{
		{0, 1, 20}, {-1, 1, 20}, {1, 0, 20}, {1, -1, 20}, {1, 1, 0}, {1, 1, -1}, {1, 1, 201}, {1, 1<<63 - 1, 200},
	} {
		if items, total, err := repo.ListByInstance(context.Background(), input.instance, input.page, input.size); items != nil || total != 0 || err == nil {
			t.Fatalf("invalid task page %+v: items=%v total=%d err=%v", input, items, total, err)
		}
	}
}
