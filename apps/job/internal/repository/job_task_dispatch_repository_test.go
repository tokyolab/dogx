package repository

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

func dispatchBusyFixture() DispatchBusyWrite {
	return DispatchBusyWrite{TaskID: 1, InstanceID: 2, AttemptNo: 1, Delay: 4567*time.Millisecond + 321*time.Microsecond}
}

func dispatchRejectionFixture() DispatchRejectionWrite {
	return DispatchRejectionWrite{TaskID: 1, InstanceID: 2, AttemptNo: 1,
		MaxRetries: 3, RetryIntervalSeconds: 10, Summary: "executor rejected task before admission"}
}

func TestTaskDispatchRepositoryRejectsInvalidInputs(t *testing.T) {
	repo, err := NewJobTaskRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := repo.BindExecutor(context.Background(), ExecutorBinding{}); applied || !errors.Is(err, ErrInvalidDispatchWrite) {
		t.Fatalf("invalid binding was not classified: applied=%v err=%v", applied, err)
	}
	for name, mutate := range map[string]func(*DispatchBusyWrite){
		"task":           func(w *DispatchBusyWrite) { w.TaskID = 0 },
		"instance":       func(w *DispatchBusyWrite) { w.InstanceID = -1 },
		"attempt":        func(w *DispatchBusyWrite) { w.AttemptNo = 0 },
		"zero delay":     func(w *DispatchBusyWrite) { w.Delay = 0 },
		"negative delay": func(w *DispatchBusyWrite) { w.Delay = -time.Second },
	} {
		t.Run("BUSY/"+name, func(t *testing.T) {
			write := dispatchBusyFixture()
			mutate(&write)
			if applied, err := repo.SaveDispatchBusy(context.Background(), write); applied || !errors.Is(err, ErrInvalidDispatchWrite) {
				t.Fatalf("invalid BUSY: applied=%v err=%v", applied, err)
			}
		})
	}
	for name, mutate := range map[string]func(*DispatchRejectionWrite){
		"task":              func(w *DispatchRejectionWrite) { w.TaskID = 0 },
		"instance":          func(w *DispatchRejectionWrite) { w.InstanceID = -1 },
		"attempt":           func(w *DispatchRejectionWrite) { w.AttemptNo = 0 },
		"retries":           func(w *DispatchRejectionWrite) { w.MaxRetries = -1 },
		"zero interval":     func(w *DispatchRejectionWrite) { w.RetryIntervalSeconds = 0 },
		"negative interval": func(w *DispatchRejectionWrite) { w.RetryIntervalSeconds = -1 },
	} {
		t.Run("rejection/"+name, func(t *testing.T) {
			write := dispatchRejectionFixture()
			mutate(&write)
			if applied, err := repo.SaveDispatchRejection(context.Background(), write); applied || !errors.Is(err, ErrInvalidDispatchWrite) {
				t.Fatalf("invalid rejection: applied=%v err=%v", applied, err)
			}
		})
	}
}

func TestTaskDispatchRepositoryConfirmsIndependentCommit(t *testing.T) {
	broken := errors.New("synthetic commit confirmation failure")
	for _, busy := range []bool{true, false} {
		for _, commitErr := range []error{nil, broken, &pgconn.PgError{Code: "40003", Message: "statement completion unknown"},
			&pgconn.PgError{Code: "08007", Message: "transaction resolution unknown"}} {
			conn := &resultDriverConn{commitErr: commitErr}
			repo, err := NewJobTaskRepository(resultDriverDatabase(t, conn))
			if err != nil {
				t.Fatal(err)
			}
			applied, err := saveDispatchFixture(context.Background(), repo, busy, 1, 2)
			if commitErr == nil {
				if !applied || err != nil {
					t.Fatalf("committed response: busy=%v applied=%v err=%v", busy, applied, err)
				}
			} else if applied || !errors.Is(err, commitErr) || errors.Is(err, ErrDispatchWriteRejected) || errors.Is(err, ErrInvalidDispatchWrite) {
				t.Fatalf("unconfirmed response: busy=%v applied=%v err=%v", busy, applied, err)
			}
			if conn.execs != 1 || conn.queries != 0 || conn.commits != 1 {
				t.Fatalf("response performed extra work: execs=%d queries=%d commits=%d", conn.execs, conn.queries, conn.commits)
			}
		}
	}
}

func TestTaskDispatchRepositoryRejectsOuterTransaction(t *testing.T) {
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
	for _, busy := range []bool{true, false} {
		if applied, err := saveDispatchFixture(context.Background(), repo, busy, 1, 2); applied || !errors.Is(err, ErrInvalidDispatchWrite) || conn.execs != 0 {
			t.Fatalf("outer transaction accepted: busy=%v applied=%v err=%v execs=%d", busy, applied, err, conn.execs)
		}
	}
}

func TestTaskDispatchRepositoryClassifiesDefiniteRejectionsWithoutLosingCause(t *testing.T) {
	for _, scenario := range []struct {
		code     string
		rejected bool
	}{
		{"22003", true}, {"22P05", true}, {"23502", true}, {"23503", true}, {"23505", true}, {"23514", true},
		{"42601", true}, {"42501", true}, {"42P01", true},
		{"08006", false}, {"08007", false}, {"40001", false}, {"40003", false}, {"40P01", false},
		{"53100", false}, {"53300", false}, {"55P03", false}, {"57014", false}, {"57P01", false}, {"XX000", false},
		{"", false}, {"23", false},
	} {
		t.Run(scenario.code, func(t *testing.T) {
			cause := &pgconn.PgError{Code: scenario.code, Message: "synthetic diagnostic"}
			err := dispatchWriteError("save response", fmt.Errorf("driver wrapper: %w", cause))
			var preserved *pgconn.PgError
			if errors.Is(err, ErrDispatchWriteRejected) != scenario.rejected || errors.Is(err, ErrInvalidDispatchWrite) ||
				!errors.Is(err, cause) || !errors.As(err, &preserved) || preserved != cause {
				t.Fatalf("incorrect rejection classification for SQLSTATE %s: %v", scenario.code, err)
			}
		})
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, errors.New("connection failure"), errors.New("SQLSTATE 23505")} {
		err := dispatchWriteError("save response", cause)
		if !errors.Is(err, cause) || errors.Is(err, ErrDispatchWriteRejected) || errors.Is(err, ErrInvalidDispatchWrite) {
			t.Fatalf("unconfirmed failure became a definite rejection: %v", err)
		}
	}
}

func saveDispatchFixture(ctx context.Context, repo JobTaskRepository, busy bool, taskID, instanceID int64) (bool, error) {
	if busy {
		write := dispatchBusyFixture()
		write.TaskID, write.InstanceID = taskID, instanceID
		return repo.SaveDispatchBusy(ctx, write)
	}
	write := dispatchRejectionFixture()
	write.TaskID, write.InstanceID = taskID, instanceID
	return repo.SaveDispatchRejection(ctx, write)
}
