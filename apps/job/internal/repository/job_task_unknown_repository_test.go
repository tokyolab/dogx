package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	pgdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func unknownSettlementFixture() UnknownSettlement {
	now := time.Date(2026, 11, 5, 12, 0, 0, 123456000, time.UTC)
	return UnknownSettlement{TaskID: 1, InstanceID: 2, ExpectedAttemptNo: 7, MaxAttempts: 4,
		ProcessedAt: now, ObservationCutoff: now.Add(-30 * time.Second), RetryAt: now.Add(10 * time.Second)}
}

func TestUnknownRepositoryRejectsInvalidInputsBeforeDatabaseWork(t *testing.T) {
	repo, err := NewJobTaskRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]int64{{0}, {-1}, {1, 0}, make([]int64, 17)} {
		if rows, err := repo.FindUnboundTargets(context.Background(), ids); rows != nil || err == nil {
			t.Fatalf("invalid target IDs returned rows: len=%d err=%v", len(rows), err)
		}
	}
	for _, ids := range [][]int64{nil, {}} {
		if rows, err := repo.FindUnboundTargets(context.Background(), ids); rows == nil || len(rows) != 0 || err != nil {
			t.Fatalf("empty target input: rows=%v err=%v", rows, err)
		}
	}
	for name, mutate := range map[string]func(*UnknownSettlement){
		"task identity":    func(w *UnknownSettlement) { w.TaskID = 0 },
		"parent identity":  func(w *UnknownSettlement) { w.InstanceID = -1 },
		"attempt identity": func(w *UnknownSettlement) { w.ExpectedAttemptNo = 0 },
		"execution budget": func(w *UnknownSettlement) { w.MaxAttempts = 0 },
		"missing time":     func(w *UnknownSettlement) { w.ProcessedAt = time.Time{} },
		"missing cutoff":   func(w *UnknownSettlement) { w.ObservationCutoff = time.Time{} },
		"missing retry":    func(w *UnknownSettlement) { w.RetryAt = time.Time{} },
		"zero observation": func(w *UnknownSettlement) { w.ObservationCutoff = w.ProcessedAt },
		"future cutoff":    func(w *UnknownSettlement) { w.ObservationCutoff = w.ProcessedAt.Add(time.Second) },
		"zero retry delay": func(w *UnknownSettlement) { w.RetryAt = w.ProcessedAt },
		"earlier retry":    func(w *UnknownSettlement) { w.RetryAt = w.ProcessedAt.Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			write := unknownSettlementFixture()
			mutate(&write)
			if settled, err := repo.SettleUnknown(context.Background(), write); settled != nil || err == nil {
				t.Fatalf("invalid settlement: settled=%+v err=%v", settled, err)
			}
		})
	}
}

func TestUnknownRepositoryRequiresConfirmedIndependentCommit(t *testing.T) {
	failure := errors.New("synthetic unknown-result commit failure")
	for _, skipDefault := range []bool{false, true} {
		for _, state := range []model.TaskStatus{0, model.TaskRetryWaiting, model.TaskFailed} {
			for _, commitErr := range []error{nil, failure} {
				t.Run(fmt.Sprintf("skip=%t/state=%d/commit_error=%t", skipDefault, state, commitErr != nil), func(t *testing.T) {
					conn := &unknownDriverConn{commitErr: commitErr}
					if state != 0 {
						conn.rows = [][]driver.Value{{int64(1), int64(2), int64(7), int64(state)}}
					}
					db := unknownDriverDatabase(t, conn).Session(&gorm.Session{SkipDefaultTransaction: skipDefault})
					repo, err := NewJobTaskRepository(db)
					if err != nil {
						t.Fatal(err)
					}
					settled, err := repo.SettleUnknown(context.Background(), unknownSettlementFixture())
					if !errors.Is(err, commitErr) {
						t.Fatalf("settlement lost commit outcome: settled=%+v err=%v", settled, err)
					}
					if commitErr != nil || state == 0 {
						if settled != nil {
							t.Fatalf("unconfirmed or zero-row write returned settlement: %+v", settled)
						}
					} else if settled == nil || *settled != (SettledTask{TaskID: 1, InstanceID: 2, AttemptNo: 7, Status: state}) {
						t.Fatalf("confirmed settlement lost its returned facts: %+v", settled)
					}
					if conn.begins != 1 || conn.commits != 1 || conn.queries != 1 || !strings.HasPrefix(conn.lastQuery, "UPDATE ") || !strings.Contains(conn.lastQuery, "RETURNING") {
						t.Fatalf("settlement lost its explicit transaction or added a reread: begins=%d commits=%d queries=%d query=%s", conn.begins, conn.commits, conn.queries, conn.lastQuery)
					}
				})
			}
		}
	}
}

func TestUnknownRepositoryPreservesWriteErrorsAndRejectsInvalidReturningFacts(t *testing.T) {
	failure := errors.New("synthetic unknown-result database failure")
	for _, scenario := range []struct {
		name     string
		beginErr error
		queryErr error
		rows     [][]driver.Value
		want     error
	}{
		{name: "begin failure", beginErr: failure, want: failure},
		{name: "write failure", queryErr: failure, want: failure},
		{name: "wrong task", rows: [][]driver.Value{{int64(9), int64(2), int64(7), int64(3)}}},
		{name: "wrong parent", rows: [][]driver.Value{{int64(1), int64(9), int64(7), int64(3)}}},
		{name: "wrong attempt", rows: [][]driver.Value{{int64(1), int64(2), int64(9), int64(3)}}},
		{name: "wrong status", rows: [][]driver.Value{{int64(1), int64(2), int64(7), int64(4)}}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			conn := &unknownDriverConn{beginErr: scenario.beginErr, queryErr: scenario.queryErr, rows: scenario.rows}
			repo, err := NewJobTaskRepository(unknownDriverDatabase(t, conn))
			if err != nil {
				t.Fatal(err)
			}
			if settled, err := repo.SettleUnknown(context.Background(), unknownSettlementFixture()); settled != nil || err == nil || (scenario.want != nil && !errors.Is(err, scenario.want)) {
				t.Fatalf("invalid settlement escaped: settled=%+v err=%v", settled, err)
			}
			if conn.commits != 0 || (scenario.beginErr == nil && conn.rollbacks != 1) {
				t.Fatalf("failed settlement committed or failed to roll back: commits=%d rollbacks=%d", conn.commits, conn.rollbacks)
			}
		})
	}
}

func TestUnknownRepositoryRejectsOuterTransactionAndPreservesReadFailure(t *testing.T) {
	conn := &unknownDriverConn{}
	db := unknownDriverDatabase(t, conn)
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	repo, err := NewJobTaskRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	if settled, err := repo.SettleUnknown(context.Background(), unknownSettlementFixture()); settled != nil || err == nil || conn.queries != 0 || conn.commits != 0 {
		t.Fatalf("outer transaction produced a confirmed settlement: settled=%+v err=%v queries=%d commits=%d", settled, err, conn.queries, conn.commits)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	conn.queryErr = errors.New("synthetic unbound target read failure")
	repo, err = NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := repo.FindUnboundTargets(context.Background(), []int64{1, 2}); rows != nil || !errors.Is(err, conn.queryErr) || conn.queries != 1 {
		t.Fatalf("target read failure became an empty batch: rows=%v err=%v queries=%d", rows, err, conn.queries)
	}
}

func unknownDriverDatabase(t *testing.T, conn *unknownDriverConn) *gorm.DB {
	t.Helper()
	sqlDB := sql.OpenDB(unknownDriverConnector{conn: conn})
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(pgdriver.New(pgdriver.Config{Conn: sqlDB}), &gorm.Config{
		DisableAutomaticPing: true, Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// unknownDriverConn separates UPDATE RETURNING from transaction confirmation.
// Its failures model the caller boundary, not an actual PostgreSQL disconnect.
type unknownDriverConn struct {
	beginErr, queryErr, commitErr error
	rows                          [][]driver.Value
	begins, queries, commits      int
	rollbacks                     int
	lastQuery                     string
}

func (*unknownDriverConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepared statement")
}
func (*unknownDriverConn) Close() error { return nil }
func (c *unknownDriverConn) Begin() (driver.Tx, error) {
	c.begins++
	if c.beginErr != nil {
		return nil, c.beginErr
	}
	return unknownDriverTx{conn: c}, nil
}
func (c *unknownDriverConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.queries++
	c.lastQuery = query
	return &unknownDriverRows{rows: append([][]driver.Value(nil), c.rows...)}, c.queryErr
}

type unknownDriverConnector struct{ conn *unknownDriverConn }

func (c unknownDriverConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c unknownDriverConnector) Driver() driver.Driver                        { return unknownDriver{conn: c.conn} }

type unknownDriver struct{ conn *unknownDriverConn }

func (d unknownDriver) Open(string) (driver.Conn, error) { return d.conn, nil }

type unknownDriverTx struct{ conn *unknownDriverConn }

func (t unknownDriverTx) Commit() error   { t.conn.commits++; return t.conn.commitErr }
func (t unknownDriverTx) Rollback() error { t.conn.rollbacks++; return nil }

type unknownDriverRows struct{ rows [][]driver.Value }

func (*unknownDriverRows) Columns() []string {
	return []string{"id", "instance_id", "attempt_no", "status"}
}
func (*unknownDriverRows) Close() error { return nil }
func (r *unknownDriverRows) Next(values []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(values, r.rows[0])
	r.rows = r.rows[1:]
	return nil
}
