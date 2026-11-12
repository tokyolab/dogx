package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	pgdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func resultWriteFixture() TaskResultWrite {
	return TaskResultWrite{TaskID: 1, InstanceID: 2, AttemptNo: 1,
		ExecutorInstanceID: uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		Status:             model.TaskSucceeded, Data: model.TaskData(`null`), Summary: "done"}
}

func TestTaskResultRepositoryRejectsInvalidInputs(t *testing.T) {
	repo, err := NewJobTaskRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{0, -1} {
		if target, err := repo.FindResultTarget(context.Background(), id); target != nil || err == nil {
			t.Fatalf("invalid lookup: target=%+v err=%v", target, err)
		}
	}
	for name, mutate := range map[string]func(*TaskResultWrite){
		"task":           func(w *TaskResultWrite) { w.TaskID = 0 },
		"instance":       func(w *TaskResultWrite) { w.InstanceID = -1 },
		"attempt":        func(w *TaskResultWrite) { w.AttemptNo = 0 },
		"nil identity":   func(w *TaskResultWrite) { w.ExecutorInstanceID = uuid.Nil },
		"UUID version":   func(w *TaskResultWrite) { w.ExecutorInstanceID[6] = 0x50 },
		"UUID variant":   func(w *TaskResultWrite) { w.ExecutorInstanceID[8] = 0 },
		"running":        func(w *TaskResultWrite) { w.Status = model.TaskRunning },
		"canceled":       func(w *TaskResultWrite) { w.Status = model.TaskCanceled },
		"unknown status": func(w *TaskResultWrite) { w.Status = model.TaskStatus(99) },
		"retry interval": func(w *TaskResultWrite) { w.Status = model.TaskRetryWaiting; w.RetryIntervalSeconds = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			write := resultWriteFixture()
			mutate(&write)
			if applied, err := repo.SaveResult(context.Background(), write); applied || err == nil {
				t.Fatalf("invalid write: applied=%v err=%v", applied, err)
			}
		})
	}
	if valid, err := repo.CheckResultData(context.Background(), nil); valid || err == nil {
		t.Fatalf("absent data became probe evidence: valid=%v err=%v", valid, err)
	}
}

func TestTaskResultRepositoryConfirmsIndependentCommit(t *testing.T) {
	broken := errors.New("synthetic commit confirmation failure")
	for _, commitErr := range []error{nil, broken} {
		conn := &resultDriverConn{commitErr: commitErr}
		db := resultDriverDatabase(t, conn)
		repo, err := NewJobTaskRepository(db)
		if err != nil {
			t.Fatal(err)
		}
		applied, err := repo.SaveResult(context.Background(), resultWriteFixture())
		if commitErr == nil {
			if !applied || err != nil {
				t.Fatalf("committed result: applied=%v err=%v", applied, err)
			}
		} else if applied || !errors.Is(err, broken) {
			t.Fatalf("unconfirmed result: applied=%v err=%v", applied, err)
		}
		if conn.execs != 1 || conn.queries != 0 || conn.commits != 1 {
			t.Fatalf("write performed extra work: execs=%d queries=%d commits=%d", conn.execs, conn.queries, conn.commits)
		}
	}
}

func TestTaskResultRepositoryRejectsOuterTransaction(t *testing.T) {
	conn := &resultDriverConn{}
	db := resultDriverDatabase(t, conn)
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	repo, err := NewJobTaskRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := repo.SaveResult(context.Background(), resultWriteFixture()); applied || err == nil || conn.execs != 0 {
		t.Fatalf("outer transaction accepted: applied=%v err=%v execs=%d", applied, err, conn.execs)
	}
}

func TestTaskResultDataProbeRequiresDefiniteAnswer(t *testing.T) {
	broken := errors.New("synthetic query failure")
	for _, scenario := range []struct {
		name string
		rows [][]driver.Value
		err  error
		want bool
		fail bool
	}{
		{"valid", [][]driver.Value{{true}}, nil, true, false},
		{"invalid", [][]driver.Value{{false}}, nil, false, false},
		{"null", [][]driver.Value{{nil}}, nil, false, true},
		{"missing", nil, nil, false, true},
		{"query failure", nil, broken, false, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			conn := &resultDriverConn{rows: scenario.rows, queryErr: scenario.err}
			repo, err := NewJobTaskRepository(resultDriverDatabase(t, conn))
			if err != nil {
				t.Fatal(err)
			}
			valid, err := repo.CheckResultData(context.Background(), model.TaskData(`"\u0000"`))
			if valid != scenario.want || (err != nil) != scenario.fail || (scenario.err != nil && !errors.Is(err, scenario.err)) {
				t.Fatalf("probe: valid=%v err=%v", valid, err)
			}
			if conn.queries != 1 || conn.execs != 0 || strings.Contains(conn.lastQuery, "error_info") {
				t.Fatalf("probe performed unexpected work: queries=%d execs=%d", conn.queries, conn.execs)
			}
		})
	}
}

func resultDriverDatabase(t *testing.T, conn *resultDriverConn) *gorm.DB {
	t.Helper()
	sqlDB := sql.OpenDB(resultDriverConnector{conn: conn})
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

// resultDriverConn makes commit acknowledgement independent of affected rows.
// It also supplies missing or NULL probe answers that a real scalar query does
// not normally produce, so neither can masquerade as invalid-content evidence.
type resultDriverConn struct {
	commitErr, queryErr error
	rows                [][]driver.Value
	execs, queries      int
	commits             int
	lastQuery           string
}

func (*resultDriverConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepared statement")
}
func (*resultDriverConn) Close() error                { return nil }
func (c *resultDriverConn) Begin() (driver.Tx, error) { return resultDriverTx{conn: c}, nil }
func (c *resultDriverConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.execs++
	return driver.RowsAffected(1), nil
}
func (c *resultDriverConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.queries++
	c.lastQuery = query
	return &resultDriverRows{rows: c.rows}, c.queryErr
}

type resultDriverConnector struct{ conn *resultDriverConn }

func (c resultDriverConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c resultDriverConnector) Driver() driver.Driver                        { return resultDriver{conn: c.conn} }

type resultDriver struct{ conn *resultDriverConn }

func (d resultDriver) Open(string) (driver.Conn, error) { return d.conn, nil }

type resultDriverTx struct{ conn *resultDriverConn }

func (t resultDriverTx) Commit() error { t.conn.commits++; return t.conn.commitErr }
func (resultDriverTx) Rollback() error { return nil }

type resultDriverRows struct{ rows [][]driver.Value }

func (*resultDriverRows) Columns() []string { return []string{"valid"} }
func (*resultDriverRows) Close() error      { return nil }
func (r *resultDriverRows) Next(values []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(values, r.rows[0])
	r.rows = r.rows[1:]
	return nil
}
