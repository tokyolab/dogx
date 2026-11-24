//go:build integration && e2e && load

package main

import (
	"context"
	"encoding/json"
	"flag"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/config"
	"github.com/tokyolab/dogx/pkg/postgres"
	"github.com/tokyolab/dogx/pkg/rpchost"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestJobHistoryLoadQueryPlans is a read-only helper for the history acceptance
// workload. Its caller supplies an explicit isolated configuration and output
// file. It opens existing tables without starting servers, migrating, creating
// schemas or changing planner settings. Queries and EXPLAIN use the production
// repositories with their original bound arguments, in separate READ COMMITTED
// statements; concurrent execution may change rows between those statements.
func TestJobHistoryLoadQueryPlans(t *testing.T) {
	output := strings.TrimSpace(os.Getenv("DOGX_HISTORY_PLAN_OUTPUT"))
	var explicitConfig bool
	flag.Visit(func(value *flag.Flag) {
		if value.Name == "f" {
			explicitConfig = true
		}
	})
	if output == "" || !explicitConfig {
		t.Skip("history query plans require an explicit isolated configuration and output file")
	}
	if !filepath.IsAbs(output) {
		t.Fatal("history query plan output must be an absolute path")
	}
	var c config.Config
	if _, err := rpchost.LoadConfig(*configFile, &c); err != nil {
		t.Fatal("load isolated history query plan configuration")
	}
	host := net.ParseIP(c.Postgres.Host)
	if (host == nil || !host.IsLoopback()) && c.Postgres.Host != "localhost" {
		t.Fatal("history query plans require a loopback PostgreSQL host")
	}
	if !strings.HasSuffix(strings.ToLower(c.Postgres.Database), "_test") {
		t.Fatal("history query plans require a dedicated test database")
	}
	// This separate read-only observer needs one connection, independently of
	// the production processes' unchanged pools.
	c.Postgres.MaxOpenConns, c.Postgres.MaxIdleConns = 1, 1
	db, pool, err := postgres.Open(c.Postgres)
	if err != nil {
		t.Fatal("open isolated history query plan database")
	}
	t.Cleanup(func() {
		if err := pool.Close(); err != nil {
			t.Error("close history query plan database")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	report := jobHistoryQueryPlanReport{StartedAt: time.Now().UTC()}
	if err := pool.QueryRowContext(ctx, "SELECT current_schema(), clock_timestamp()").Scan(&report.Schema, &report.DatabaseTime); err != nil {
		t.Fatal("read history query plan schema and database clock")
	}
	if !strings.HasPrefix(report.Schema, "dogx_it_") {
		t.Fatal("history query plans require an existing integration test schema")
	}
	report.Cutoff = report.DatabaseTime.Add(-7 * 24 * time.Hour)
	if value := strings.TrimSpace(os.Getenv("DOGX_HISTORY_PLAN_CUTOFF")); value != "" {
		report.Cutoff, err = time.Parse(time.RFC3339Nano, value)
		if err != nil {
			t.Fatal("history query plan cutoff must use RFC3339Nano")
		}
		report.CutoffOverridden = true
	}
	t.Cleanup(func() {
		report.CompletedAt, report.Passed = time.Now().UTC(), !t.Failed()
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			t.Error("encode history query plan report")
			return
		}
		if err := os.MkdirAll(filepath.Dir(output), 0o700); err != nil {
			t.Error("create history query plan output directory")
			return
		}
		if err := os.WriteFile(output, append(data, '\n'), 0o600); err != nil {
			t.Error("write history query plan report")
		}
	})
	capture := &jobHistoryQueryCapture{Interface: logger.Default.LogMode(logger.Silent)}
	observed := db.Session(&gorm.Session{Logger: capture})
	instances, err := repository.NewJobInstanceRepository(observed)
	if err != nil {
		t.Fatal("construct history Instance query repository")
	}
	tasks, err := repository.NewJobTaskRepository(observed)
	if err != nil {
		t.Fatal("construct history Task query repository")
	}
	measure := func(name string, run func() (int, error)) {
		t.Helper()
		capture.queries = nil
		started := time.Now()
		rows, err := run()
		if err != nil {
			t.Fatalf("run history query plan repository method %s", name)
		}
		entry := jobHistoryQueryPlan{Name: name, ReturnedRows: rows, RepositoryElapsedMS: time.Since(started).Seconds() * 1000}
		if len(capture.queries) != 1 {
			t.Fatalf("history query %s captured %d statements, want one", name, len(capture.queries))
		}
		query := capture.queries[0]
		entry.SQL = query.sql
		if !strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query.sql)), "SELECT ") {
			t.Fatalf("history query %s is not a read-only SELECT", name)
		}
		entry.Parameters = historyPlanParameters(t, query.vars)
		planContext, stop := context.WithTimeout(ctx, 10*time.Second)
		var plan []byte
		err = pool.QueryRowContext(planContext, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query.sql, query.vars...).Scan(&plan)
		stop()
		if err != nil {
			t.Fatalf("explain history query %s", name)
		}
		var documents []json.RawMessage
		if err := json.Unmarshal(plan, &documents); err != nil || len(documents) != 1 {
			t.Fatalf("history query %s returned an invalid EXPLAIN document", name)
		}
		entry.Plan = plan
		report.Queries = append(report.Queries, entry)
	}
	measure("dispatch_first", func() (int, error) {
		items, err := tasks.ListDispatchCandidates(ctx, report.DatabaseTime, nil, 128)
		return len(items), err
	})
	measure("running_first", func() (int, error) {
		items, err := instances.ListRunningCandidates(ctx, report.DatabaseTime.Add(-4*time.Minute), nil, 256)
		return len(items), err
	})
	var stoppingUpper int64
	measure("stopping_upper_bound", func() (int, error) {
		var err error
		stoppingUpper, err = instances.StoppingUpperID(ctx)
		if stoppingUpper == 0 {
			return 0, err
		}
		return 1, err
	})
	if stoppingUpper == 0 {
		// The production scanner can skip an empty STOPPING range. An existing
		// positive table bound lets this probe also explain its empty-page SQL;
		// it does not assert that production scanned a backlog at this checkpoint.
		report.StoppingEmptyPageProbe = true
		if err := pool.QueryRowContext(ctx, "SELECT coalesce(max(id), 1) FROM job_instance").Scan(&stoppingUpper); err != nil {
			t.Fatal("read an existing upper bound for the empty STOPPING page probe")
		}
	}
	measure("stopping_first", func() (int, error) {
		items, err := instances.ListStoppingCandidates(ctx, 0, stoppingUpper, 256)
		return len(items), err
	})
	var first []repository.HistoryCandidate
	measure("history_first", func() (int, error) {
		var err error
		first, err = instances.ListExpiredHistory(ctx, report.Cutoff, nil, 100)
		return len(first), err
	})
	if len(first) != 0 {
		cursor := first[len(first)-1]
		measure("history_continuation", func() (int, error) {
			items, err := instances.ListExpiredHistory(ctx, report.Cutoff, &cursor, 100)
			return len(items), err
		})
	} else {
		report.HistoryContinuationSkipped = "the first page was empty at the supplied cutoff"
	}
}

type jobHistoryQueryPlanReport struct {
	StartedAt, CompletedAt, DatabaseTime, Cutoff time.Time
	Schema                                       string
	Passed, CutoffOverridden                     bool
	StoppingEmptyPageProbe                       bool
	HistoryContinuationSkipped                   string
	Queries                                      []jobHistoryQueryPlan
}

type jobHistoryQueryPlan struct {
	Name, SQL           string
	Parameters          []jobHistoryQueryParameter
	ReturnedRows        int
	RepositoryElapsedMS float64
	Plan                json.RawMessage
}

type jobHistoryQueryParameter struct {
	Type, Value string
}

type jobHistoryCapturedQuery struct {
	sql  string
	vars []any
}

// jobHistoryQueryCapture is confined to this serial observer. Calling the trace
// closure invokes GORM's parameter filter without emitting interpolated SQL.
type jobHistoryQueryCapture struct {
	logger.Interface
	queries []jobHistoryCapturedQuery
}

func (c *jobHistoryQueryCapture) Trace(_ context.Context, _ time.Time, query func() (string, int64), _ error) {
	_, _ = query()
}

func (c *jobHistoryQueryCapture) ParamsFilter(_ context.Context, statement string, vars ...any) (string, []any) {
	c.queries = append(c.queries, jobHistoryCapturedQuery{sql: statement, vars: append([]any(nil), vars...)})
	return statement, vars
}

// historyPlanParameters rejects unexpected binding types rather than exporting
// arbitrary repository data. Decimal strings preserve BIGINT precision for JSON
// readers, and timestamps retain their original timezone and fractional seconds.
func historyPlanParameters(t testing.TB, vars []any) []jobHistoryQueryParameter {
	t.Helper()
	result := make([]jobHistoryQueryParameter, 0, len(vars))
	for _, value := range vars {
		var parameter jobHistoryQueryParameter
		switch value := value.(type) {
		case int:
			parameter = jobHistoryQueryParameter{Type: "int", Value: strconv.Itoa(value)}
		case int32:
			parameter = jobHistoryQueryParameter{Type: "int32", Value: strconv.FormatInt(int64(value), 10)}
		case int64:
			parameter = jobHistoryQueryParameter{Type: "int64", Value: strconv.FormatInt(value, 10)}
		case time.Time:
			parameter = jobHistoryQueryParameter{Type: "time", Value: value.Format(time.RFC3339Nano)}
		default:
			t.Fatal("history query plan has a binding outside the numeric and timestamp allowlist")
		}
		result = append(result, parameter)
	}
	return result
}
