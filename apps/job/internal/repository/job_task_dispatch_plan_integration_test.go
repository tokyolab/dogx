//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	dispatchPlanHistoryParents = 3000
	dispatchPlanHistoryTasks   = 90000
	dispatchPlanDueTasks       = 12288
	dispatchPlanFutureTasks    = 96
)

func TestDispatchCandidatePlansAcrossHistoryAndParentFiltering(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		sparse bool
	}{
		{"terminal_history_with_live_backlog", false},
		{"mostly_stopped_or_expired_parents", true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			db, _ := testutil.OpenMigratedPostgres(t)
			now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
			expected := seedDispatchPlanDistribution(t, db, now, scenario.sparse)
			var indexDefinition string
			if err := db.Raw("SELECT pg_get_indexdef('idx_job_task_dispatch_due'::regclass)").Scan(&indexDefinition).Error; err != nil {
				t.Fatal(err)
			}
			compact := strings.NewReplacer(" ", "", "(", "", ")", "").Replace(indexDefinition)
			if !strings.Contains(compact, "USINGbtreenext_dispatch_at,idWHEREstatus=ANYARRAY[1,3]") {
				t.Fatalf("dispatch access path lost its ordered keys or partial predicate: %s", indexDefinition)
			}
			t.Logf("dispatch_candidate_distribution history_tasks=%d due_tasks=%d eligible_tasks=%d future_tasks=%d sparse_parents=%t index=%s",
				dispatchPlanHistoryTasks, dispatchPlanDueTasks, len(expected), dispatchPlanFutureTasks, scenario.sparse, indexDefinition)
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			capture := &executionQueryCapture{queryCounter: counter}
			repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: capture}))
			if err != nil {
				t.Fatal(err)
			}
			boundary := 0
			for boundary < len(expected) && expected[boundary].NextDispatchAt.Before(now) {
				boundary++
			}
			if boundary < 128 || len(expected)-boundary < 128 {
				t.Fatal("plan fixture must contain complete pages on both sides of the due-time boundary")
			}
			// The second page starts within a timestamp tie. A separate boundary
			// page also crosses into tasks exactly due at the fixed scan time.
			offsets := []int{0, 128}
			if boundary-64 != 128 {
				offsets = append(offsets, boundary-64)
			}
			offsets = append(offsets, len(expected))
			for _, offset := range offsets {
				t.Run(fmt.Sprintf("offset_%d", offset), func(t *testing.T) {
					var cursor *DispatchCursor
					if offset > 0 {
						previous := expected[offset-1]
						cursor = &DispatchCursor{NextDispatchAt: previous.NextDispatchAt, TaskID: previous.TaskID}
					}
					capture.reset()
					page, err := repo.ListDispatchCandidates(context.Background(), now, cursor, 128)
					want := expected[offset:min(offset+128, len(expected))]
					assertDispatchPlanPage(t, page, err, want)
					query := capturedDispatchPlanQuery(t, capture, cursor != nil)
					explainDispatchCandidatePlan(t, db, query, len(page))
				})
			}
			if scenario.sparse {
				t.Run("no_live_parents", func(t *testing.T) {
					// A valid LIMIT can still return zero after examining many due
					// Tasks. Keep their index entries and remove only parent eligibility.
					if err := db.Exec(`UPDATE job_instance SET status = 6, end_reason = 'manual_stop'
						WHERE id > ? AND status = 2 AND deadline_at > ?`, dispatchPlanHistoryParents, now).Error; err != nil {
						t.Fatal(err)
					}
					if err := db.Exec("ANALYZE job_instance").Error; err != nil {
						t.Fatal(err)
					}
					capture.reset()
					page, err := repo.ListDispatchCandidates(context.Background(), now, nil, 128)
					assertDispatchPlanPage(t, page, err, nil)
					query := capturedDispatchPlanQuery(t, capture, false)
					explainDispatchCandidatePlan(t, db, query, 0)
				})
			}
		})
	}
}

func seedDispatchPlanDistribution(t *testing.T, db *gorm.DB, now time.Time, sparse bool) []DispatchCandidate {
	t.Helper()
	// Explicit fixture IDs make expected pages independent of a second copy of
	// the production query. History has 30 completed shards per completed parent.
	if err := db.Exec(`INSERT INTO job_instance
		(id, job_id, job_name, executor_group_id, trigger_type, scheduled_at, params_overridden,
		 execution_config, status, started_at, deadline_at, finished_at)
		SELECT n, n, 'dispatch plan history', 1, 1, ?::timestamptz, FALSE,
		jsonb_build_object('schema_version', 1, 'execution_mode', 2, 'params', repeat('h', 256)),
		3, ?::timestamptz - INTERVAL '2 hours', ?::timestamptz - INTERVAL '1 hour', ?::timestamptz - INTERVAL '1 hour'
		FROM generate_series(1, ?) n`, now, now, now, now, dispatchPlanHistoryParents).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO job_task
		(id, instance_id, task_type, status, attempt_no, used_attempts, input_data, result_data)
		SELECT n, (n - 1) % ? + 1, 3, 4, 1, 1,
		jsonb_build_object('item', n), to_jsonb(repeat('h', 128)) FROM generate_series(1, ?) n`,
		dispatchPlanHistoryParents, dispatchPlanHistoryTasks).Error; err != nil {
		t.Fatal(err)
	}
	// Each cohort contains first shard/Post and ordinary/Pre/shard/Post retries.
	// In the sparse distribution only one cohort in 32 has eligible parents;
	// the rest include STOPPING, terminal, exact-deadline and expired parents.
	if err := db.Exec(`INSERT INTO job_instance
		(id, job_id, job_name, executor_group_id, trigger_type, scheduled_at, params_overridden,
		 execution_config, status, started_at, deadline_at)
		SELECT ? + n, ? + n, 'dispatch plan backlog', 1, 1, ?::timestamptz, FALSE,
		jsonb_build_object('schema_version', 1, 'execution_mode', CASE WHEN n % 6 = 3 THEN 1 ELSE 2 END,
		'processor_name', 'test.dispatch', 'params', repeat('b', 256), 'post_enabled', n % 6 IN (0, 2),
		'max_retries', 2, 'retry_interval_seconds', 5, 'timeout_seconds', 900),
		CASE WHEN NOT ?::boolean OR ((n - 1) / 6) % 32 = 0 THEN 2
		WHEN ((n - 1) / 6) % 4 = 0 THEN 6 WHEN ((n - 1) / 6) % 4 = 1 THEN 3 ELSE 2 END,
		?::timestamptz - INTERVAL '1 minute',
		CASE WHEN NOT ?::boolean OR ((n - 1) / 6) % 32 = 0 THEN ?::timestamptz + INTERVAL '1 hour'
		WHEN ((n - 1) / 6) % 4 = 2 THEN ?::timestamptz ELSE ?::timestamptz - INTERVAL '1 second' END
		FROM generate_series(1, ?) n`, dispatchPlanHistoryParents, dispatchPlanHistoryParents, now, sparse,
		now, sparse, now, now, now, dispatchPlanDueTasks+dispatchPlanFutureTasks).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO job_task
		(id, instance_id, task_type, status, attempt_no, used_attempts, started_at, attempt_registered_at, next_dispatch_at, input_data)
		SELECT ? + n, ? + n,
		CASE n % 6 WHEN 1 THEN 3 WHEN 2 THEN 4 WHEN 3 THEN 1 WHEN 4 THEN 2 WHEN 5 THEN 3 ELSE 4 END,
		CASE WHEN n % 6 IN (1, 2) THEN 1 ELSE 3 END,
		CASE WHEN n % 6 IN (1, 2) THEN 0 ELSE 2 END,
		CASE WHEN n % 6 IN (1, 2) THEN 0 ELSE 1 END,
		CASE WHEN n % 6 IN (1, 2) THEN NULL ELSE ?::timestamptz - INTERVAL '1 minute' END,
		CASE WHEN n % 6 IN (1, 2) THEN NULL ELSE ?::timestamptz - INTERVAL '30 seconds' END,
		CASE WHEN n > ? THEN ?::timestamptz + INTERVAL '1 second'
		WHEN n <= ? THEN ?::timestamptz - INTERVAL '1 minute' ELSE ?::timestamptz END,
		CASE WHEN n % 6 IN (2, 0) THEN jsonb_build_object('total', 2, 'succeeded', 1, 'failed', 1)
		WHEN n % 6 IN (1, 5) THEN jsonb_build_object('item', n) ELSE NULL END
		FROM generate_series(1, ?) n`, dispatchPlanHistoryTasks, dispatchPlanHistoryParents, now, now,
		dispatchPlanDueTasks, now, dispatchPlanDueTasks/2, now, now, dispatchPlanDueTasks+dispatchPlanFutureTasks).Error; err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"job_instance", "job_task"} {
		if err := db.Exec("ANALYZE " + table).Error; err != nil {
			t.Fatal(err)
		}
	}
	stages := []model.TaskType{model.TaskTypeShard, model.TaskTypePost, model.TaskTypeOrdinary, model.TaskTypePrepare, model.TaskTypeShard, model.TaskTypePost}
	var expected []DispatchCandidate
	for n := 1; n <= dispatchPlanDueTasks; n++ {
		if sparse && ((n-1)/6)%32 != 0 {
			continue
		}
		state, attempt := model.TaskRetryWaiting, int64(2)
		if n%6 == 1 || n%6 == 2 {
			state, attempt = model.TaskWaiting, 0
		}
		due := now
		if n <= dispatchPlanDueTasks/2 {
			due = now.Add(-time.Minute)
		}
		expected = append(expected, DispatchCandidate{TaskID: int64(dispatchPlanHistoryTasks + n),
			InstanceID: int64(dispatchPlanHistoryParents + n), TaskType: stages[(n-1)%6], Status: state,
			AttemptNo: attempt, NextDispatchAt: due})
	}
	return expected
}

func assertDispatchPlanPage(t *testing.T, page []DispatchCandidate, err error, expected []DispatchCandidate) {
	t.Helper()
	if err != nil || page == nil || len(page) != len(expected) {
		t.Fatalf("dispatch plan page: actual=%d expected=%d err=%v", len(page), len(expected), err)
	}
	for index, got := range page {
		want := expected[index]
		if got.TaskID != want.TaskID || got.InstanceID != want.InstanceID || got.TaskType != want.TaskType ||
			got.Status != want.Status || got.AttemptNo != want.AttemptNo || !got.NextDispatchAt.Equal(want.NextDispatchAt) {
			t.Fatalf("dispatch plan candidate %d: actual=%+v expected=%+v", index, got, want)
		}
	}
}

func capturedDispatchPlanQuery(t *testing.T, capture *executionQueryCapture, continuation bool) executionQuery {
	t.Helper()
	if capture.count != 1 || len(capture.queries) != 1 {
		t.Fatalf("dispatch candidate read was not one captured statement: count=%d captured=%d", capture.count, len(capture.queries))
	}
	query := capture.queries[0]
	if query.sql == "" || len(query.vars) == 0 {
		t.Fatal("dispatch query lost its original SQL or parameters")
	}
	for _, required := range []string{
		"t.id AS task_id, t.instance_id, t.task_type, t.status, t.attempt_no, t.next_dispatch_at",
		"t.status IN (1, 3)", "t.status = 3 OR (t.status = 1 AND t.task_type IN (3, 4)",
		"t.attempt_no = 0 AND t.used_attempts = 0", "t.started_at IS NULL AND t.attempt_registered_at IS NULL",
		"t.next_dispatch_at <=", "i.status = 2 AND i.deadline_at >", "ORDER BY t.next_dispatch_at, t.id",
	} {
		if !strings.Contains(query.sql, required) {
			t.Fatalf("dispatch candidate query lost %q: %s", required, query.sql)
		}
	}
	for _, forbidden := range []string{"input_data", "result_data", "result_summary", "execution_config", "job_definition", "SELECT *", "OFFSET", "FOR UPDATE"} {
		if strings.Contains(query.sql, forbidden) {
			t.Fatalf("dispatch candidate query added %q: %s", forbidden, query.sql)
		}
	}
	if strings.Contains(query.sql, "(t.next_dispatch_at, t.id) >") != continuation {
		t.Fatalf("dispatch cursor differs from the requested page: continuation=%t sql=%s", continuation, query.sql)
	}
	return query
}

type dispatchPlanNode struct {
	NodeType            string             `json:"Node Type"`
	RelationName        string             `json:"Relation Name"`
	IndexName           string             `json:"Index Name"`
	ActualRows          float64            `json:"Actual Rows"`
	ActualLoops         float64            `json:"Actual Loops"`
	RowsRemovedByFilter float64            `json:"Rows Removed by Filter"`
	RowsRemovedByJoin   float64            `json:"Rows Removed by Join Filter"`
	SharedHitBlocks     int64              `json:"Shared Hit Blocks"`
	SharedReadBlocks    int64              `json:"Shared Read Blocks"`
	TempReadBlocks      int64              `json:"Temp Read Blocks"`
	TempWrittenBlocks   int64              `json:"Temp Written Blocks"`
	Plans               []dispatchPlanNode `json:"Plans"`
}

func explainDispatchCandidatePlan(t *testing.T, db *gorm.DB, query executionQuery, expectedRows int) {
	t.Helper()
	// Explain exactly the repository SQL and bindings. The preceding repository
	// read warms this workload; these measurements are not cold-cache results or
	// latency thresholds. No planner switch or index hint selects an access path.
	var data []byte
	if err := db.Statement.ConnPool.QueryRowContext(context.Background(),
		"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query.sql, query.vars...).Scan(&data); err != nil {
		t.Fatal(err)
	}
	var documents []struct {
		Plan          dispatchPlanNode `json:"Plan"`
		PlanningTime  float64          `json:"Planning Time"`
		ExecutionTime float64          `json:"Execution Time"`
	}
	if err := json.Unmarshal(data, &documents); err != nil || len(documents) != 1 {
		t.Fatalf("invalid dispatch EXPLAIN document: err=%v plan=%s", err, data)
	}
	document := documents[0]
	if document.Plan.NodeType != "Limit" || document.Plan.ActualRows != float64(expectedRows) || document.Plan.ActualLoops != 1 {
		t.Fatalf("EXPLAIN did not execute the same bounded page: expected_rows=%d plan=%s", expectedRows, data)
	}
	var dueIndex, sequentialScan, sorted bool
	var visit func(dispatchPlanNode)
	visit = func(node dispatchPlanNode) {
		dueIndex = dueIndex || node.IndexName == "idx_job_task_dispatch_due"
		sequentialScan = sequentialScan || node.NodeType == "Seq Scan"
		sorted = sorted || strings.Contains(node.NodeType, "Sort")
		if node.RelationName != "" || node.RowsRemovedByJoin > 0 {
			// PostgreSQL reports rounded per-loop averages for filtered rows.
			// Log those raw values and loops, not an invented exact visit count.
			t.Logf("dispatch_candidate_plan_node relation=%s node=%q index=%s rows_per_loop=%.3f loops=%.0f filter_removed_per_loop=%.3f join_removed_per_loop=%.3f shared_hits=%d shared_reads=%d",
				node.RelationName, node.NodeType, node.IndexName, node.ActualRows, node.ActualLoops,
				node.RowsRemovedByFilter, node.RowsRemovedByJoin, node.SharedHitBlocks, node.SharedReadBlocks)
		}
		for _, child := range node.Plans {
			visit(child)
		}
	}
	visit(document.Plan)
	// Root buffer totals already include children; summing node totals would
	// double-count work. LIMIT bounds returned rows, not buffers or scanned tuples.
	t.Logf("dispatch_candidate_plan_summary returned=%d planning_ms=%.3f execution_ms=%.3f shared_hits=%d shared_reads=%d temp_reads=%d temp_writes=%d due_index=%t seq_scan=%t sort=%t cache_context=repository_read_before_explain",
		expectedRows, document.PlanningTime, document.ExecutionTime, document.Plan.SharedHitBlocks,
		document.Plan.SharedReadBlocks, document.Plan.TempReadBlocks, document.Plan.TempWrittenBlocks,
		dueIndex, sequentialScan, sorted)
	t.Logf("dispatch candidate execution plan:\n%s", data)
}
