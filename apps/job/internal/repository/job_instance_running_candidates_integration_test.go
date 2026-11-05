//go:build integration

package repository

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestRunningCandidatesUseStrictAgeBoundaryAndMinimalProjection(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	bound := now.Add(-4 * time.Minute)
	older, same, recent := bound.Add(-time.Hour), bound.Add(-time.Microsecond), now.Add(-2*time.Minute)
	futureDeadline, expiredDeadline := now.Add(24*time.Hour), now.Add(-time.Second)
	long := insertRunningQueryInstance(t, db, job, model.InstanceRunning, &older, &futureDeadline)
	expired := insertRunningQueryInstance(t, db, job, model.InstanceRunning, &same, &expiredDeadline)
	noDeadline := insertRunningQueryInstance(t, db, job, model.InstanceRunning, &same, nil)
	insertRunningQueryInstance(t, db, job, model.InstanceRunning, &bound, &expiredDeadline)
	insertRunningQueryInstance(t, db, job, model.InstanceRunning, &recent, &expiredDeadline)
	insertRunningQueryInstance(t, db, job, model.InstanceRunning, nil, &expiredDeadline)
	for _, status := range []model.InstanceStatus{model.InstanceWaiting, model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled, model.InstanceStopping} {
		insertRunningQueryInstance(t, db, job, status, &older, &expiredDeadline)
	}
	// Candidate discovery must not decode snapshots, depend on the current Job,
	// or reinterpret audit fields as progress. Those facts belong to later checks.
	if err := db.Model(&model.JobInstance{}).Where("id = ?", noDeadline).
		Updates(map[string]any{"execution_config": gorm.Expr("?::jsonb", `"invalid snapshot shape"`),
			"created_at": now.Add(time.Hour), "updated_at": now.Add(time.Hour)}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(job).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobInstanceRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	first, err := repo.ListRunningCandidates(context.Background(), bound, nil, 2)
	assertRunningPage(t, first, err, []RunningCandidate{{ID: long, StartedAt: older}, {ID: expired, StartedAt: same}})
	assertRunningCandidateProjection(t, counter)
	counter.count, counter.statements = 0, nil
	cursor := first[len(first)-1]
	second, err := repo.ListRunningCandidates(context.Background(), bound, &cursor, 2)
	assertRunningPage(t, second, err, []RunningCandidate{{ID: noDeadline, StartedAt: same}})
	assertRunningCandidateProjection(t, counter)
	counter.count, counter.statements = 0, nil
	cursor = second[len(second)-1]
	last, err := repo.ListRunningCandidates(context.Background(), bound, &cursor, 2)
	assertRunningPage(t, last, err, nil)
	assertRunningCandidateProjection(t, counter)
}

func TestRunningCandidatesKeepFixedBoundAndRediscoverLateEarlierPositions(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	bound := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	earlier, later, newlyEligible := bound.Add(-time.Minute), bound.Add(-time.Second), bound.Add(10*time.Second)
	late := db.Begin()
	if late.Error != nil {
		t.Fatal(late.Error)
	}
	defer late.Rollback()
	// The lower ID is allocated on a different transaction but not yet visible.
	// A traversal that has already passed its composite position cannot recover
	// it until a fresh traversal starts; no cross-page snapshot is implied.
	low := insertRunningQueryInstance(t, late, job, model.InstanceRunning, &earlier, nil)
	high := insertRunningQueryInstance(t, db, job, model.InstanceRunning, &earlier, nil)
	future := insertRunningQueryInstance(t, db, job, model.InstanceRunning, &newlyEligible, nil)
	if low >= high {
		t.Fatal("test did not allocate the late commit before the visible row")
	}
	repo, err := NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	first, err := repo.ListRunningCandidates(context.Background(), bound, nil, 1)
	assertRunningPage(t, first, err, []RunningCandidate{{ID: high, StartedAt: earlier}})
	if err := late.Commit().Error; err != nil {
		t.Fatal(err)
	}
	visibleAfterCursor := insertRunningQueryInstance(t, db, job, model.InstanceRunning, &later, nil)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", high).Update("status", model.InstanceSucceeded).Error; err != nil {
		t.Fatal(err)
	}
	cursor := first[0]
	continued, err := repo.ListRunningCandidates(context.Background(), bound, &cursor, 256)
	assertRunningPage(t, continued, err, []RunningCandidate{{ID: visibleAfterCursor, StartedAt: later}})
	restarted, err := repo.ListRunningCandidates(context.Background(), bound, nil, 256)
	assertRunningPage(t, restarted, err, []RunningCandidate{{ID: low, StartedAt: earlier}, {ID: visibleAfterCursor, StartedAt: later}})
	// A later traversal can use a fresh database-derived boundary, admitting the
	// young row that remained outside every page of the original fixed bound.
	fresh, err := repo.ListRunningCandidates(context.Background(), bound.Add(time.Minute), nil, 256)
	assertRunningPage(t, fresh, err, []RunningCandidate{
		{ID: low, StartedAt: earlier}, {ID: visibleAfterCursor, StartedAt: later}, {ID: future, StartedAt: newlyEligible},
	})
}

func TestRunningCandidateQueryErrorsDoNotBecomeEmptySuccess(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	repo, err := NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if items, err := repo.ListRunningCandidates(ctx, time.Now(), nil, 256); items != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read lost its cause: items=%v error=%v", items, err)
	}
	if err := db.Exec("DROP TABLE job_instance").Error; err != nil {
		t.Fatal(err)
	}
	items, err := repo.ListRunningCandidates(context.Background(), time.Now(), nil, 256)
	var cause *pgconn.PgError
	if items != nil || !errors.As(err, &cause) || cause.Code != "42P01" {
		t.Fatalf("failed read returned data or lost its PostgreSQL cause: items=%v error=%v", items, err)
	}
}

func TestRunningCandidateActualQueriesUseStartTimePartialIndex(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	// Large terminal history plus old and recent running rows exposes missing
	// partial-index predicates, an unnecessary sort, or a second time filter.
	// Plan checks do not disable planner choices or invent a duration guarantee.
	if err := db.Exec(`INSERT INTO job_instance
		(job_id, job_name, executor_group_id, trigger_type, scheduled_at, params_overridden, execution_config, status, started_at, deadline_at)
		SELECT n, 'running candidate history', 1, 1, CURRENT_TIMESTAMP, FALSE,
		jsonb_build_object('schema_version', 1, 'execution_mode', n % 2 + 1, 'params', repeat('x', 256)),
		CASE WHEN n % 20 = 0 THEN 2 ELSE 3 END,
		TIMESTAMPTZ '2026-11-05 00:00:00+00' + n * INTERVAL '1 second',
		CASE WHEN n % 40 = 0 THEN TIMESTAMPTZ '2026-11-05 00:00:00+00' + n * INTERVAL '1 second' + INTERVAL '1 minute'
		ELSE TIMESTAMPTZ '2026-11-06 00:00:00+00' END
		FROM generate_series(1, 30000) n`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE job_instance").Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobInstanceRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	bound := time.Date(2026, 11, 5, 6, 0, 0, 0, time.UTC)
	var cursor *RunningCandidate
	for pageNumber := range 2 {
		counter.count, counter.statements = 0, nil
		page, err := repo.ListRunningCandidates(context.Background(), bound, cursor, 256)
		if err != nil || len(page) != 256 || page[0].ID != int64(pageNumber*5120+20) || page[255].ID != int64((pageNumber+1)*5120) {
			t.Fatalf("indexed page %d: rows=%d error=%v", pageNumber, len(page), err)
		}
		assertRunningCandidateProjection(t, counter)
		plan := explainExecutionQuery(t, db, counter.statements[0])
		if !strings.Contains(plan, "idx_job_instance_running_started") || strings.Contains(plan, "Seq Scan") ||
			strings.Contains(plan, "Sort  (") || !strings.Contains(plan, "started_at <") {
			t.Fatalf("running candidate read did not use its ordered partial index:\n%s", plan)
		}
		if pageNumber > 0 && !strings.Contains(plan, "ROW(started_at, id)") {
			t.Fatalf("continuation lost the composite index condition:\n%s", plan)
		}
		after := page[len(page)-1]
		cursor = &after
	}
}

func insertRunningQueryInstance(t *testing.T, db *gorm.DB, job *model.JobDefinition, status model.InstanceStatus, started, deadline *time.Time) int64 {
	t.Helper()
	request, operator := uuid.New(), int64(1)
	instance := model.JobInstance{JobID: job.ID, JobName: job.Name, ExecutorGroupID: job.ExecutorGroupID,
		TriggerType: model.TriggerManual, RequestID: &request, TriggeredBy: &operator,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: job.ProcessorName,
			ExecutionMode: model.ExecutionModeNormal, RetryIntervalSeconds: 10, TimeoutSeconds: 900},
		Status: status, StartedAt: started, DeadlineAt: deadline}
	if err := db.Create(&instance).Error; err != nil {
		t.Fatal(err)
	}
	return instance.ID
}

func assertRunningPage(t *testing.T, actual []RunningCandidate, err error, expected []RunningCandidate) {
	t.Helper()
	if err != nil || actual == nil || len(actual) != len(expected) {
		t.Fatalf("running candidates: actual=%v expected=%v error=%v", actual, expected, err)
	}
	for index, candidate := range actual {
		if candidate.ID != expected[index].ID || !candidate.StartedAt.Equal(expected[index].StartedAt) {
			t.Fatalf("running candidate %d: actual=%v expected=%v", index, candidate, expected[index])
		}
	}
}

func assertRunningCandidateProjection(t *testing.T, counter *queryCounter) {
	t.Helper()
	if counter.count != 1 || len(counter.statements) != 1 || !strings.HasPrefix(counter.statements[0], `SELECT "id","started_at" FROM "job_instance"`) {
		t.Fatalf("running candidate read was not one minimal query: %v", counter.statements)
	}
	statement := counter.statements[0]
	for _, forbidden := range []string{"execution_config", "result_summary", "deadline_at", "created_at", "updated_at", "job_task", "job_definition", "FOR UPDATE", "OFFSET", "JOIN", "SELECT *"} {
		if strings.Contains(statement, forbidden) {
			t.Fatalf("running candidate read added %s: %s", forbidden, statement)
		}
	}
	if !strings.Contains(statement, "status = 2 AND started_at <") || !strings.Contains(statement, "ORDER BY started_at, id") {
		t.Fatalf("running candidate read changed its strict time/status/order contract: %s", statement)
	}
}
