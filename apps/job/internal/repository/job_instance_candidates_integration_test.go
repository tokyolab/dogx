//go:build integration

package repository

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestWaitingCandidatesUseFixedUpperBoundAndCurrentState(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobInstanceRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if upper, err := repo.MaxWaitingID(ctx); upper != 0 || err != nil {
		t.Fatalf("empty upper bound=%d err=%v", upper, err)
	}
	if ids, err := repo.ListWaitingIDs(ctx, 0, 100, 128); ids == nil || len(ids) != 0 || err != nil {
		t.Fatalf("empty candidates=%v err=%v", ids, err)
	}
	first := insertWaitingCandidate(t, writer, job, model.ExecutionModeNormal)
	sharded := insertWaitingCandidate(t, writer, job, model.ExecutionModeSharded)
	last := insertWaitingCandidate(t, writer, job, model.ExecutionModeNormal)
	terminal := insertWaitingCandidate(t, writer, job, model.ExecutionModeNormal)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", terminal).Update("status", model.InstanceSucceeded).Error; err != nil {
		t.Fatal(err)
	}
	// Neither a removed current Job nor an undecodable historical snapshot may
	// filter or break this ID-only candidate read. Authorization inspects it later.
	if err := db.Delete(job).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobInstance{}).Where("id = ?", last).
		Update("execution_config", gorm.Expr("?::jsonb", `"invalid snapshot shape"`)).Error; err != nil {
		t.Fatal(err)
	}
	counter.count, counter.statements = 0, nil
	upper, err := repo.MaxWaitingID(ctx)
	if err != nil || upper != last {
		t.Fatalf("upper=%d want=%d err=%v", upper, last, err)
	}
	assertWaitingProjection(t, counter)
	if ids, err := repo.ListWaitingIDs(ctx, 0, upper, 128); err != nil || !reflect.DeepEqual(ids, []int64{first, sharded, last}) {
		t.Fatalf("mode-neutral candidates=%v err=%v", ids, err)
	}
	counter.count, counter.statements = 0, nil
	if ids, err := repo.ListWaitingIDs(ctx, 0, upper, 1); err != nil || !reflect.DeepEqual(ids, []int64{first}) {
		t.Fatalf("first page=%v err=%v", ids, err)
	}
	assertWaitingProjection(t, counter)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", sharded).Update("status", model.InstanceRunning).Error; err != nil {
		t.Fatal(err)
	}
	newID := insertWaitingCandidate(t, writer, job, model.ExecutionModeNormal)
	counter.count, counter.statements = 0, nil
	if ids, err := repo.ListWaitingIDs(ctx, first, upper, 128); err != nil || !reflect.DeepEqual(ids, []int64{last}) {
		t.Fatalf("fixed-bound continuation=%v err=%v", ids, err)
	}
	assertWaitingProjection(t, counter)
	if nextUpper, err := repo.MaxWaitingID(ctx); err != nil || nextUpper != newID {
		t.Fatalf("next traversal upper=%d want=%d err=%v", nextUpper, newID, err)
	}
	if ids, err := repo.ListWaitingIDs(ctx, 0, newID, 128); err != nil || !reflect.DeepEqual(ids, []int64{first, last, newID}) {
		t.Fatalf("next traversal=%v err=%v", ids, err)
	}
}

func TestWaitingCandidatesRediscoverLowerIDCommittedAfterTraversal(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	ctx := context.Background()
	late := db.Begin()
	if late.Error != nil {
		t.Fatal(late.Error)
	}
	defer late.Rollback()
	lateRepo, err := NewJobInstanceRepository(late)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	// Sequence allocation precedes commit. The lower ID remains uncommitted on
	// one connection while the higher ID is committed and scanned on another.
	low := insertWaitingCandidate(t, lateRepo, job, model.ExecutionModeNormal)
	high := insertWaitingCandidate(t, repo, job, model.ExecutionModeNormal)
	if high <= low {
		t.Fatalf("invalid allocation order: low=%d high=%d", low, high)
	}
	upper, err := repo.MaxWaitingID(ctx)
	if err != nil || upper != high {
		t.Fatalf("upper=%d err=%v", upper, err)
	}
	if ids, err := repo.ListWaitingIDs(ctx, 0, upper, 128); err != nil || !reflect.DeepEqual(ids, []int64{high}) {
		t.Fatalf("uncommitted lower ID became visible: ids=%v err=%v", ids, err)
	}
	if err := late.Commit().Error; err != nil {
		t.Fatal(err)
	}
	if ids, err := repo.ListWaitingIDs(ctx, high, upper, 128); err != nil || ids == nil || len(ids) != 0 {
		t.Fatalf("completed traversal changed: ids=%v err=%v", ids, err)
	}
	upper, err = repo.MaxWaitingID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ids, err := repo.ListWaitingIDs(ctx, 0, upper, 128); err != nil || !reflect.DeepEqual(ids, []int64{low, high}) {
		t.Fatalf("next traversal lost late commit: ids=%v err=%v", ids, err)
	}
}

func TestWaitingCandidateFailuresDoNotBecomeEmptyPages(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	repo, err := NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if upper, err := repo.MaxWaitingID(ctx); upper != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled upper=%d err=%v", upper, err)
	}
	if ids, err := repo.ListWaitingIDs(ctx, 0, 10, 128); ids != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled page=%v err=%v", ids, err)
	}
	if err := db.Exec("DROP TABLE job_instance").Error; err != nil {
		t.Fatal(err)
	}
	upper, upperErr := repo.MaxWaitingID(context.Background())
	ids, pageErr := repo.ListWaitingIDs(context.Background(), 0, 10, 128)
	if upper != 0 || ids != nil {
		t.Fatalf("failed reads returned candidate data: upper=%d ids=%v", upper, ids)
	}
	for _, err := range []error{upperErr, pageErr} {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
			t.Fatalf("candidate failure lost its database cause: %v", err)
		}
	}
}

func TestWaitingCandidateQueriesUsePartialIndex(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	// A history-heavy fixture exposes an accidental full-table or configuration
	// scan. No planner switches or timing limits force a particular plan or speed.
	if err := db.Exec(`INSERT INTO job_instance
		(job_id, job_name, executor_group_id, trigger_type, scheduled_at, params_overridden, execution_config, status)
		SELECT n, 'candidate history', 1, 1, CURRENT_TIMESTAMP, FALSE,
		jsonb_build_object('schema_version', 1, 'execution_mode', n % 2 + 1, 'params', repeat('x', 256)),
		CASE WHEN n % 50 = 0 THEN 1 ELSE 3 END
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
	if upper, err := repo.MaxWaitingID(context.Background()); err != nil || upper != 30000 {
		t.Fatalf("history upper=%d err=%v", upper, err)
	}
	if ids, err := repo.ListWaitingIDs(context.Background(), 10000, 25000, 128); err != nil || len(ids) != 128 || ids[0] != 10050 || ids[len(ids)-1] != 16400 {
		t.Fatalf("history page=%v err=%v", ids, err)
	}
	if counter.count != 2 || len(counter.statements) != 2 {
		t.Fatalf("candidate queries performed extra work: %v", counter.statements)
	}
	// Explain the actual GORM-generated statements, including both key bounds,
	// rather than a separate idealized query that production never executes.
	for _, statement := range counter.statements {
		var lines []string
		if err := db.Raw("EXPLAIN (ANALYZE, BUFFERS) " + statement).Scan(&lines).Error; err != nil {
			t.Fatal(err)
		}
		plan := strings.Join(lines, "\n")
		if !strings.Contains(plan, "idx_job_instance_waiting") || strings.Contains(plan, "Seq Scan") || strings.Contains(plan, "Sort  (") {
			t.Fatalf("candidate query did not use its ordered partial index:\n%s", plan)
		}
		t.Logf("candidate query plan:\n%s", plan)
	}
}

func insertWaitingCandidate(t *testing.T, repo JobInstanceRepository, job *model.JobDefinition, mode model.ExecutionMode) int64 {
	t.Helper()
	request, operator := uuid.New(), int64(1)
	instance := &model.JobInstance{JobID: job.ID, JobName: job.Name, ExecutorGroupID: job.ExecutorGroupID,
		TriggerType: model.TriggerManual, RequestID: &request, TriggeredBy: &operator,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: job.ProcessorName, ExecutionMode: mode,
			RetryIntervalSeconds: 10, TimeoutSeconds: 900}}
	id, created, err := repo.Insert(context.Background(), instance)
	if err != nil || !created {
		t.Fatalf("insert candidate: id=%d created=%v err=%v", id, created, err)
	}
	return id
}

func assertWaitingProjection(t *testing.T, counter *queryCounter) {
	t.Helper()
	if counter.count != 1 || len(counter.statements) != 1 || !strings.HasPrefix(counter.statements[0], `SELECT "id" FROM "job_instance"`) {
		t.Fatalf("candidate read was not one ID-only query: %v", counter.statements)
	}
	statement := counter.statements[0]
	for _, forbidden := range []string{"execution_config", "job_definition", "FOR UPDATE", "OFFSET", "JOIN", "SELECT *"} {
		if strings.Contains(statement, forbidden) {
			t.Fatalf("candidate read added %s: %s", forbidden, statement)
		}
	}
}
