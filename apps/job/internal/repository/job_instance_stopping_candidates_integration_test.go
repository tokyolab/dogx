//go:build integration

package repository

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestStoppingCandidatesReadOnlyParentIDsAndRetainFixedUpperBound(t *testing.T) {
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
	if upper, err := repo.StoppingUpperID(ctx); err != nil || upper != 0 {
		t.Fatalf("empty stopping bound=%d error=%v", upper, err)
	}
	if ids, err := repo.ListStoppingCandidates(ctx, 0, 100, 2); err != nil || ids == nil || len(ids) != 0 {
		t.Fatalf("empty stopping page=%v error=%v", ids, err)
	}
	first := insertStoppingCandidate(t, db, writer, job)
	second := insertStoppingCandidate(t, db, writer, job)
	last := insertStoppingCandidate(t, db, writer, job)
	for _, state := range []model.InstanceStatus{model.InstanceWaiting, model.InstanceRunning, model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled} {
		id := insertWaitingCandidate(t, writer, job, model.ExecutionModeNormal)
		if err := db.Model(&model.JobInstance{}).Where("id = ?", id).Update("status", state).Error; err != nil {
			t.Fatal(err)
		}
	}
	// Stopping a waiting round does not manufacture execution timestamps.
	// Candidate discovery also remains independent of historical configuration.
	if err := db.Model(&model.JobInstance{}).Where("id = ?", first).
		Update("execution_config", gorm.Expr("?::jsonb", `"invalid snapshot shape"`)).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(job).Error; err != nil {
		t.Fatal(err)
	}
	var noExecutionTimes int64
	if err := db.Model(&model.JobInstance{}).Where("id = ? AND started_at IS NULL AND deadline_at IS NULL", first).Count(&noExecutionTimes).Error; err != nil || noExecutionTimes != 1 {
		t.Fatalf("waiting stop unexpectedly required execution times: count=%d error=%v", noExecutionTimes, err)
	}
	counter.count, counter.statements = 0, nil
	upper, err := repo.StoppingUpperID(ctx)
	if err != nil || upper != last {
		t.Fatalf("stopping upper=%d want=%d error=%v", upper, last, err)
	}
	assertStoppingProjection(t, counter)
	counter.count, counter.statements = 0, nil
	if ids, err := repo.ListStoppingCandidates(ctx, 0, upper, 2); err != nil || !reflect.DeepEqual(ids, []int64{first, second}) {
		t.Fatalf("first stopping page=%v error=%v", ids, err)
	}
	assertStoppingProjection(t, counter)
	newID := insertStoppingCandidate(t, db, writer, job)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", second).Update("status", model.InstanceCanceled).Error; err != nil {
		t.Fatal(err)
	}
	counter.count, counter.statements = 0, nil
	if ids, err := repo.ListStoppingCandidates(ctx, first, upper, 2); err != nil || !reflect.DeepEqual(ids, []int64{last}) {
		t.Fatalf("continuation included terminal/new upper candidates: ids=%v error=%v", ids, err)
	}
	assertStoppingProjection(t, counter)
	if upper, err := repo.StoppingUpperID(ctx); err != nil || upper != newID {
		t.Fatalf("next stopping upper=%d want=%d error=%v", upper, newID, err)
	}
}

func TestStoppingCandidatesRediscoverLowerIDCommittedAfterTraversal(t *testing.T) {
	db, job := resultRepositoryFixture(t)
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
	low := insertStoppingCandidate(t, late, lateRepo, job)
	high := insertStoppingCandidate(t, db, repo, job)
	ctx := context.Background()
	upper, err := repo.StoppingUpperID(ctx)
	if err != nil || upper != high || high <= low {
		t.Fatalf("stopping allocation order: low=%d high=%d upper=%d error=%v", low, high, upper, err)
	}
	if ids, err := repo.ListStoppingCandidates(ctx, 0, upper, 2); err != nil || !reflect.DeepEqual(ids, []int64{high}) {
		t.Fatalf("uncommitted stopping ID was visible: ids=%v error=%v", ids, err)
	}
	if err := late.Commit().Error; err != nil {
		t.Fatal(err)
	}
	if ids, err := repo.ListStoppingCandidates(ctx, high, upper, 2); err != nil || len(ids) != 0 {
		t.Fatalf("completed stopping traversal changed: ids=%v error=%v", ids, err)
	}
	upper, err = repo.StoppingUpperID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ids, err := repo.ListStoppingCandidates(ctx, 0, upper, 2); err != nil || !reflect.DeepEqual(ids, []int64{low, high}) {
		t.Fatalf("fresh stopping traversal lost lower late commit: ids=%v error=%v", ids, err)
	}
}

func TestStoppingCandidateFailuresRemainErrors(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	repo, err := NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, bounds := range [][3]int64{{-1, 9, 2}, {0, 0, 2}, {10, 9, 2}, {0, 9, 0}, {0, 9, -1}} {
		if ids, err := repo.ListStoppingCandidates(context.Background(), bounds[0], bounds[1], int(bounds[2])); err == nil || ids != nil {
			t.Fatalf("invalid stopping bounds accepted: %v", bounds)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if upper, err := repo.StoppingUpperID(ctx); upper != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled stopping upper=%d error=%v", upper, err)
	}
	if ids, err := repo.ListStoppingCandidates(ctx, 0, 9, 2); ids != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled stopping page=%v error=%v", ids, err)
	}
	if err := db.Exec("DROP TABLE job_instance").Error; err != nil {
		t.Fatal(err)
	}
	upper, upperErr := repo.StoppingUpperID(context.Background())
	ids, pageErr := repo.ListStoppingCandidates(context.Background(), 0, 9, 2)
	if upper != 0 || ids != nil {
		t.Fatal("failed stopping reads returned candidate data")
	}
	for _, err := range []error{upperErr, pageErr} {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
			t.Fatalf("stopping read failure lost its PostgreSQL cause: %v", err)
		}
	}
}

func TestStoppingCandidateActualQueriesUsePartialIndex(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	if err := db.Exec(`INSERT INTO job_instance
		(job_id, job_name, executor_group_id, trigger_type, scheduled_at, params_overridden, execution_config, status)
		SELECT n, 'stopping history', 1, 1, CURRENT_TIMESTAMP, FALSE,
		jsonb_build_object('schema_version', 1, 'execution_mode', n % 2 + 1, 'params', repeat('x', 256)),
		CASE WHEN n % 50 = 0 THEN 6 ELSE 3 END
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
	if upper, err := repo.StoppingUpperID(context.Background()); err != nil || upper != 30000 {
		t.Fatalf("stopping history upper=%d error=%v", upper, err)
	}
	if ids, err := repo.ListStoppingCandidates(context.Background(), 10000, 25000, 128); err != nil || len(ids) != 128 || ids[0] != 10050 || ids[len(ids)-1] != 16400 {
		t.Fatalf("stopping history page=%v error=%v", ids, err)
	}
	if counter.count != 2 || len(counter.statements) != 2 {
		t.Fatalf("stopping candidate reads performed extra work: %v", counter.statements)
	}
	// Explain the production GORM statements without forcing planner switches.
	for _, statement := range counter.statements {
		var lines []string
		if err := db.Raw("EXPLAIN (ANALYZE, BUFFERS) " + statement).Scan(&lines).Error; err != nil {
			t.Fatal(err)
		}
		plan := strings.Join(lines, "\n")
		if !strings.Contains(plan, "idx_job_instance_stopping") || strings.Contains(plan, "Seq Scan") || strings.Contains(plan, "Sort  (") {
			t.Fatalf("stopping query did not use its ordered partial index:\n%s", plan)
		}
		t.Logf("stopping candidate query plan:\n%s", plan)
	}
}

func insertStoppingCandidate(t *testing.T, db *gorm.DB, repo JobInstanceRepository, job *model.JobDefinition) int64 {
	t.Helper()
	id := insertWaitingCandidate(t, repo, job, model.ExecutionModeNormal)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", id).
		Updates(map[string]any{"status": model.InstanceStopping, "end_reason": model.EndReasonManualStop}).Error; err != nil {
		t.Fatal(err)
	}
	return id
}

func assertStoppingProjection(t *testing.T, counter *queryCounter) {
	t.Helper()
	if counter.count != 1 || len(counter.statements) != 1 || !strings.HasPrefix(counter.statements[0], `SELECT "id" FROM "job_instance"`) {
		t.Fatalf("stopping candidate read was not one ID-only query: %v", counter.statements)
	}
	for _, forbidden := range []string{"job_task", "execution_config", "job_definition", "started_at", "deadline_at", "FOR UPDATE", "OFFSET", "JOIN", "SELECT *"} {
		if strings.Contains(counter.statements[0], forbidden) {
			t.Fatalf("stopping candidate read added %s: %s", forbidden, counter.statements[0])
		}
	}
}
