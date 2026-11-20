//go:build integration

package repository

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestHistoryCandidatesUseTerminalFinishTimeAndCompositeCursor(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	before := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	early, tied := before.Add(-2*time.Hour), before.Add(-time.Hour)
	// Insert out of finish-time order, including a timestamp tie across pages.
	firstTie := insertHistoryInstance(t, db, job, model.InstanceFailed, &tied)
	first := insertHistoryInstance(t, db, job, model.InstanceSucceeded, &early)
	secondTie := insertHistoryInstance(t, db, job, model.InstanceCanceled, &tied)
	for _, state := range []model.InstanceStatus{model.InstanceWaiting, model.InstanceRunning, model.InstanceStopping, 99} {
		insertHistoryInstance(t, db, job, state, &early)
	}
	for _, finish := range []*time.Time{nil, &before} {
		insertHistoryInstance(t, db, job, model.InstanceSucceeded, finish)
	}
	recent := before.Add(time.Hour)
	recentID := insertHistoryInstance(t, db, job, model.InstanceSucceeded, &recent)
	// Old creation alone never expires a recent finish; recent creation does not
	// hide an old finish. Neither current management rows nor snapshot shape is read.
	if err := db.Model(&model.JobInstance{}).Where("id = ?", recentID).Update("created_at", early.Add(-365*24*time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobInstance{}).Where("id = ?", first).
		Updates(map[string]any{"created_at": recent, "execution_config": gorm.Expr("?::jsonb", `"unusable snapshot"`)}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(job).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(&model.ExecutorGroup{}, job.ExecutorGroupID).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobInstanceRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	want := []HistoryCandidate{{ID: first, FinishedAt: early}, {ID: firstTie, FinishedAt: tied}, {ID: secondTie, FinishedAt: tied}}
	var got []HistoryCandidate
	var cursor *HistoryCandidate
	for {
		counter.count, counter.statements = 0, nil
		page, err := repo.ListExpiredHistory(context.Background(), before, cursor, 2)
		if err != nil || page == nil || len(page) > 2 {
			t.Fatalf("history page=%v error=%v", page, err)
		}
		assertHistoryCandidateQuery(t, counter)
		if len(page) == 0 {
			break
		}
		got = append(got, page...)
		last := page[len(page)-1]
		cursor = &last
	}
	assertHistoryCandidates(t, got, want)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if page, err := repo.ListExpiredHistory(ctx, before, nil, 100); page != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation became an empty page: page=%v error=%v", page, err)
	}
}

func TestHistoryCleanupDeletesOnlyCompleteParentAndItsChildren(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	before := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	finished := before.Add(-time.Hour)
	id := insertHistoryInstance(t, db, job, model.InstanceSucceeded, &finished)
	other := insertHistoryInstance(t, db, job, model.InstanceFailed, &finished)
	insertHistoryTasks(t, db, id, model.TaskSucceeded, model.TaskFailed, model.TaskCanceled)
	insertHistoryTasks(t, db, other, model.TaskSucceeded)
	var originalJob model.JobDefinition
	var originalGroup model.ExecutorGroup
	if err := db.First(&originalJob, job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&originalGroup, job.ExecutorGroupID).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	run, err := NewTransactionRunner(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := run(ctx, func(repos Repositories) error {
		parent, err := repos.Instances.LockForHistoryCleanup(ctx, id)
		if err != nil {
			return fmt.Errorf("lock history parent: %w", err)
		}
		if parent == nil || parent.Status != model.InstanceSucceeded || parent.FinishedAt == nil || !parent.FinishedAt.Before(before) {
			return fmt.Errorf("unexpected locked history parent: %+v", parent)
		}
		count, err := repos.Tasks.DeleteHistoryForInstance(ctx, id)
		if err != nil {
			return fmt.Errorf("delete history children: %w", err)
		}
		if count != 3 {
			return fmt.Errorf("unexpected deleted history child count: %d", count)
		}
		deleted, err := repos.Instances.DeleteHistory(ctx, id, before)
		if err != nil {
			return fmt.Errorf("delete history parent: %w", err)
		}
		if !deleted {
			return errors.New("history parent was not deleted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(counter.statements) != 3 || !strings.HasPrefix(counter.statements[0], `SELECT "id","status","finished_at"`) ||
		!strings.Contains(counter.statements[0], "FOR UPDATE NOWAIT") ||
		!strings.HasPrefix(counter.statements[1], `DELETE FROM "job_task"`) ||
		!strings.HasPrefix(counter.statements[2], `DELETE FROM "job_instance"`) ||
		!strings.Contains(counter.statements[2], "NOT EXISTS") {
		t.Fatalf("cleanup changed its bounded statements or physical deletion contract: %v", counter.statements)
	}
	assertHistoryCounts(t, db, id, 0, 0)
	assertHistoryCounts(t, db, other, 1, 1)
	var savedJob model.JobDefinition
	var savedGroup model.ExecutorGroup
	if err := db.Unscoped().First(&savedJob, job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Unscoped().First(&savedGroup, job.ExecutorGroupID).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(originalJob, savedJob) || !reflect.DeepEqual(originalGroup, savedGroup) {
		t.Fatal("history cleanup modified the Job or executor group")
	}
}

func TestHistoryCleanupRollsBackChildrenWhenAnyChildRemains(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	before := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	finished := before.Add(-time.Hour)
	run, err := NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	guarded := errors.New("remaining Task prevented parent deletion")
	for _, unfinished := range []model.TaskStatus{model.TaskWaiting, model.TaskRunning, model.TaskRetryWaiting, 99} {
		t.Run(fmt.Sprintf("remaining_status_%d", unfinished), func(t *testing.T) {
			id := insertHistoryInstance(t, db, job, model.InstanceSucceeded, &finished)
			insertHistoryTasks(t, db, id, model.TaskSucceeded, unfinished)
			ctx := context.Background()
			err := run(ctx, func(repos Repositories) error {
				parent, err := repos.Instances.LockForHistoryCleanup(ctx, id)
				if err != nil {
					return fmt.Errorf("lock history parent: %w", err)
				}
				if parent == nil {
					return errors.New("history parent was not found")
				}
				count, err := repos.Tasks.DeleteHistoryForInstance(ctx, id)
				if err != nil {
					return fmt.Errorf("delete terminal history children: %w", err)
				}
				if count != 1 {
					return fmt.Errorf("unexpected deleted terminal history child count: %d", count)
				}
				deleted, err := repos.Instances.DeleteHistory(ctx, id, before)
				if err != nil {
					return fmt.Errorf("guard history parent deletion: %w", err)
				}
				if deleted {
					return errors.New("history parent with remaining children was deleted")
				}
				return guarded
			})
			if !errors.Is(err, guarded) {
				t.Fatalf("unexpected guard result: %v", err)
			}
			assertHistoryCounts(t, db, id, 1, 2)
		})
	}
}

func TestHistoryCleanupDeleteFailureRollsBackAllChildren(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	before := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	finished := before.Add(-time.Hour)
	id := insertHistoryInstance(t, db, job, model.InstanceFailed, &finished)
	insertHistoryTasks(t, db, id, model.TaskSucceeded, model.TaskFailed, model.TaskCanceled)
	// The trigger fails the actual parent DELETE after the child DELETE succeeds.
	// It lives only in this test's isolated schema and is removed with that schema.
	if err := db.Exec(`CREATE FUNCTION reject_history_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'synthetic parent history deletion failure'; END; $$`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TRIGGER reject_history_delete BEFORE DELETE ON job_instance
FOR EACH ROW EXECUTE FUNCTION reject_history_delete()`).Error; err != nil {
		t.Fatal(err)
	}
	run, err := NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	err = run(ctx, func(repos Repositories) error {
		if _, err := repos.Instances.LockForHistoryCleanup(ctx, id); err != nil {
			return err
		}
		count, err := repos.Tasks.DeleteHistoryForInstance(ctx, id)
		if err != nil {
			return fmt.Errorf("delete history children: %w", err)
		}
		if count != 3 {
			return fmt.Errorf("unexpected deleted history child count: %d", count)
		}
		deleted, err := repos.Instances.DeleteHistory(ctx, id, before)
		if deleted || err == nil {
			return errors.New("synthetic parent failure did not fail deletion")
		}
		return err
	})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
		t.Fatalf("parent failure lost database cause: %v", err)
	}
	assertHistoryCounts(t, db, id, 1, 3)
}

func TestHistoryCleanupRechecksParentAgeAndTerminalStatus(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	before := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	early, recent := before.Add(-time.Hour), before.Add(time.Hour)
	for _, state := range []model.InstanceStatus{model.InstanceWaiting, model.InstanceRunning, model.InstanceStopping, 99, model.InstanceSucceeded} {
		for _, finish := range []*time.Time{nil, &early, &before, &recent} {
			id := insertHistoryInstance(t, db, job, state, finish)
			err := db.Transaction(func(tx *gorm.DB) error {
				repo, err := NewJobInstanceRepository(tx)
				if err != nil {
					return err
				}
				if _, err := repo.LockForHistoryCleanup(context.Background(), id); err != nil {
					return err
				}
				deleted, err := repo.DeleteHistory(context.Background(), id, before)
				if err != nil {
					return fmt.Errorf("recheck history parent deletion: %w", err)
				}
				want := state == model.InstanceSucceeded && finish != nil && finish.Before(before)
				if deleted != want {
					return fmt.Errorf("rechecked parent status=%d finish=%v deleted=%t want=%t", state, finish, deleted, want)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestHistoryCleanupNowaitSerializesRacingCleaners(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	before := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	finished := before.Add(-time.Hour)
	id := insertHistoryInstance(t, db, job, model.InstanceCanceled, &finished)
	insertHistoryTasks(t, db, id, model.TaskSucceeded, model.TaskCanceled)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first := db.WithContext(ctx).Begin()
	if first.Error != nil {
		t.Fatal(first.Error)
	}
	defer first.Rollback()
	instances, err := NewJobInstanceRepository(first)
	if err != nil {
		t.Fatal(err)
	}
	if parent, err := instances.LockForHistoryCleanup(ctx, id); err != nil || parent == nil {
		t.Fatalf("first cleaner lock: parent=%+v error=%v", parent, err)
	}
	run, err := NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	err = run(ctx, func(repos Repositories) error {
		parent, err := repos.Instances.LockForHistoryCleanup(ctx, id)
		if parent != nil {
			return errors.New("competing cleaner acquired the same parent")
		}
		return err
	})
	var pgErr *pgconn.PgError
	if !errors.Is(err, ErrInstanceLocked) || !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("racing cleaner did not fail NOWAIT: %v", err)
	}
	tasks, err := NewJobTaskRepository(first)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := tasks.DeleteHistoryForInstance(ctx, id); err != nil || count != 2 {
		t.Fatalf("winning cleaner child count=%d error=%v", count, err)
	}
	if deleted, err := instances.DeleteHistory(ctx, id, before); err != nil || !deleted {
		t.Fatalf("winning cleaner parent deleted=%t error=%v", deleted, err)
	}
	if err := first.Commit().Error; err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, func(repos Repositories) error {
		parent, err := repos.Instances.LockForHistoryCleanup(ctx, id)
		if err != nil {
			return fmt.Errorf("lock already deleted history parent: %w", err)
		}
		if parent != nil {
			return fmt.Errorf("later cleaner must see missing parent: %+v", parent)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertHistoryCounts(t, db, id, 0, 0)
}

func TestHistoryCandidatesUsePartialIndexWithRealBoundParameters(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	if err := db.Exec(`INSERT INTO job_instance
(job_id, job_name, executor_group_id, trigger_type, scheduled_at, params_overridden, execution_config, status, finished_at)
SELECT n, 'history cleanup plan', 1, 1, TIMESTAMPTZ '2026-09-01 00:00:00+00', FALSE,
jsonb_build_object('schema_version', 1, 'params', repeat('h', 512)),
CASE WHEN n % 10 = 0 THEN 2 ELSE 3 + n % 3 END,
TIMESTAMPTZ '2026-10-01 00:00:00+00' + (n / 3) * INTERVAL '1 second'
FROM generate_series(1, 30000) n`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE job_instance").Error; err != nil {
		t.Fatal(err)
	}
	capture := &executionQueryCapture{queryCounter: &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}}
	repo, err := NewJobInstanceRepository(db.Session(&gorm.Session{Logger: capture}))
	if err != nil {
		t.Fatal(err)
	}
	before := time.Date(2026, 10, 1, 2, 0, 0, 0, time.UTC)
	var cursor *HistoryCandidate
	for pageNumber := range 2 {
		capture.reset()
		page, err := repo.ListExpiredHistory(context.Background(), before, cursor, 100)
		if err != nil || len(page) != 100 {
			t.Fatalf("history plan page=%d count=%d error=%v", pageNumber, len(page), err)
		}
		assertHistoryCandidateQuery(t, capture.queryCounter)
		if len(capture.queries) != 1 || len(capture.queries[0].vars) == 0 {
			t.Fatalf("repository query did not retain its real parameter bindings: %+v", capture.queries)
		}
		plan := explainExecutionQuery(t, db, capture.queries[0])
		if !strings.Contains(plan, "idx_job_instance_history_expired") || strings.Contains(plan, "Sort") {
			t.Fatalf("history keyset page lost its ordered partial index: %s", plan)
		}
		if cursor != nil && (page[0].FinishedAt.Before(cursor.FinishedAt) ||
			(page[0].FinishedAt.Equal(cursor.FinishedAt) && page[0].ID <= cursor.ID)) {
			t.Fatal("continuation page repeated or moved before its cursor")
		}
		last := page[len(page)-1]
		cursor = &last
	}
}

func insertHistoryInstance(t *testing.T, db *gorm.DB, job *model.JobDefinition, state model.InstanceStatus, finished *time.Time) int64 {
	t.Helper()
	id := insertRunningQueryInstance(t, db, job, state, nil, nil)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", id).Update("finished_at", finished).Error; err != nil {
		t.Fatal(err)
	}
	return id
}

func insertHistoryTasks(t *testing.T, db *gorm.DB, instanceID int64, states ...model.TaskStatus) {
	t.Helper()
	tasks := make([]model.JobTask, len(states))
	for i, state := range states {
		tasks[i] = model.JobTask{InstanceID: instanceID, TaskType: model.TaskTypeShard, Status: state}
	}
	if err := db.Create(&tasks).Error; err != nil {
		t.Fatal(err)
	}
}

func assertHistoryCounts(t *testing.T, db *gorm.DB, id, parents, tasks int64) {
	t.Helper()
	var parentCount, taskCount int64
	if err := db.Model(&model.JobInstance{}).Where("id = ?", id).Count(&parentCount).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobTask{}).Where("instance_id = ?", id).Count(&taskCount).Error; err != nil {
		t.Fatal(err)
	}
	if parentCount != parents || taskCount != tasks {
		t.Fatalf("history records for parent %d: parents=%d tasks=%d want=%d/%d", id, parentCount, taskCount, parents, tasks)
	}
}

func assertHistoryCandidates(t *testing.T, got, want []HistoryCandidate) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("history candidates=%v want=%v", got, want)
	}
	for i := range got {
		if got[i].ID != want[i].ID || !got[i].FinishedAt.Equal(want[i].FinishedAt) {
			t.Fatalf("history candidate %d=%+v want=%+v", i, got[i], want[i])
		}
	}
}

func assertHistoryCandidateQuery(t *testing.T, counter *queryCounter) {
	t.Helper()
	if counter.count != 1 || len(counter.statements) != 1 ||
		!strings.HasPrefix(counter.statements[0], `SELECT "id","finished_at" FROM "job_instance"`) ||
		!strings.Contains(counter.statements[0], "ORDER BY finished_at, id") {
		t.Fatalf("history discovery must be one ordered minimal query: %v", counter.statements)
	}
	for _, forbidden := range []string{"job_task", "job_definition", "JOIN", "OFFSET", "FOR UPDATE", "execution_config", "created_at", "result_summary", "SELECT *"} {
		if strings.Contains(counter.statements[0], forbidden) {
			t.Fatalf("history discovery added %s: %s", forbidden, counter.statements[0])
		}
	}
}
