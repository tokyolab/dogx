//go:build integration

package instancestop_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/instancecheck"
	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
)

func TestTimeoutCheckCompletesLateFinalResultsWithoutStopping(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	controller, instances, _, _ := stopDependencies(t, db)
	for _, state := range []model.TaskStatus{model.TaskSucceeded, model.TaskFailed} {
		parent, child := expiredOrdinary(t, db, instances, state)
		published, resumed := 0, 0
		checker, err := instancecheck.NewChecker(instances, instancecheck.WithStopping(controller,
			func(instancestop.Decision) { published++ }, func(int64) { resumed++ }))
		if err != nil {
			t.Fatal(err)
		}
		if outcome, err := checker.Check(context.Background(), parent.ID); err != nil || outcome != instancecheck.OutcomeTerminal {
			t.Fatalf("late final result did not normally finish: %d %v", outcome, err)
		}
		current := readStopParent(t, db, parent.ID)
		want := model.InstanceSucceeded
		if state == model.TaskFailed {
			want = model.InstanceFailed
		}
		if current.Status != want || current.ResultSummary == nil || *current.ResultSummary != *child.ResultSummary ||
			current.EndReason == nil || *current.EndReason == model.EndReasonInstanceTimeout || published != 0 || resumed != 0 {
			t.Fatal("late complete facts were reclassified as timeout")
		}
	}
}

func TestTimeoutCheckCreatesOneDecisionThenConvergesChildrenAndParent(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	controller, instances, _, _ := stopDependencies(t, db)
	parent, child := expiredOrdinary(t, db, instances, model.TaskRunning)
	var decisions []instancestop.Decision
	var resumed []int64
	checker, err := instancecheck.NewChecker(instances, instancecheck.WithStopping(controller,
		func(decision instancestop.Decision) { decisions = append(decisions, decision) },
		func(id int64) { resumed = append(resumed, id) }))
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := checker.Check(context.Background(), parent.ID); err != nil || outcome != instancecheck.OutcomeStopping ||
		len(decisions) != 1 || !decisions[0].New || decisions[0].EndReason == nil || *decisions[0].EndReason != model.EndReasonInstanceTimeout {
		t.Fatalf("timeout decision was not committed and handed off: %d %v", outcome, err)
	}
	stopping := readStopParent(t, db, parent.ID)
	if stopping.Status != model.InstanceStopping || stopping.FinishedAt != nil || !stopping.DeadlineAt.Equal(*parent.DeadlineAt) {
		t.Fatal("timeout decision prematurely finalized or extended the parent")
	}
	if err := db.First(&child, child.ID).Error; err != nil {
		t.Fatal(err)
	}
	if child.Status != model.TaskRunning {
		t.Fatal("timeout decision also canceled children in its transaction")
	}
	if outcome, err := checker.Check(context.Background(), parent.ID); err != nil || outcome != instancecheck.OutcomeStopping || len(decisions) != 1 || len(resumed) != 1 || resumed[0] != parent.ID {
		t.Fatal("existing STOPPING was not resumed independently of notification")
	}
	for range 2 {
		if _, err := controller.ProcessBatch(context.Background(), parent.ID); err != nil {
			t.Fatal(err)
		}
	}
	finished := readStopParent(t, db, parent.ID)
	if err := db.First(&child, child.ID).Error; err != nil {
		t.Fatal(err)
	}
	if finished.Status != model.InstanceFailed || finished.FinishedAt == nil || finished.EndReason == nil || *finished.EndReason != model.EndReasonInstanceTimeout ||
		child.Status != model.TaskCanceled || child.EndReason == nil || *child.EndReason != model.EndReasonInstanceTimeout {
		t.Fatal("timeout did not converge to failed parent and canceled child")
	}
}

func TestTimeoutCheckDoesNotTurnInvalidFrozenFactsIntoTimeout(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	controller, instances, _, _ := stopDependencies(t, db)
	parent, _ := expiredOrdinary(t, db, instances, model.TaskRunning)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", parent.ID).Update("execution_config", gorm.Expr("'{}'::jsonb")).Error; err != nil {
		t.Fatal(err)
	}
	checker, err := instancecheck.NewChecker(instances, instancecheck.WithStopping(controller,
		func(instancestop.Decision) { t.Error("invalid facts published a stop") }, func(int64) { t.Error("invalid facts resumed a stop") }))
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := checker.Check(context.Background(), parent.ID); outcome != instancecheck.OutcomeSkipped || !errors.Is(err, instancecheck.ErrInvalidConfig) {
		t.Fatalf("invalid snapshot did not preserve the error: %d %v", outcome, err)
	}
	if current := readStopParent(t, db, parent.ID); current.Status != model.InstanceRunning || current.EndReason != nil {
		t.Fatal("configuration error changed the running parent")
	}
}

func TestTimeoutKeepsConcurrentFinalResultAndParentDecisionsDistinct(t *testing.T) {
	for _, completeParent := range []bool{false, true} {
		name := "result arrives after initial check"
		if completeParent {
			name = "normal completion commits first"
		}
		t.Run(name, func(t *testing.T) {
			db, _ := testutil.OpenMigratedPostgres(t)
			controller, instances, _, _ := stopDependencies(t, db)
			parent, child := expiredOrdinary(t, db, instances, model.TaskRunning)
			requester := &interleavedTimeoutRequester{controller: controller, before: func(ctx context.Context) error {
				if err := db.WithContext(ctx).Model(&model.JobTask{}).Where("id = ?", child.ID).Updates(map[string]any{
					"status": model.TaskSucceeded, "end_reason": model.EndReasonSuccess,
					"finished_at": gorm.Expr("clock_timestamp()"),
				}).Error; err != nil {
					return err
				}
				if completeParent {
					normal, err := instancecheck.NewChecker(instances)
					if err != nil {
						return err
					}
					outcome, err := normal.Check(ctx, parent.ID)
					if err != nil {
						return err
					}
					if outcome != instancecheck.OutcomeTerminal {
						return errors.New("concurrent normal completion did not commit")
					}
				}
				return nil
			}}
			published := 0
			checker, err := instancecheck.NewChecker(instances, instancecheck.WithStopping(requester,
				func(instancestop.Decision) { published++ }, func(int64) { t.Error("unexpected existing stopping handoff") }))
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := checker.Check(context.Background(), parent.ID)
			if err != nil {
				t.Fatal(err)
			}
			current := readStopParent(t, db, parent.ID)
			if completeParent {
				if outcome != instancecheck.OutcomeTerminal || current.Status != model.InstanceSucceeded || published != 0 {
					t.Fatal("timeout overwrote a normal completion committed before its parent lock")
				}
			} else if outcome != instancecheck.OutcomeStopping || current.Status != model.InstanceStopping || published != 1 {
				t.Fatal("timeout reread Task facts instead of applying the approved parent-only recheck")
			}
			if err := db.First(&child, child.ID).Error; err != nil {
				t.Fatal(err)
			}
			if child.Status != model.TaskSucceeded {
				t.Fatal("timeout decision rewrote the concurrent Task result")
			}
		})
	}
}

func TestTimeoutReadsDatabaseTimeAfterActuallyObtainingTheParentLock(t *testing.T) {
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	_, instances, _, runner := stopDependencies(t, db)
	controller, err := instancestop.NewController(runner, instancestop.WithSQLTimeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	parent, _ := expiredOrdinary(t, db, instances, model.TaskRunning)
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	var blocker int
	if err := tx.Raw("SELECT pg_backend_pid()").Scan(&blocker).Error; err != nil {
		t.Fatal(err)
	}
	// The transaction starts before the new deadline, but its parent row lock
	// is held until after that deadline. transaction_timestamp would be stale.
	var deadline time.Time
	if err := tx.Raw("SELECT clock_timestamp() + INTERVAL '1 second'").Scan(&deadline).Error; err != nil {
		t.Fatal(err)
	}
	if err := tx.Model(&model.JobInstance{}).Where("id = ?", parent.ID).Update("deadline_at", deadline).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan struct{})
	var decision instancestop.Decision
	var decisionErr error
	go func() { decision, decisionErr = controller.RequestTimeout(ctx, parent.ID); close(done) }()
	defer func() { _ = tx.Rollback().Error; cancel(); <-done }()
	waitTimeoutBlockers(t, ctx, sqlDB, blocker, 1, done)
	var beganBeforeDeadline bool
	if err := sqlDB.QueryRowContext(ctx, "SELECT COALESCE(bool_and(xact_start < $2), false) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", blocker, deadline).Scan(&beganBeforeDeadline); err != nil {
		t.Fatal(err)
	}
	if !beganBeforeDeadline {
		t.Fatal("test transaction did not begin before its deadline")
	}
	for {
		now, err := instances.DatabaseTime(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !now.Before(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	<-done
	if decisionErr != nil || !decision.New || decision.EndReason == nil || *decision.EndReason != model.EndReasonInstanceTimeout {
		t.Fatalf("locked current time was not used: %+v %v", decision, decisionErr)
	}
}

func TestManualAndTimeoutDecisionsCoordinateWithConcurrentNormalCompletion(t *testing.T) {
	for _, timeoutWins := range []bool{false, true} {
		name := "manual wins"
		if timeoutWins {
			name = "timeout wins"
		}
		t.Run(name, func(t *testing.T) {
			db, sqlDB := testutil.OpenMigratedPostgres(t)
			_, instances, _, runner := stopDependencies(t, db)
			parent, child := expiredOrdinary(t, db, instances, model.TaskRunning)
			release, written := make(chan struct{}), make(chan int, 1)
			finish := sync.OnceFunc(func() { close(release) })
			defer finish()
			// The test runner holds a real written decision before commit, so
			// competing parent writes must observe the winning committed state.
			gated := func(ctx context.Context, call func(repository.Repositories) error) error {
				return runner(ctx, func(repos repository.Repositories) error {
					if err := call(repos); err != nil {
						return err
					}
					// The transaction's row lock can be identified from pg_locks by
					// finding the concurrent statements waiting on the parent below.
					written <- 1
					<-release
					return nil
				})
			}
			winner, err := instancestop.NewController(gated, instancestop.WithSQLTimeout(5*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			loser, err := instancestop.NewController(runner, instancestop.WithSQLTimeout(5*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			var first, second instancestop.Decision
			var firstErr, secondErr, normalErr error
			var completed bool
			firstDone, secondDone, normalDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			go func() {
				if timeoutWins {
					first, firstErr = winner.RequestTimeout(ctx, parent.ID)
				} else {
					first, firstErr = winner.RequestManual(ctx, parent.ID)
				}
				close(firstDone)
			}()
			defer func() { finish(); cancel(); <-firstDone }()
			<-written
			// A result may commit without the parent lock while the stop decision
			// is still uncommitted. The competing normal completion therefore has
			// genuine stable final Task facts, but must respect the parent winner.
			if err := db.Model(&model.JobTask{}).Where("id = ?", child.ID).Updates(map[string]any{
				"status": model.TaskSucceeded, "end_reason": model.EndReasonSuccess,
				"finished_at": gorm.Expr("clock_timestamp()"),
			}).Error; err != nil {
				t.Fatal(err)
			}
			go func() {
				if timeoutWins {
					second, secondErr = loser.RequestManual(ctx, parent.ID)
				} else {
					second, secondErr = loser.RequestTimeout(ctx, parent.ID)
				}
				close(secondDone)
			}()
			go func() {
				completed, normalErr = instances.Complete(ctx, parent.ID, model.InstanceSucceeded, nil)
				close(normalDone)
			}()
			defer func() { finish(); cancel(); <-secondDone; <-normalDone }()
			// All sessions use this test's isolated search_path. The database-wide
			// query is only an observation of locks, never a work-discovery path.
			waitTimeoutBlockers(t, ctx, sqlDB, 0, 2, firstDone)
			finish()
			<-firstDone
			<-secondDone
			<-normalDone
			want := model.EndReasonManualStop
			if timeoutWins {
				want = model.EndReasonInstanceTimeout
			}
			if firstErr != nil || secondErr != nil || normalErr != nil || !first.New || second.New || completed ||
				first.EndReason == nil || second.EndReason == nil || *first.EndReason != want || *second.EndReason != want {
				t.Fatalf("competing decisions lost the winner: first=%+v second=%+v errors=%v/%v/%v", first, second, firstErr, secondErr, normalErr)
			}
			current := readStopParent(t, db, parent.ID)
			if current.Status != model.InstanceStopping || current.EndReason == nil || *current.EndReason != want {
				t.Fatal("normal completion overwrote STOPPING")
			}
		})
	}
}

func expiredOrdinary(t *testing.T, db *gorm.DB, instances repository.JobInstanceRepository, state model.TaskStatus) (model.JobInstance, model.JobTask) {
	t.Helper()
	parent := stopParent(t, db, instances, model.InstanceRunning)
	deadline := parent.StartedAt.Add(30 * time.Second)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", parent.ID).Update("deadline_at", deadline).Error; err != nil {
		t.Fatal(err)
	}
	parent.DeadlineAt = &deadline
	child := model.JobTask{InstanceID: parent.ID, TaskType: model.TaskTypeOrdinary, Status: state,
		AttemptNo: 1, UsedAttempts: 1, StartedAt: parent.StartedAt, AttemptRegisteredAt: parent.StartedAt,
		CreatedAt: *parent.StartedAt, UpdatedAt: *parent.StartedAt}
	if state == model.TaskSucceeded || state == model.TaskFailed {
		summary, reason := "final business result", model.EndReasonSuccess
		if state == model.TaskFailed {
			reason = model.EndReasonExecutionFailed
		}
		child.ResultSummary, child.EndReason, child.FinishedAt = &summary, &reason, &deadline
	}
	if err := db.Create(&child).Error; err != nil {
		t.Fatal(err)
	}
	return parent, child
}

func waitTimeoutBlockers(t *testing.T, ctx context.Context, db *sql.DB, blocker, count int, premature <-chan struct{}) {
	t.Helper()
	observed, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for {
		var waiting int
		query := "SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))"
		var err error
		if blocker != 0 {
			err = db.QueryRowContext(observed, query, blocker).Scan(&waiting)
		} else {
			err = db.QueryRowContext(observed, "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%job_instance%'").Scan(&waiting)
		}
		if err != nil {
			t.Fatal(err)
		}
		if waiting >= count {
			return
		}
		select {
		case <-premature:
			t.Fatal("transaction returned before the expected parent lock contention")
		case <-observed.Done():
			t.Fatal("expected real parent lock waits were not observed")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// interleavedTimeoutRequester injects a separately committed business result
// between the caller's NotReady observation and the real parent-only timeout
// transaction. It does not replace the controller or hold its transaction open.
type interleavedTimeoutRequester struct {
	controller *instancestop.Controller
	before     func(context.Context) error
}

func (r *interleavedTimeoutRequester) RequestTimeout(ctx context.Context, id int64) (instancestop.Decision, error) {
	if err := r.before(ctx); err != nil {
		return instancestop.Decision{}, err
	}
	return r.controller.RequestTimeout(ctx, id)
}
