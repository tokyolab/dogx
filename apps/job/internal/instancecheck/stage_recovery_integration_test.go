//go:build integration

package instancecheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
)

func TestRunningRecoveryResumesLostShardedHintsAtTheFourMinuteBoundary(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	prepared := newStageIntegrationFixture(t, db, false, 0)
	prepared.savePreparation(t, []json.RawMessage{json.RawMessage(`null`), json.RawMessage(`9007199254740993`)}, taskresult.OutcomeSuccess)
	readyPost := newStageIntegrationFixture(t, db, true, 0)
	readyPost.savePreparation(t, []json.RawMessage{json.RawMessage(`1`), json.RawMessage(`2`)}, taskresult.OutcomeSuccess)
	if got, err := readyPost.checker.Check(context.Background(), readyPost.parent.ID); err != nil || got != OutcomeAdvanced {
		t.Fatalf("prepare final shards: %d %v", got, err)
	}
	for _, task := range readyPost.tasksOfType(t, model.TaskTypeShard) {
		readyPost.finishTask(t, task, taskresult.OutcomeSuccess)
	}
	finishedPost := newStageIntegrationFixture(t, db, true, 0)
	finishedPost.savePreparation(t, []json.RawMessage{}, taskresult.OutcomeSuccess)
	if got, err := finishedPost.checker.Check(context.Background(), finishedPost.parent.ID); err != nil || got != OutcomeAdvanced {
		t.Fatalf("prepare completed Post: %d %v", got, err)
	}
	finishedPost.finishTask(t, finishedPost.tasksOfType(t, model.TaskTypePost)[0], taskresult.OutcomeSuccess)
	boundary := newStageIntegrationFixture(t, db, false, 0)
	boundary.savePreparation(t, []json.RawMessage{json.RawMessage(`1`)}, taskresult.OutcomeSuccess)
	young := newStageIntegrationFixture(t, db, false, 0)
	young.savePreparation(t, []json.RawMessage{json.RawMessage(`1`)}, taskresult.OutcomeSuccess)
	expiredGeneration := newStageIntegrationFixture(t, db, false, 0)
	expiredGeneration.savePreparation(t, []json.RawMessage{json.RawMessage(`1`)}, taskresult.OutcomeSuccess)
	expiredPost := newStageIntegrationFixture(t, db, true, 0)
	expiredPost.savePreparation(t, []json.RawMessage{}, taskresult.OutcomeSuccess)

	// Keep one actual database timestamp fixed for this traversal so the strict
	// four-minute edge is deterministic at PostgreSQL's microsecond precision.
	scanNow, err := instances.DatabaseTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		fixture *stageIntegrationFixture
		age     time.Duration
	}{
		{prepared, 4*time.Minute + time.Microsecond}, {readyPost, 5 * time.Minute},
		{finishedPost, 20 * time.Minute}, {boundary, 4 * time.Minute}, {young, 3 * time.Minute},
		{expiredGeneration, 20 * time.Minute}, {expiredPost, 20 * time.Minute},
	} {
		started := scanNow.Add(-entry.age)
		deadline := started.Add(time.Duration(entry.fixture.parent.ExecutionConfig.TimeoutSeconds) * time.Second)
		if err := db.Model(&model.JobInstance{}).Where("id = ?", entry.fixture.parent.ID).Updates(map[string]any{
			"started_at": started, "deadline_at": deadline,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	var before []model.JobTask
	if err := db.Order("id").Find(&before).Error; err != nil {
		t.Fatal(err)
	}
	observer := &recoveryCheckObserver{inner: stageIntegrationChecker(t, db), calls: make(map[int64]Outcome)}
	queue, err := NewQueue(observer, Config{Capacity: 16, Concurrency: 2, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(func() {
		queue.Stop()
		cleanup, finish := context.WithTimeout(context.Background(), 5*time.Second)
		defer finish()
		if err := queue.Wait(cleanup); err != nil {
			t.Error(err)
		}
	})
	exhausted := make(chan struct{}, 1)
	source := &runningSourceStub{
		clock: func(context.Context) (time.Time, error) { return scanNow, nil },
		list: func(ctx context.Context, bound time.Time, after *repository.RunningCandidate, limit int) ([]repository.RunningCandidate, error) {
			if !bound.Equal(scanNow.Add(-4 * time.Minute)) {
				t.Error("recovery changed the four-minute eligibility boundary")
			}
			rows, err := instances.ListRunningCandidates(ctx, bound, after, limit)
			if err == nil && len(rows) == 0 {
				select {
				case exhausted <- struct{}{}:
				default:
				}
			}
			return rows, err
		},
	}
	recovery, err := NewRecovery(source, queue, RecoveryConfig{PageSize: 2, RunTimeout: 5 * time.Second, SQLTimeout: time.Second, MinAge: 4 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	term, endTerm := context.WithCancel(ctx)
	done := make(chan error, 1)
	recoveryFinished := make(chan struct{})
	go func() { defer close(recoveryFinished); done <- recovery.Run(term, make(chan struct{})) }()
	defer func() { endTerm(); <-recoveryFinished }()
	select {
	case <-exhausted:
	case <-ctx.Done():
		t.Fatal("recovery traversal did not finish")
	}
	endTerm()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("stop recovery term: %v", err)
	}
	queue.Stop()
	if err := queue.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	checks := observer.snapshot()
	for id, want := range map[int64]Outcome{prepared.parent.ID: OutcomeAdvanced, readyPost.parent.ID: OutcomeAdvanced, finishedPost.parent.ID: OutcomeTerminal, expiredGeneration.parent.ID: OutcomeStopping, expiredPost.parent.ID: OutcomeStopping} {
		if got, ok := checks[id]; !ok || got != want {
			t.Fatalf("lost hint was not recovered: id=%d got=%d found=%v want=%d", id, got, ok, want)
		}
	}
	if len(checks) != 5 {
		t.Fatalf("young or boundary Instance entered recovery: %v", checks)
	}
	if len(prepared.tasksOfType(t, model.TaskTypeShard)) != 2 || len(readyPost.tasksOfType(t, model.TaskTypePost)) != 1 || finishedPost.loadParent(t).Status != model.InstanceSucceeded {
		t.Fatal("recovery did not resume all three durable stage gaps")
	}
	for _, f := range []*stageIntegrationFixture{boundary, young, expiredGeneration, expiredPost} {
		if len(f.tasksOfType(t, model.TaskTypeShard))+len(f.tasksOfType(t, model.TaskTypePost)) != 0 {
			t.Fatal("recovery generated new work for an excluded or expired parent")
		}
	}
	for _, original := range before {
		var current model.JobTask
		if err := db.First(&current, original.ID).Error; err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(current, original) {
			t.Fatal("recovering a missing stage repeated a completed business attempt")
		}
	}
	// The same stop component converges expired rounds; recovery only handed off
	// the stopping decision, without pretending the children were already canceled.
	for _, f := range []*stageIntegrationFixture{expiredGeneration, expiredPost} {
		assertStageStopConverges(t, f, model.InstanceFailed, model.EndReasonInstanceTimeout)
	}
}

func TestStageTransactionsInterleaveWithManualStopAndConverge(t *testing.T) {
	for _, post := range []bool{false, true} {
		for _, stageFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("Post_%v_stage_first_%v", post, stageFirst), func(t *testing.T) {
				db, _ := testutil.OpenMigratedPostgres(t)
				f := newStageIntegrationFixture(t, db, post, 0)
				items := []json.RawMessage{json.RawMessage(`null`), json.RawMessage(`{"id":9007199254740993}`)}
				if post {
					items = []json.RawMessage{}
				}
				f.savePreparation(t, items, taskresult.OutcomeSuccess)
				tasks, err := repository.NewJobTaskRepository(db)
				if err != nil {
					t.Fatal(err)
				}
				prepared, err := tasks.FindPreparationResult(context.Background(), f.parent.ID)
				if err != nil || prepared == nil {
					t.Fatalf("read committed preparation: %v", err)
				}
				before := f.loadParent(t)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				release, written := make(chan struct{}), make(chan int, 1)
				finish := sync.OnceFunc(func() { close(release) })
				defer finish()
				gated := gatedStageTransaction(db, written, release)
				stageRun, stopRun := f.run, gated
				if stageFirst {
					stageRun, stopRun = gated, f.run
				}
				var hints atomic.Int32
				generator, err := NewGenerator(stageRun, func(int64) { hints.Add(1) })
				if err != nil {
					t.Fatal(err)
				}
				creator, err := NewPostCreator(stageRun, func(int64) { hints.Add(1) })
				if err != nil {
					t.Fatal(err)
				}
				stop, err := instancestop.NewController(stopRun, instancestop.WithSQLTimeout(5*time.Second))
				if err != nil {
					t.Fatal(err)
				}
				stage := func() (bool, error) {
					if post {
						return creator.Create(ctx, f.parent.ID, repository.ShardCounts{})
					}
					return generator.Generate(ctx, f.parent.ID, *prepared)
				}
				type stageResult struct {
					created bool
					err     error
				}
				type stopResult struct {
					decision instancestop.Decision
					err      error
				}
				stageDone, stopDone := make(chan stageResult, 1), make(chan stopResult, 1)
				stageFinished, stopFinished := make(chan struct{}), make(chan struct{})
				stageStarted, stopStarted := false, false
				defer func() {
					finish()
					cancel()
					if stageStarted {
						<-stageFinished
					}
					if stopStarted {
						<-stopFinished
					}
				}()
				if stageFirst {
					stageStarted = true
					go func() { defer close(stageFinished); created, err := stage(); stageDone <- stageResult{created, err} }()
					var blocker int
					select {
					case blocker = <-written:
					case <-ctx.Done():
						t.Fatal("stage transaction never reached its uncommitted write")
					}
					stopStarted = true
					go func() {
						defer close(stopFinished)
						decision, err := stop.RequestManual(ctx, f.parent.ID)
						stopDone <- stopResult{decision, err}
					}()
					waitForStageLockWaiter(t, ctx, db, blocker)
					// Until commit, neither a new task nor its dispatch hint is visible.
					if hints.Load() != 0 || len(f.tasksOfType(t, model.TaskTypeShard))+len(f.tasksOfType(t, model.TaskTypePost)) != 0 {
						t.Fatal("uncommitted stage escaped")
					}
					finish()
					result := <-stageDone
					if result.err != nil || !result.created || hints.Load() != 1 {
						t.Fatalf("stage did not confirm its independent commit: %+v", result)
					}
				} else {
					stopStarted = true
					go func() {
						defer close(stopFinished)
						decision, err := stop.RequestManual(ctx, f.parent.ID)
						stopDone <- stopResult{decision, err}
					}()
					select {
					case <-written:
					case <-ctx.Done():
						t.Fatal("stop transaction never reached its uncommitted decision")
					}
					created, err := stage()
					if created || !errors.Is(err, repository.ErrInstanceLocked) || hints.Load() != 0 {
						t.Fatalf("stage waited for stop or ignored NOWAIT: %v %v", created, err)
					}
					finish()
				}
				stopped := <-stopDone
				if stopped.err != nil || !stopped.decision.New || stopped.decision.Status != model.InstanceStopping {
					t.Fatalf("interleaved manual stop: %+v", stopped)
				}
				// Retry with the ordinary runner, not the one-shot test gate.
				generator, _ = NewGenerator(f.run, nil)
				creator, _ = NewPostCreator(f.run, nil)
				if created, err := stage(); created || err != nil {
					t.Fatalf("stopped parent permitted stage creation: %v %v", created, err)
				}
				if got, err := f.checker.Check(ctx, f.parent.ID); err != nil || got != OutcomeStopping {
					t.Fatalf("stopped stage entered normal completion: %d %v", got, err)
				}
				assertStageStopConverges(t, f, model.InstanceCanceled, model.EndReasonManualStop)
				after := f.loadParent(t)
				if !reflect.DeepEqual(after.StartedAt, before.StartedAt) || !reflect.DeepEqual(after.DeadlineAt, before.DeadlineAt) || after.ExecutionConfig != before.ExecutionConfig {
					t.Fatal("convergence changed the frozen round")
				}
				var children []model.JobTask
				if err := db.Where("instance_id = ?", f.parent.ID).Find(&children).Error; err != nil {
					t.Fatal(err)
				}
				want := 1
				if stageFirst {
					if post {
						want++
					} else {
						want += len(items)
					}
				}
				if len(children) != want {
					t.Fatalf("interleaving lost or added stage rows: got=%d want=%d", len(children), want)
				}
				for _, child := range children {
					if child.TaskType == model.TaskTypePrepare {
						if child.Status != model.TaskSucceeded {
							t.Fatal("stop changed completed preparation")
						}
						continue
					}
					if child.Status != model.TaskCanceled || child.EndReason == nil || *child.EndReason != model.EndReasonManualStop || child.FinishedAt == nil ||
						child.AttemptNo != 0 || child.UsedAttempts != 0 || child.StartedAt != nil || child.AttemptRegisteredAt != nil || child.InputData == nil {
						t.Fatalf("new stage did not converge to cancellation: %+v", child)
					}
				}
				terminal := f.loadParent(t)
				if got, err := f.checker.Check(ctx, f.parent.ID); err != nil || got != OutcomeTerminal || !reflect.DeepEqual(f.loadParent(t), terminal) {
					t.Fatal("normal check rewrote the completed stop")
				}
			})
		}
	}
}

func TestShardedCompletionLosesToAnUncommittedManualStop(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	f := newStageIntegrationFixture(t, db, true, 0)
	f.savePreparation(t, []json.RawMessage{}, taskresult.OutcomeSuccess)
	if got, err := f.checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeAdvanced {
		t.Fatalf("create Post: %d %v", got, err)
	}
	post := f.tasksOfType(t, model.TaskTypePost)[0]
	attempt, err := f.registrar.RegisterTask(context.Background(), repository.DispatchCandidate{
		TaskID: post.ID, InstanceID: post.InstanceID, TaskType: post.TaskType, Status: post.Status, AttemptNo: post.AttemptNo, NextDispatchAt: *post.NextDispatchAt,
	})
	if err != nil || attempt == nil {
		t.Fatalf("register Post: %+v %v", attempt, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	release, written := make(chan struct{}), make(chan int, 1)
	finish := sync.OnceFunc(func() { close(release) })
	stop, err := instancestop.NewController(gatedStageTransaction(db, written, release), instancestop.WithSQLTimeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	stopDone, checkDone := make(chan struct{}), make(chan struct{})
	checkStarted := false
	defer func() {
		finish()
		cancel()
		<-stopDone
		if checkStarted {
			<-checkDone
		}
	}()
	var decision instancestop.Decision
	var stopErr, checkErr error
	var outcome Outcome
	go func() { defer close(stopDone); decision, stopErr = stop.RequestManual(ctx, f.parent.ID) }()
	var blocker int
	select {
	case blocker = <-written:
	case <-ctx.Done():
		t.Fatal("stop did not hold its uncommitted decision")
	}
	// Task result persistence is independent of the parent lock. Another check
	// can therefore observe complete final facts while the stop is uncommitted.
	if result, err := f.saver.Save(ctx, taskresult.Report{GroupID: f.parent.ExecutorGroupID, JobID: f.parent.JobID, InstanceID: f.parent.ID,
		TaskID: post.ID, AttemptNo: attempt.AttemptNo, ExecutorInstanceID: uuid.NewString(), Outcome: taskresult.OutcomeSuccess}); err != nil || !result.Applied {
		t.Fatalf("save Post during stop: %+v %v", result, err)
	}
	checkStarted = true
	go func() { defer close(checkDone); outcome, checkErr = f.checker.Check(ctx, f.parent.ID) }()
	waitForStageLockWaiter(t, ctx, db, blocker)
	finish()
	<-stopDone
	<-checkDone
	if stopErr != nil || !decision.New || decision.Status != model.InstanceStopping || checkErr != nil || outcome != OutcomeNotApplied {
		t.Fatalf("completion overwrote a winning stop: decision=%+v outcome=%d errors=%v/%v", decision, outcome, stopErr, checkErr)
	}
	assertStageStopConverges(t, f, model.InstanceCanceled, model.EndReasonManualStop)
	posts := f.tasksOfType(t, model.TaskTypePost)
	if len(posts) != 1 || posts[0].ID != post.ID || posts[0].Status != model.TaskSucceeded || posts[0].AttemptNo != 1 || !reflect.DeepEqual(posts[0].InputData, post.InputData) {
		t.Fatal("stop convergence overwrote completed Post or changed its fixed input")
	}
}

// recoveryCheckObserver delegates every actual check while recording only its
// outcome. It never emits a normal result hint or advances durable state itself.
type recoveryCheckObserver struct {
	inner *Checker
	mu    sync.Mutex
	calls map[int64]Outcome
}

func (o *recoveryCheckObserver) Check(ctx context.Context, id int64) (Outcome, error) {
	outcome, err := o.inner.Check(ctx, id)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls[id] = outcome
	return outcome, err
}
func (o *recoveryCheckObserver) snapshot() map[int64]Outcome {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make(map[int64]Outcome, len(o.calls))
	for id, outcome := range o.calls {
		result[id] = outcome
	}
	return result
}

// gatedStageTransaction uses real repositories in one default-isolation database
// transaction. Its gate delays only commit, allowing another connection to prove
// the actual lock wait. This is deterministic interleaving, not a network failure.
func gatedStageTransaction(db *gorm.DB, written chan<- int, release <-chan struct{}) repository.TransactionRunner {
	return func(ctx context.Context, fn func(repository.Repositories) error) error {
		return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			instances, err := repository.NewJobInstanceRepository(tx)
			if err != nil {
				return err
			}
			tasks, err := repository.NewJobTaskRepository(tx)
			if err != nil {
				return err
			}
			var pid int
			if err := tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
				return err
			}
			if err := fn(repository.Repositories{Instances: instances, Tasks: tasks}); err != nil {
				return err
			}
			select {
			case written <- pid:
			case <-ctx.Done():
				return ctx.Err()
			}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}
}

func waitForStageLockWaiter(t *testing.T, ctx context.Context, db *gorm.DB, blocker int) {
	t.Helper()
	for {
		var waiting bool
		if err := db.WithContext(ctx).Raw("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid)))", blocker).Scan(&waiting).Error; err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("competing stop did not wait on the stage transaction")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func assertStageStopConverges(t *testing.T, f *stageIntegrationFixture, want model.InstanceStatus, reason string) {
	t.Helper()
	controller, err := instancestop.NewController(f.run, instancestop.WithBatchSize(1))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for iteration := 0; iteration < 10; iteration++ {
		result, err := controller.ProcessBatch(ctx, f.parent.ID)
		if err != nil {
			t.Fatal(err)
		}
		if result.Completed || result.Status == want {
			parent := f.loadParent(t)
			if parent.Status != want || parent.EndReason == nil || *parent.EndReason != reason || parent.FinishedAt == nil {
				t.Fatalf("invalid final stopped parent: %+v", parent)
			}
			var unfinished int64
			if err := f.db.Model(&model.JobTask{}).Where("instance_id = ? AND status IN (1, 2, 3)", f.parent.ID).Count(&unfinished).Error; err != nil || unfinished != 0 {
				t.Fatalf("stop retained unfinished children: %d %v", unfinished, err)
			}
			return
		}
		if !result.More {
			t.Fatalf("stop made no progress: %+v", result)
		}
	}
	t.Fatal("bounded stop convergence did not finish")
}
