//go:build integration

package instancecheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/dispatch"
	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestShardedCheckCompletesZeroPartialAndAllFailedRounds(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	for _, scenario := range []struct {
		name          string
		total, failed int
		post          bool
		postFailure   bool
	}{
		{"empty", 0, 0, false, false}, {"success", 3, 0, false, false}, {"partial_failure", 3, 1, false, false}, {"all_failed", 3, 3, false, false},
		{"empty_Post", 0, 0, true, false}, {"Post_success", 3, 0, true, false}, {"failed_shards_Post_success", 3, 3, true, false}, {"Post_failure", 3, 0, true, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newStageIntegrationFixture(t, db, scenario.post, 0)
			items := make([]json.RawMessage, scenario.total)
			for i := range items {
				items[i] = json.RawMessage(fmt.Sprintf(`{"item":%d}`, i))
			}
			f.savePreparation(t, items, taskresult.OutcomeSuccess)
			if scenario.total > 0 {
				if got, err := f.checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeAdvanced {
					t.Fatalf("generate stage: %d %v", got, err)
				}
				shards := f.tasksOfType(t, model.TaskTypeShard)
				if len(shards) != scenario.total {
					t.Fatal("generation did not save complete shard set")
				}
				for i, task := range shards {
					outcome := taskresult.OutcomeSuccess
					if i < scenario.failed {
						outcome = taskresult.OutcomeFailure
					}
					f.finishTask(t, task, outcome)
				}
			}
			if scenario.post {
				if got, err := f.checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeAdvanced {
					t.Fatalf("create Post stage: %d %v", got, err)
				}
				posts := f.tasksOfType(t, model.TaskTypePost)
				if len(posts) != 1 || posts[0].Status != model.TaskWaiting || posts[0].AttemptNo != 0 || posts[0].UsedAttempts != 0 || posts[0].StartedAt != nil || posts[0].AttemptRegisteredAt != nil {
					t.Fatal("Post creation registered execution early")
				}
				var counts repository.ShardCounts
				if err := json.Unmarshal(posts[0].InputData, &counts); err != nil || counts.Total != int64(scenario.total) || counts.Failed != int64(scenario.failed) || counts.Succeeded != int64(scenario.total-scenario.failed) {
					t.Fatalf("Post input: %+v %v", counts, err)
				}
				if got, err := f.checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeNotReady {
					t.Fatalf("pending Post completed parent: %d %v", got, err)
				}
				outcome := taskresult.OutcomeSuccess
				if scenario.postFailure {
					outcome = taskresult.OutcomeFailure
				}
				f.finishTask(t, posts[0], outcome)
			}
			// Complete results remain authoritative when platform checking is delayed.
			f.expire(t)
			if got, err := f.checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeTerminal {
				t.Fatalf("complete stage: %d %v", got, err)
			}
			parent := f.loadParent(t)
			want := model.InstanceSucceeded
			if scenario.failed > 0 || scenario.postFailure {
				want = model.InstanceFailed
			}
			if parent.Status != want || parent.ResultSummary == nil || !strings.Contains(*parent.ResultSummary, fmt.Sprintf("total=%d", scenario.total)) || parent.EndReason == nil || *parent.EndReason == model.EndReasonInstanceTimeout {
				t.Fatalf("wrong final result: %+v", parent)
			}
		})
	}
}

func TestShardedPreFailureAndPostRetryKeepTheirBoundaries(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	t.Run("pre_final_failure", func(t *testing.T) {
		f := newStageIntegrationFixture(t, db, true, 0)
		f.savePreparation(t, nil, taskresult.OutcomeFailure)
		f.expire(t)
		if got, err := f.checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeTerminal {
			t.Fatalf("failed preparation: %d %v", got, err)
		}
		parent := f.loadParent(t)
		if parent.Status != model.InstanceFailed || parent.ResultSummary == nil || *parent.ResultSummary != "preparation result" || len(f.tasksOfType(t, model.TaskTypeShard))+len(f.tasksOfType(t, model.TaskTypePost)) != 0 {
			t.Fatal("failed preparation created subsequent work or changed its summary")
		}
	})
	for _, outcome := range []taskresult.Outcome{taskresult.OutcomeSuccess, taskresult.OutcomeFailure} {
		t.Run(fmt.Sprintf("post_retry_%d", outcome), func(t *testing.T) {
			f := newStageIntegrationFixture(t, db, true, 1)
			f.savePreparation(t, []json.RawMessage{}, taskresult.OutcomeSuccess)
			if got, err := f.checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeAdvanced {
				t.Fatalf("create empty Post: %d %v", got, err)
			}
			initial := f.tasksOfType(t, model.TaskTypePost)[0]
			failed := f.finishTask(t, initial, taskresult.OutcomeFailure)
			if failed.Status != model.TaskRetryWaiting || !reflect.DeepEqual(failed.InputData, initial.InputData) {
				t.Fatal("Post failure changed fixed input or bypassed retry")
			}
			if got, err := f.checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeNotReady {
				t.Fatalf("retrying Post completed parent: %d %v", got, err)
			}
			if err := db.Model(&model.JobTask{}).Where("id = ?", failed.ID).Update("next_dispatch_at", time.Now().Add(-time.Second)).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.First(&failed, failed.ID).Error; err != nil {
				t.Fatal(err)
			}
			final := f.finishTask(t, failed, outcome)
			if final.ID != initial.ID || final.AttemptNo != 2 || final.UsedAttempts != 2 || !reflect.DeepEqual(final.InputData, initial.InputData) || !reflect.DeepEqual(final.StartedAt, failed.StartedAt) {
				t.Fatal("Post retry recreated work or changed fixed facts")
			}
			if len(f.tasksOfType(t, model.TaskTypePrepare)) != 1 || len(f.tasksOfType(t, model.TaskTypeShard)) != 0 || len(f.tasksOfType(t, model.TaskTypePost)) != 1 {
				t.Fatal("Post retry replayed preparation or generated shards")
			}
			f.expire(t)
			if got, err := f.checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeTerminal {
				t.Fatalf("Post final completion: %d %v", got, err)
			}
			want := model.InstanceSucceeded
			if outcome == taskresult.OutcomeFailure {
				want = model.InstanceFailed
			}
			if f.loadParent(t).Status != want {
				t.Fatal("Post final outcome lost")
			}
		})
	}
}

func TestShardedCheckStopsExpiredMissingWorkButRejectsUnknownFacts(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	for _, scenario := range []string{"generation", "Post", "canceled", "unknown"} {
		t.Run(scenario, func(t *testing.T) {
			f := newStageIntegrationFixture(t, db, scenario == "Post", 0)
			items := []json.RawMessage{json.RawMessage(`null`)}
			if scenario == "Post" {
				items = []json.RawMessage{}
			}
			f.savePreparation(t, items, taskresult.OutcomeSuccess)
			if scenario == "canceled" || scenario == "unknown" {
				if got, err := f.checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeAdvanced {
					t.Fatalf("generate: %d %v", got, err)
				}
				state := model.TaskCanceled
				if scenario == "unknown" {
					state = 99
				}
				if err := db.Model(&model.JobTask{}).Where("instance_id = ? AND task_type = ?", f.parent.ID, model.TaskTypeShard).Update("status", state).Error; err != nil {
					t.Fatal(err)
				}
			}
			f.expire(t)
			got, err := f.checker.Check(context.Background(), f.parent.ID)
			if scenario == "unknown" {
				if !errors.Is(err, ErrInvalidFacts) || got == OutcomeStopping || f.loadParent(t).Status != model.InstanceRunning {
					t.Fatalf("unknown shard became timeout: %d %v", got, err)
				}
				return
			}
			if err != nil || got != OutcomeStopping || f.loadParent(t).Status != model.InstanceStopping {
				t.Fatalf("expired incomplete stage: %d %v", got, err)
			}
			if scenario == "generation" && len(f.tasksOfType(t, model.TaskTypeShard)) != 0 || len(f.tasksOfType(t, model.TaskTypePost)) != 0 {
				t.Fatal("expired check created new stage work")
			}
		})
	}
}

func TestPostCreationNOWAITCommitAmbiguityAndCompetingStop(t *testing.T) {
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	f := newStageIntegrationFixture(t, db, true, 0)
	f.savePreparation(t, []json.RawMessage{}, taskresult.OutcomeSuccess)
	var hints atomic.Int32
	creator, _ := NewPostCreator(f.run, func(int64) { hints.Add(1) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	holder, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback() }()
	var id int64
	if err := holder.QueryRowContext(ctx, "SELECT id FROM job_instance WHERE id = $1 FOR UPDATE", f.parent.ID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if created, err := creator.Create(ctx, f.parent.ID, repository.ShardCounts{}); created || !errors.Is(err, repository.ErrInstanceLocked) {
		t.Fatalf("Post waited on competing parent: %v %v", created, err)
	}
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	var committed atomic.Int32
	var workers sync.WaitGroup
	for range 6 {
		workers.Go(func() {
			created, err := creator.Create(ctx, f.parent.ID, repository.ShardCounts{})
			if err != nil && !errors.Is(err, repository.ErrInstanceLocked) {
				t.Errorf("concurrent Post: %v", err)
			}
			if created {
				committed.Add(1)
			}
		})
	}
	workers.Wait()
	if committed.Load() != 1 || hints.Load() != 1 || len(f.tasksOfType(t, model.TaskTypePost)) != 1 {
		t.Fatal("competing Post creation duplicated work")
	}
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown_commit_%v", commit), func(t *testing.T) {
			f := newStageIntegrationFixture(t, db, true, 0)
			f.savePreparation(t, []json.RawMessage{}, taskresult.OutcomeSuccess)
			cause := errors.New("synthetic commit confirmation failure")
			calls := 0
			creator, _ := NewPostCreator(func(ctx context.Context, fn func(repository.Repositories) error) error {
				err := f.run(ctx, func(repos repository.Repositories) error {
					if err := fn(repos); err != nil {
						return err
					}
					if !commit {
						return cause
					}
					return nil
				})
				if err != nil {
					return err
				}
				return cause
			}, func(int64) { calls++ })
			if created, err := creator.Create(ctx, f.parent.ID, repository.ShardCounts{}); created || !errors.Is(err, cause) || calls != 0 {
				t.Fatalf("unknown commit escaped: %v %v", created, err)
			}
			creator, _ = NewPostCreator(f.run, nil)
			if created, err := creator.Create(ctx, f.parent.ID, repository.ShardCounts{}); err != nil || created == commit || len(f.tasksOfType(t, model.TaskTypePost)) != 1 {
				t.Fatalf("Post retry duplicated/lost work: %v %v", created, err)
			}
		})
	}
	for _, expire := range []bool{false, true} {
		f := newStageIntegrationFixture(t, db, true, 0)
		f.savePreparation(t, []json.RawMessage{}, taskresult.OutcomeSuccess)
		if expire {
			f.expire(t)
		} else {
			controller, _ := instancestop.NewController(f.run)
			if _, err := controller.RequestManual(ctx, f.parent.ID); err != nil {
				t.Fatal(err)
			}
		}
		creator, _ := NewPostCreator(f.run, nil)
		if created, err := creator.Create(ctx, f.parent.ID, repository.ShardCounts{}); err != nil || created || len(f.tasksOfType(t, model.TaskTypePost)) != 0 {
			t.Fatalf("stopped/expired parent created Post: %v %v", created, err)
		}
	}
}

func TestShardedDatabaseQueriesAvoidRepeatedPayloadAndAggregation(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	f := newStageIntegrationFixture(t, db, true, 0)
	f.savePreparation(t, []json.RawMessage{json.RawMessage(`null`)}, taskresult.OutcomeSuccess)
	if _, err := f.checker.Check(context.Background(), f.parent.ID); err != nil {
		t.Fatal(err)
	}
	trace := &stageSQLTrace{Interface: logger.Default.LogMode(logger.Silent)}
	checker := stageIntegrationChecker(t, db.Session(&gorm.Session{Logger: trace}))
	if got, err := checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeNotReady {
		t.Fatalf("waiting shard: %d %v", got, err)
	}
	if len(trace.statements) != 5 {
		t.Fatalf("unfinished stage query count=%d SQL=%v", len(trace.statements), trace.statements)
	}
	for _, statement := range trace.statements {
		if strings.Contains(statement, "result_data") || strings.Contains(statement, "COUNT(*)") {
			t.Fatalf("unfinished stage loaded unnecessary data: %s", statement)
		}
	}
	f.finishTask(t, f.tasksOfType(t, model.TaskTypeShard)[0], taskresult.OutcomeSuccess)
	trace.statements = nil
	if got, err := checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeAdvanced {
		t.Fatalf("Post creation: %d %v", got, err)
	}
	locked := false
	aggregates := 0
	for _, statement := range trace.statements {
		if strings.Contains(statement, "FOR UPDATE NOWAIT") {
			locked = true
		}
		if strings.Contains(statement, "COUNT(*)") {
			aggregates++
			if locked {
				t.Fatal("Post creation aggregated while holding parent lock")
			}
		}
	}
	if !locked || aggregates != 1 {
		t.Fatalf("creation did not use one lock-free aggregate: %v", trace.statements)
	}
	trace.statements = nil
	if got, err := checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeNotReady {
		t.Fatalf("waiting Post: %d %v", got, err)
	}
	if len(trace.statements) != 3 {
		t.Fatalf("pending Post re-read shard facts: %v", trace.statements)
	}
	for _, statement := range trace.statements {
		if strings.Contains(statement, "result_data") || strings.Contains(statement, "COUNT(*)") || strings.Contains(statement, "task_type = 3") {
			t.Fatalf("pending Post repeated shard work: %s", statement)
		}
	}
}

type stageIntegrationFixture struct {
	db        *gorm.DB
	parent    model.JobInstance
	initial   dispatch.RegisteredAttempt
	run       repository.TransactionRunner
	checker   *Checker
	saver     *taskresult.Saver
	registrar *dispatch.Registrar
}

func newStageIntegrationFixture(t *testing.T, db *gorm.DB, post bool, retries int32) *stageIntegrationFixture {
	t.Helper()
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := repository.NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	run, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	registrar, err := dispatch.NewRegistrar(run)
	if err != nil {
		t.Fatal(err)
	}
	saver, err := taskresult.NewSaver(tasks)
	if err != nil {
		t.Fatal(err)
	}
	request, operator := uuid.New(), int64(1)
	parent := model.JobInstance{JobID: 11, JobName: "stage check fixture", ExecutorGroupID: 17, TriggerType: model.TriggerManual, RequestID: &request, TriggeredBy: &operator, ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: "test.stages", ExecutionMode: model.ExecutionModeSharded, PostEnabled: post, MaxRetries: retries, RetryIntervalSeconds: 1, TimeoutSeconds: 900}}
	if _, created, err := instances.Insert(context.Background(), &parent); err != nil || !created {
		t.Fatalf("insert parent: %v %v", created, err)
	}
	attempt, err := registrar.StartInstance(context.Background(), parent.ID)
	if err != nil || attempt == nil {
		t.Fatalf("start preparation: %+v %v", attempt, err)
	}
	return &stageIntegrationFixture{db: db, parent: parent, initial: *attempt, run: run, checker: stageIntegrationChecker(t, db), saver: saver, registrar: registrar}
}
func stageIntegrationChecker(t *testing.T, db *gorm.DB) *Checker {
	t.Helper()
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := repository.NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	run, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	generator, err := NewGenerator(run, nil)
	if err != nil {
		t.Fatal(err)
	}
	post, err := NewPostCreator(run, nil)
	if err != nil {
		t.Fatal(err)
	}
	stop, err := instancestop.NewController(run)
	if err != nil {
		t.Fatal(err)
	}
	checker, err := NewChecker(instances, WithSharding(tasks, generator, post), WithStopping(stop, func(instancestop.Decision) {}, func(int64) {}))
	if err != nil {
		t.Fatal(err)
	}
	return checker
}
func (f *stageIntegrationFixture) savePreparation(t *testing.T, items []json.RawMessage, outcome taskresult.Outcome) {
	t.Helper()
	report := taskresult.Report{GroupID: f.parent.ExecutorGroupID, JobID: f.parent.JobID, InstanceID: f.parent.ID, TaskID: f.initial.TaskID, AttemptNo: f.initial.AttemptNo, ExecutorInstanceID: uuid.NewString(), Outcome: outcome, Summary: "preparation result", PreparedItems: items}
	if result, err := f.saver.Save(context.Background(), report); err != nil || !result.Applied {
		t.Fatalf("save preparation: %+v %v", result, err)
	}
}
func (f *stageIntegrationFixture) finishTask(t *testing.T, task model.JobTask, outcome taskresult.Outcome) model.JobTask {
	t.Helper()
	if task.NextDispatchAt == nil {
		t.Fatal("task is not dispatchable")
	}
	attempt, err := f.registrar.RegisterTask(context.Background(), repository.DispatchCandidate{TaskID: task.ID, InstanceID: task.InstanceID, TaskType: task.TaskType, Status: task.Status, AttemptNo: task.AttemptNo, NextDispatchAt: *task.NextDispatchAt})
	if err != nil || attempt == nil {
		t.Fatalf("register stage task: %+v %v", attempt, err)
	}
	if result, err := f.saver.Save(context.Background(), taskresult.Report{GroupID: f.parent.ExecutorGroupID, JobID: f.parent.JobID, InstanceID: f.parent.ID, TaskID: task.ID, AttemptNo: attempt.AttemptNo, ExecutorInstanceID: uuid.NewString(), Outcome: outcome, Summary: "stage result"}); err != nil || !result.Applied {
		t.Fatalf("save stage result: %+v %v", result, err)
	}
	var stored model.JobTask
	if err := f.db.First(&stored, task.ID).Error; err != nil {
		t.Fatal(err)
	}
	return stored
}
func (f *stageIntegrationFixture) tasksOfType(t *testing.T, taskType model.TaskType) []model.JobTask {
	t.Helper()
	var tasks []model.JobTask
	if err := f.db.Where("instance_id = ? AND task_type = ?", f.parent.ID, taskType).Order("id").Find(&tasks).Error; err != nil {
		t.Fatal(err)
	}
	return tasks
}
func (f *stageIntegrationFixture) loadParent(t *testing.T) model.JobInstance {
	t.Helper()
	var parent model.JobInstance
	if err := f.db.First(&parent, f.parent.ID).Error; err != nil {
		t.Fatal(err)
	}
	return parent
}
func (f *stageIntegrationFixture) expire(t *testing.T) {
	t.Helper()
	if err := f.db.Model(&model.JobInstance{}).Where("id = ?", f.parent.ID).Updates(map[string]any{"started_at": time.Now().Add(-2 * time.Hour), "deadline_at": time.Now().Add(-time.Hour)}).Error; err != nil {
		t.Fatal(err)
	}
}

type stageSQLTrace struct {
	logger.Interface
	statements []string
}

func (s *stageSQLTrace) Trace(_ context.Context, _ time.Time, query func() (string, int64), _ error) {
	sql, _ := query()
	s.statements = append(s.statements, sql)
}
