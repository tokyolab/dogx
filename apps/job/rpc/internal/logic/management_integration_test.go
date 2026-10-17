//go:build integration

package logic

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/schedule"
	"github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/apps/job/internal/trigger"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	jobrpc "github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/bizerror"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"gorm.io/gorm"
)

func integrationPersistenceFixture(t *testing.T) (*gorm.DB, *sql.DB, *integrationUseCases, *model.JobDefinition) {
	t.Helper()
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	group := model.ExecutorGroup{Name: "offline group"}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	job := integrationValidDefinition()
	job.ExecutorGroupID, job.MisfirePolicy = group.ID, model.MisfireOnce
	repo, err := repository.NewJobDefinitionRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	id, err := repo.Create(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	job.ID = id
	transact, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	creator := integrationNewCreator(t, transact)
	s := &integrationUseCases{Creator: creator, svcCtx: &svc.ServiceContext{
		JobRepo: repo, Transactions: transact, Trigger: creator,
		Clock: func() time.Time { return integrationTestNow },
	}}
	return db, sqlDB, s, job
}

func TestAutomaticCommitFreezesSnapshotAndUpdatesAuditTime(t *testing.T) {
	db, _, s, job := integrationPersistenceFixture(t)
	ctx := context.Background()
	oldAuditTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := db.Model(&model.JobDefinition{}).Where("id = ?", job.ID).UpdateColumn("updated_at", oldAuditTime).Error; err != nil {
		t.Fatal(err)
	}
	var before model.JobDefinition
	if err := db.First(&before, job.ID).Error; err != nil {
		t.Fatal(err)
	}
	hints := 0
	s.Creator = integrationNewCreator(t, s.svcCtx.Transactions, trigger.WithInstanceHint(func(id int64) {
		hints++
		var committed int64
		if err := db.Model(&model.JobInstance{}).Where("id = ?", id).Count(&committed).Error; err != nil || committed != 1 {
			t.Errorf("hint preceded durable commit: count=%d err=%v", committed, err)
		}
	}))
	result, err := s.TriggerAutomatic(ctx, job.ID)
	if err != nil || !result.Created || hints != 1 {
		t.Fatalf("result=%+v hints=%d err=%v", result, hints, err)
	}
	var instance model.JobInstance
	if err := db.First(&instance, result.InstanceID).Error; err != nil {
		t.Fatal(err)
	}
	if instance.Status != model.InstanceWaiting || instance.StartedAt != nil || instance.DeadlineAt != nil || instance.FinishedAt != nil || instance.ScheduledAt == nil || !instance.ScheduledAt.Equal(*job.NextTriggerAt) || instance.ExecutionConfig != integrationSnapshot(job).ExecutionConfig {
		t.Fatalf("instance=%+v", instance)
	}
	var after model.JobDefinition
	if err := db.First(&after, job.ID).Error; err != nil {
		t.Fatal(err)
	}
	wantNext, err := schedule.Next(job.CronExpr, integrationTestNow)
	if err != nil {
		t.Fatal(err)
	}
	if after.NextTriggerAt == nil || !after.NextTriggerAt.Equal(wantNext) || !after.UpdatedAt.After(before.UpdatedAt) || !after.CreatedAt.Equal(before.CreatedAt) {
		t.Fatalf("progress=%+v before.updated=%v", after, before.UpdatedAt)
	}
	if err := s.UpdateJobStatus(ctx, job.ID, basemodel.RecordStatusDisabled); err != nil {
		t.Fatal(err)
	}
	job.Name, job.Params, job.ProcessorName = "changed", "new params", "new processor"
	if err := s.UpdateJob(ctx, job.ID, job); err != nil {
		t.Fatal(err)
	}
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	items, err := instances.LoadWaiting(ctx, []int64{instance.ID})
	if err != nil || len(items) != 1 || items[0].ExecutionConfig != instance.ExecutionConfig {
		t.Fatalf("old integrationSnapshot changed: %+v err=%v", items, err)
	}
	if err := s.DeleteJob(ctx, job.ID); !integrationHasSubcode(err, subcode.JobActiveInstances) {
		t.Fatalf("WAITING did not prevent deletion: %v", err)
	}
}

func TestAutomaticProgressFailureRollsBackInstance(t *testing.T) {
	db, _, s, job := integrationPersistenceFixture(t)
	// A fixture trigger forces a real PostgreSQL error after INSERT succeeds.
	// The new identity and progress must both disappear on transaction rollback.
	if err := db.Exec(`CREATE FUNCTION test_reject_progress() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test progress write failed'; END $$`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TRIGGER test_reject_progress BEFORE UPDATE OF next_trigger_at ON job_definition FOR EACH ROW EXECUTE FUNCTION test_reject_progress()`).Error; err != nil {
		t.Fatal(err)
	}
	s.Creator = integrationNewCreator(t, s.svcCtx.Transactions,
		trigger.WithInstanceHint(func(int64) { t.Error("failed transaction published hint") }))
	if result, err := s.TriggerAutomatic(context.Background(), job.ID); err == nil || result != (trigger.AutomaticResult{}) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	var count int64
	if err := db.Model(&model.JobInstance{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("rolled back instances=%d err=%v", count, err)
	}
	var stored model.JobDefinition
	if err := db.First(&stored, job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.NextTriggerAt == nil || !stored.NextTriggerAt.Equal(*job.NextTriggerAt) {
		t.Fatalf("failed progress changed=%v", stored.NextTriggerAt)
	}
}

func TestAutomaticWaitsForJobLockThenSeesCommittedActiveInstance(t *testing.T) {
	db, sqlDB, s, job := integrationPersistenceFixture(t)
	ctx := context.Background()
	writer, pid := integrationLockedJob(t, db, job.ID)
	i := integrationManualSnapshot(job, "11111111-1111-4111-8111-111111111111")
	instances, err := repository.NewJobInstanceRepository(writer)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := instances.Insert(ctx, i); err != nil {
		t.Fatal(err)
	}
	var clockCalls atomic.Int32
	s.Creator = integrationNewCreator(t, s.svcCtx.Transactions,
		trigger.WithClock(func() time.Time { clockCalls.Add(1); return integrationTestNow }))
	done := make(chan integrationAutomaticOutcome, 1)
	go func() {
		result, err := s.TriggerAutomatic(ctx, job.ID)
		done <- integrationAutomaticOutcome{result, err}
	}()
	integrationWaitForBlockedBy(t, sqlDB, pid)
	if clockCalls.Load() != 0 {
		t.Fatal("decision time was read before the Job lock")
	}
	if err := writer.Commit().Error; err != nil {
		t.Fatal(err)
	}
	out := integrationAwaitOutcome(t, done)
	if out.err != nil || out.result.Created || out.result.Reason != "blocked" || clockCalls.Load() != 1 {
		t.Fatalf("outcome=%+v clock calls=%d", out, clockCalls.Load())
	}
	var count int64
	if err := db.Model(&model.JobInstance{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("instances=%d err=%v", count, err)
	}
	var stored model.JobDefinition
	if err := db.First(&stored, job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.NextTriggerAt == nil || !stored.NextTriggerAt.After(integrationTestNow) {
		t.Fatalf("blocked trigger did not advance: %v", stored.NextTriggerAt)
	}
}

func TestConcurrentAutomaticEntrypointsCreateOnlyOneIdentity(t *testing.T) {
	_, _, s, job := integrationPersistenceFixture(t)
	other := integrationNewCreator(t, s.svcCtx.Transactions)
	start := make(chan struct{})
	done := make(chan integrationAutomaticOutcome, 2)
	for _, creator := range []*trigger.Creator{s.Creator, other} {
		go func(creator *trigger.Creator) {
			<-start
			result, err := creator.TriggerAutomatic(context.Background(), job.ID)
			done <- integrationAutomaticOutcome{result, err}
		}(creator)
	}
	close(start)
	created := 0
	for range 2 {
		out := integrationAwaitOutcome(t, done)
		if out.err != nil {
			t.Fatal(out.err)
		}
		if out.result.Created {
			created++
		} else if out.result.Reason != "not_due" {
			t.Fatalf("unexpected loser result=%+v", out.result)
		}
	}
	if created != 1 {
		t.Fatalf("created=%d", created)
	}
}

func TestConcurrentManualReplayUsesFrozenNormalizedRequest(t *testing.T) {
	db, _, s, job := integrationPersistenceFixture(t)
	request := trigger.ManualTrigger{JobID: job.ID, RequestID: uuid.MustParse("22222222-2222-4222-8222-222222222222"), OperatorID: 77, ParamsOverridden: true, Params: "  original \n"}
	done := make(chan integrationManualOutcome, 8)
	start := make(chan struct{})
	for range 8 {
		go func() {
			<-start
			id, err := s.TriggerManual(context.Background(), request)
			done <- integrationManualOutcome{id, err}
		}()
	}
	close(start)
	var first int64
	for range 8 {
		out := integrationAwaitOutcome(t, done)
		if out.err != nil {
			t.Fatal(out.err)
		}
		if first == 0 {
			first = out.id
		}
		if out.id != first {
			t.Fatalf("same request created distinct IDs %d/%d", first, out.id)
		}
	}
	if err := db.Exec("UPDATE job_definition SET params = 'edited current parameters' WHERE id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("UPDATE job_executor_group SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?", job.ExecutorGroupID).Error; err != nil {
		t.Fatal(err)
	}
	request.Params, request.OperatorID = "original", 88
	if id, err := s.TriggerManual(context.Background(), request); err != nil || id != first {
		t.Fatalf("replay id=%d err=%v", id, err)
	}
	var stored model.JobInstance
	if err := db.First(&stored, first).Error; err != nil {
		t.Fatal(err)
	}
	if stored.TriggeredBy == nil || *stored.TriggeredBy != 77 || stored.ExecutionConfig.Params != "original" {
		t.Fatalf("replay changed frozen values: %+v", stored)
	}
	request.Params = "different"
	if _, err := s.TriggerManual(context.Background(), request); !errors.Is(err, trigger.ErrRequestConflict) {
		t.Fatalf("conflicting replay err=%v", err)
	}
	if err := db.Exec("UPDATE job_definition SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	request.Params = "original"
	if _, err := s.TriggerManual(context.Background(), request); !errors.Is(err, repository.ErrJobNotFound) {
		t.Fatalf("deleted Job replay err=%v", err)
	}
}

func TestCreateAndEditWaitForGroupDeletionMark(t *testing.T) {
	for _, operation := range []string{"create", "edit"} {
		t.Run(operation, func(t *testing.T) {
			db, sqlDB, s, job := integrationPersistenceFixture(t)
			if err := s.UpdateJobStatus(context.Background(), job.ID, basemodel.RecordStatusDisabled); err != nil {
				t.Fatal(err)
			}
			target := model.ExecutorGroup{Name: "target"}
			if err := db.Create(&target).Error; err != nil {
				t.Fatal(err)
			}
			writer := integrationBeginPersistenceTransaction(t, db)
			groups, err := repository.NewExecutorGroupRepository(writer)
			if err != nil {
				t.Fatal(err)
			}
			if err := groups.LockAvailable(context.Background(), target.ID); err != nil {
				t.Fatal(err)
			}
			pid := integrationBackendPID(t, writer)
			if err := writer.Exec("UPDATE job_executor_group SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?", target.ID).Error; err != nil {
				t.Fatal(err)
			}
			input := *job
			input.ExecutorGroupID = target.ID
			done := make(chan error, 1)
			go func() {
				if operation == "create" {
					_, err := s.CreateJob(context.Background(), &input)
					done <- err
				} else {
					done <- s.UpdateJob(context.Background(), job.ID, &input)
				}
			}()
			integrationWaitForBlockedBy(t, sqlDB, pid)
			if err := writer.Commit().Error; err != nil {
				t.Fatal(err)
			}
			if err := integrationAwaitOutcome(t, done); !integrationHasSubcode(err, subcode.JobExecutorGroupUnavailable) {
				t.Fatalf("write crossed group deletion: %v", err)
			}
			var count int64
			if err := db.Model(&model.JobDefinition{}).Where("executor_group_id = ?", target.ID).Count(&count).Error; err != nil || count != 0 {
				t.Fatalf("references=%d err=%v", count, err)
			}
		})
	}
}

func TestEnableAndEditSerializeOnCurrentConfiguration(t *testing.T) {
	for _, first := range []string{"enable", "edit"} {
		t.Run(first, func(t *testing.T) {
			db, sqlDB, s, job := integrationPersistenceFixture(t)
			ctx := context.Background()
			if err := s.UpdateJobStatus(ctx, job.ID, basemodel.RecordStatusDisabled); err != nil {
				t.Fatal(err)
			}
			writer := integrationBeginPersistenceTransaction(t, db)
			r := integrationRepositories(t, writer)
			updated := *job
			updated.CronExpr, updated.Params = "30 * * * * *", "edited"
			if first == "edit" {
				if err := r.Groups.LockAvailable(ctx, updated.ExecutorGroupID); err != nil {
					t.Fatal(err)
				}
				if err := r.Jobs.Update(ctx, job.ID, &updated); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := r.Jobs.Lock(ctx, job.ID); err != nil {
					t.Fatal(err)
				}
				next, err := schedule.Next(job.CronExpr, integrationTestNow)
				if err != nil {
					t.Fatal(err)
				}
				if err := r.Jobs.Enable(ctx, job.ID, next); err != nil {
					t.Fatal(err)
				}
			}
			pid := integrationBackendPID(t, writer)
			done := make(chan error, 1)
			go func() {
				if first == "edit" {
					done <- s.UpdateJobStatus(ctx, job.ID, basemodel.RecordStatusEnabled)
				} else {
					done <- s.UpdateJob(ctx, job.ID, &updated)
				}
			}()
			integrationWaitForBlockedBy(t, sqlDB, pid)
			if err := writer.Commit().Error; err != nil {
				t.Fatal(err)
			}
			err := integrationAwaitOutcome(t, done)
			if first == "enable" {
				if !integrationHasSubcode(err, subcode.JobStateNotAllowed) {
					t.Fatalf("enabled edit err=%v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			var stored model.JobDefinition
			if err := db.First(&stored, job.ID).Error; err != nil {
				t.Fatal(err)
			}
			wantCron := job.CronExpr
			if first == "edit" {
				wantCron = updated.CronExpr
			}
			wantNext, err := schedule.Next(wantCron, integrationTestNow)
			if err != nil {
				t.Fatal(err)
			}
			if stored.CronExpr != wantCron || stored.Status != basemodel.RecordStatusEnabled || stored.NextTriggerAt == nil || !stored.NextTriggerAt.Equal(wantNext) {
				t.Fatalf("mixed configuration/progress: %+v", stored)
			}
		})
	}
}

func TestDeleteSeesActiveInstanceCommittedByPriorLockHolder(t *testing.T) {
	db, sqlDB, s, job := integrationPersistenceFixture(t)
	ctx := context.Background()
	if err := s.UpdateJobStatus(ctx, job.ID, basemodel.RecordStatusDisabled); err != nil {
		t.Fatal(err)
	}
	writer, pid := integrationLockedJob(t, db, job.ID)
	instances, err := repository.NewJobInstanceRepository(writer)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := instances.Insert(ctx, integrationManualSnapshot(job, "33333333-3333-4333-8333-333333333333")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.DeleteJob(ctx, job.ID) }()
	integrationWaitForBlockedBy(t, sqlDB, pid)
	if err := writer.Commit().Error; err != nil {
		t.Fatal(err)
	}
	if err := integrationAwaitOutcome(t, done); !integrationHasSubcode(err, subcode.JobActiveInstances) {
		t.Fatalf("delete ignored committed active instance: %v", err)
	}
}

func TestWaitingScanRediscoversLowerIDCommittedLater(t *testing.T) {
	db, _, _, job := integrationPersistenceFixture(t)
	ctx := context.Background()
	late := integrationBeginPersistenceTransaction(t, db)
	lateInstances, err := repository.NewJobInstanceRepository(late)
	if err != nil {
		t.Fatal(err)
	}
	low, _, err := lateInstances.Insert(ctx, integrationManualSnapshot(job, "44444444-4444-4444-8444-444444444444"))
	if err != nil {
		t.Fatal(err)
	}
	repo, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	high, _, err := repo.Insert(ctx, integrationManualSnapshot(job, "55555555-5555-4555-8555-555555555555"))
	if err != nil || high <= low {
		t.Fatalf("low=%d high=%d err=%v", low, high, err)
	}
	ids, err := repo.ListWaitingIDs(ctx, 0, 1024)
	if err != nil || !reflect.DeepEqual(ids, []int64{high}) {
		t.Fatalf("before late commit=%v err=%v", ids, err)
	}
	if err := late.Commit().Error; err != nil {
		t.Fatal(err)
	}
	if ids, err := repo.ListWaitingIDs(ctx, high, 1024); err != nil || len(ids) != 0 {
		t.Fatalf("temporary high cursor=%v err=%v", ids, err)
	}
	ids, err = repo.ListWaitingIDs(ctx, 0, 1024)
	if err != nil || !reflect.DeepEqual(ids, []int64{low, high}) {
		t.Fatalf("recovery did not rediscover late commit=%v err=%v", ids, err)
	}
}

type integrationAutomaticOutcome struct {
	result trigger.AutomaticResult
	err    error
}

func TestManualAndAutomaticCreationShareJobBlockingDecision(t *testing.T) {
	db, _, s, job := integrationPersistenceFixture(t)
	start := make(chan struct{})
	automatic := make(chan integrationAutomaticOutcome, 1)
	manual := make(chan integrationManualOutcome, 1)
	go func() {
		<-start
		result, err := s.TriggerAutomatic(context.Background(), job.ID)
		automatic <- integrationAutomaticOutcome{result, err}
	}()
	go func() {
		<-start
		id, err := s.TriggerManual(context.Background(), trigger.ManualTrigger{JobID: job.ID, RequestID: uuid.MustParse("66666666-6666-4666-8666-666666666666"), OperatorID: 42})
		manual <- integrationManualOutcome{id, err}
	}()
	close(start)
	autoResult, manualResult := integrationAwaitOutcome(t, automatic), integrationAwaitOutcome(t, manual)
	if autoResult.err != nil {
		t.Fatal(autoResult.err)
	}
	if autoResult.result.Created {
		if !errors.Is(manualResult.err, trigger.ErrBlocked) || manualResult.id != 0 {
			t.Fatalf("automatic creation did not block manual: %+v", manualResult)
		}
	} else if autoResult.result.Reason != "blocked" || manualResult.err != nil || manualResult.id <= 0 {
		t.Fatalf("manual creation did not block automatic: auto=%+v manual=%+v", autoResult, manualResult)
	}
	var count int64
	if err := db.Model(&model.JobInstance{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("competing sources created %d instances: %v", count, err)
	}
}

func TestGroupReferenceCheckAfterLockSeesNewlyCommittedJob(t *testing.T) {
	db, sqlDB, _, job := integrationPersistenceFixture(t)
	ctx := context.Background()
	group := model.ExecutorGroup{Name: "new target"}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	writer := integrationBeginPersistenceTransaction(t, db)
	groups, err := repository.NewExecutorGroupRepository(writer)
	if err != nil {
		t.Fatal(err)
	}
	if err := groups.LockAvailable(ctx, group.ID); err != nil {
		t.Fatal(err)
	}
	pid := integrationBackendPID(t, writer)
	job.ExecutorGroupID = group.ID
	jobs, err := repository.NewJobDefinitionRepository(writer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jobs.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	type referenceOutcome struct {
		referenced bool
		err        error
	}
	done := make(chan referenceOutcome, 1)
	go func() {
		var referenced bool
		err := db.Transaction(func(tx *gorm.DB) error {
			groups, err := repository.NewExecutorGroupRepository(tx)
			if err != nil {
				return err
			}
			if err := groups.LockAvailable(ctx, group.ID); err != nil {
				return err
			}
			// This only verifies the future deletion protocol's first gate.
			// Task convergence and credentials remain outside this module.
			return tx.Raw("SELECT EXISTS (SELECT 1 FROM job_definition WHERE executor_group_id = ? AND deleted_at IS NULL)", group.ID).Scan(&referenced).Error
		})
		done <- referenceOutcome{referenced, err}
	}()
	integrationWaitForBlockedBy(t, sqlDB, pid)
	if err := writer.Commit().Error; err != nil {
		t.Fatal(err)
	}
	out := integrationAwaitOutcome(t, done)
	if out.err != nil || !out.referenced {
		t.Fatalf("post-lock reference check missed committed Job: %+v", out)
	}
}

type integrationManualOutcome struct {
	id  int64
	err error
}

func integrationAwaitOutcome[T any](t *testing.T, done <-chan T) T {
	t.Helper()
	select {
	case out := <-done:
		return out
	case <-time.After(12 * time.Second):
		t.Fatal("database operation did not finish")
	}
	var zero T
	return zero
}

func integrationBeginPersistenceTransaction(t *testing.T, db *gorm.DB) *gorm.DB {
	t.Helper()
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	t.Cleanup(func() { tx.Rollback() })
	return tx
}

func integrationLockedJob(t *testing.T, db *gorm.DB, id int64) (*gorm.DB, int) {
	t.Helper()
	tx := integrationBeginPersistenceTransaction(t, db)
	repo, err := repository.NewJobDefinitionRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Lock(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	return tx, integrationBackendPID(t, tx)
}

func integrationBackendPID(t *testing.T, tx *gorm.DB) int {
	t.Helper()
	var pid int
	if err := tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
		t.Fatal(err)
	}
	return pid
}

func integrationWaitForBlockedBy(t *testing.T, db *sql.DB, pid int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked); err != nil {
			t.Fatalf("observe row-lock wait: %v", err)
		}
		if blocked {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("competing transaction never waited for the expected row lock")
		}
	}
}

func integrationManualSnapshot(job *model.JobDefinition, requestID string) *model.JobInstance {
	i := integrationSnapshot(job)
	request, operator := uuid.MustParse(requestID), int64(42)
	i.TriggerType, i.RequestID, i.TriggeredBy = model.TriggerManual, &request, &operator
	return i
}

var integrationTestNow = time.Date(2026, 10, 16, 12, 0, 5, 0, time.Local)

func integrationValidDefinition() *model.JobDefinition {
	due := integrationTestNow.Add(-5 * time.Second)
	return &model.JobDefinition{
		Name: "job", ExecutorGroupID: 2, ProcessorName: "processor",
		ExecutionMode: model.ExecutionModeNormal, Params: "current", CronExpr: "0 * * * * *",
		Status: basemodel.RecordStatusEnabled, MisfirePolicy: model.MisfireIgnore, BlockPolicy: model.BlockDiscard,
		MaxRetries: 3, RetryIntervalSeconds: 10, TimeoutSeconds: 900, NextTriggerAt: &due,
	}
}

// This test-only adapter keeps concurrent scenarios readable while exercising
// each real management Logic, not a second implementation of its transactions.
type integrationUseCases struct {
	*trigger.Creator
	svcCtx *svc.ServiceContext
}

func (s *integrationUseCases) CreateJob(ctx context.Context, definition *model.JobDefinition) (int64, error) {
	state := int32(definition.Status)
	result, err := NewCreateJobLogic(ctx, s.svcCtx).CreateJob(&jobrpc.CreateJobRequest{
		Config: integrationJobInput(definition), Status: &state,
	})
	if err != nil {
		return 0, err
	}
	return result.Id, nil
}

func (s *integrationUseCases) UpdateJob(ctx context.Context, id int64, definition *model.JobDefinition) error {
	_, err := NewUpdateJobLogic(ctx, s.svcCtx).UpdateJob(&jobrpc.UpdateJobRequest{
		Id: id, Config: integrationJobInput(definition),
	})
	return err
}

func (s *integrationUseCases) UpdateJobStatus(ctx context.Context, id int64, state basemodel.RecordStatus) error {
	value := int32(state)
	_, err := NewUpdateJobStatusLogic(ctx, s.svcCtx).UpdateJobStatus(&jobrpc.UpdateJobStatusRequest{Id: id, Status: &value})
	return err
}

func (s *integrationUseCases) DeleteJob(ctx context.Context, id int64) error {
	_, err := NewDeleteJobLogic(ctx, s.svcCtx).DeleteJob(&jobrpc.DeleteJobRequest{Id: id})
	return err
}

func integrationJobInput(definition *model.JobDefinition) *jobrpc.JobInput {
	execution, misfire, block := int32(definition.ExecutionMode), int32(definition.MisfirePolicy), int32(definition.BlockPolicy)
	return &jobrpc.JobInput{
		Name: definition.Name, Remark: definition.Remark, ExecutorGroupId: definition.ExecutorGroupID,
		ProcessorName: definition.ProcessorName, ExecutionMode: &execution, PostEnabled: &definition.PostEnabled,
		Params: definition.Params, CronExpr: definition.CronExpr, MisfirePolicy: &misfire, BlockPolicy: &block,
		MaxRetries: &definition.MaxRetries, RetryIntervalSeconds: &definition.RetryIntervalSeconds,
		TimeoutSeconds: definition.TimeoutSeconds,
	}
}

func integrationNewCreator(t *testing.T, transact repository.TransactionRunner, options ...trigger.Option) *trigger.Creator {
	t.Helper()
	defaults := []trigger.Option{
		trigger.WithClock(func() time.Time { return integrationTestNow }),
		trigger.WithCreationLimits(32, 10*time.Second),
	}
	creator, err := trigger.New(transact, append(defaults, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	return creator
}

func integrationRepositories(t *testing.T, db *gorm.DB) repository.Repositories {
	t.Helper()
	jobs, err := repository.NewJobDefinitionRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	groups, err := repository.NewExecutorGroupRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	return repository.Repositories{Jobs: jobs, Groups: groups, Instances: instances}
}

func integrationHasSubcode(err error, want string) bool {
	business, ok := bizerror.From(err)
	return ok && business.Subcode() == want
}

func integrationSnapshot(definition *model.JobDefinition) *model.JobInstance {
	return &model.JobInstance{
		JobID: definition.ID, JobName: definition.Name, ExecutorGroupID: definition.ExecutorGroupID,
		Status: model.InstanceWaiting,
		ExecutionConfig: model.ExecutionConfig{
			SchemaVersion: 1, ProcessorName: definition.ProcessorName, ExecutionMode: definition.ExecutionMode,
			PostEnabled: definition.PostEnabled, Params: definition.Params, MaxRetries: definition.MaxRetries,
			RetryIntervalSeconds: definition.RetryIntervalSeconds, TimeoutSeconds: definition.TimeoutSeconds,
		},
	}
}
