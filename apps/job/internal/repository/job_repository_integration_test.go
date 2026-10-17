//go:build integration

package repository

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func repositoryFixture(t *testing.T) (*gorm.DB, *model.JobDefinition) {
	t.Helper()
	db, _ := testutil.OpenMigratedPostgres(t)
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	t.Cleanup(func() { tx.Rollback() })
	group := model.ExecutorGroup{Name: "offline group"}
	if err := tx.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	due := time.Date(2026, 10, 16, 12, 0, 0, 123456000, time.UTC)
	job := &model.JobDefinition{Name: "job", ExecutorGroupID: group.ID, ProcessorName: "Processor", ExecutionMode: model.ExecutionModeNormal,
		CronExpr: "0 * * * * *", Status: basemodel.RecordStatusDisabled, RetryIntervalSeconds: 10, TimeoutSeconds: 900, NextTriggerAt: &due}
	repo, err := NewJobDefinitionRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := repo.Create(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	job.ID = id
	return tx, job
}

func TestJobRepositoryListLiteralFiltersAndMissingGroup(t *testing.T) {
	db, job := repositoryFixture(t)
	ctx := context.Background()
	repo, err := NewJobDefinitionRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	job.Name = "50%_Done! 中文"
	if err := repo.Update(ctx, job.ID, job); err != nil {
		t.Fatal(err)
	}
	other := *job
	other.Name = "50XXdone!"
	if _, err := repo.Create(ctx, &other); err != nil {
		t.Fatal(err)
	}
	// The missing group remains observable as NULL without dropping its Job.
	if err := db.Exec("UPDATE job_executor_group SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?", job.ExecutorGroupID).Error; err != nil {
		t.Fatal(err)
	}
	trace := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err = NewJobDefinitionRepository(db.Session(&gorm.Session{Logger: trace}))
	if err != nil {
		t.Fatal(err)
	}
	status := basemodel.RecordStatusDisabled
	items, total, err := repo.List(ctx, JobFilter{Page: 1, PageSize: 20, ID: &job.ID, Name: "%_done!", ExecutorGroupID: &job.ExecutorGroupID, Status: &status})
	if err != nil || total != 1 || len(items) != 1 {
		t.Fatalf("items=%+v total=%d err=%v", items, total, err)
	}
	if trace.count != 2 {
		t.Fatalf("list queries=%d want=2", trace.count)
	}
	if items[0].ExecutorGroupName != nil || items[0].NextTriggerAt != nil {
		t.Fatalf("disabled/missing projection=%+v", items[0])
	}
	for _, sql := range trace.statements {
		if strings.Contains(sql, "j.params") || strings.Contains(sql, "j.remark") {
			t.Fatalf("list loaded large fields: %s", sql)
		}
	}
	detail, err := repo.Get(ctx, job.ID)
	if err != nil || detail.Name != job.Name || detail.ExecutorGroupName != nil || detail.NextTriggerAt != nil {
		t.Fatalf("detail=%+v err=%v", detail, err)
	}
	items, total, err = repo.List(ctx, JobFilter{Page: 1, PageSize: 20})
	if err != nil || total != 2 || len(items) != 2 || items[0].ID <= items[1].ID {
		t.Fatalf("ordering=%+v total=%d err=%v", items, total, err)
	}
	items, total, err = repo.List(ctx, JobFilter{Page: 3, PageSize: 20})
	if err != nil || items == nil || len(items) != 0 || total != 2 {
		t.Fatalf("empty page=%+v total=%d err=%v", items, total, err)
	}
}

func TestJobRepositoryWritesZeroValuesAndPreservesProgress(t *testing.T) {
	db, job := repositoryFixture(t)
	ctx := context.Background()
	repo, err := NewJobDefinitionRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	job.ExecutionMode, job.PostEnabled, job.MaxRetries = model.ExecutionModeSharded, true, 5
	job.Params, job.Remark = "payload", "remark"
	if err := repo.Update(ctx, job.ID, job); err != nil {
		t.Fatal(err)
	}
	job.PostEnabled, job.MaxRetries, job.Params, job.Remark = false, 0, "", ""
	job.Status, job.NextTriggerAt = basemodel.RecordStatusEnabled, nil
	if err := repo.Update(ctx, job.ID, job); err != nil {
		t.Fatal(err)
	}
	var stored model.JobDefinition
	if err := db.First(&stored, job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.PostEnabled || stored.MaxRetries != 0 || stored.Params != "" || stored.Remark != "" || stored.Status != basemodel.RecordStatusDisabled || stored.NextTriggerAt == nil {
		t.Fatalf("full update changed protected columns or lost zero values: %+v", stored)
	}
	next := stored.NextTriggerAt.Add(time.Hour)
	if err := repo.Enable(ctx, job.ID, next); err != nil {
		t.Fatal(err)
	}
	if err := repo.Update(ctx, job.ID, job); !errors.Is(err, ErrJobStateNotAllowed) {
		t.Fatalf("enabled update err=%v", err)
	}
	if err := repo.Delete(ctx, job.ID); !errors.Is(err, ErrJobStateNotAllowed) {
		t.Fatalf("enabled delete err=%v", err)
	}
	if err := repo.Disable(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	locked, err := repo.Lock(ctx, job.ID)
	if err != nil || locked.NextTriggerAt == nil || !locked.NextTriggerAt.Equal(next) {
		t.Fatalf("disabled progress=%+v err=%v", locked, err)
	}
	if err := repo.Advance(ctx, job.ID, next.Add(time.Minute)); !errors.Is(err, ErrJobStateNotAllowed) {
		t.Fatalf("disabled progress update err=%v", err)
	}
	if err := repo.Delete(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	var deleted model.JobDefinition
	if err := db.Unscoped().First(&deleted, job.ID).Error; err != nil {
		t.Fatalf("soft delete removed the physical row: %v", err)
	}
	if !deleted.DeletedAt.Valid || !deleted.CreatedAt.Equal(stored.CreatedAt) || deleted.Status != basemodel.RecordStatusDisabled {
		t.Fatalf("soft delete changed business fields or lost its deletion marker: %+v", deleted)
	}
	if err := db.First(&model.JobDefinition{}, job.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("GORM default scope included deleted Job: %v", err)
	}
	if err := repo.Delete(ctx, job.ID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("repeated delete err=%v", err)
	}
	if _, err := repo.Get(ctx, job.ID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("deleted get err=%v", err)
	}
	if _, err := repo.Lock(ctx, job.ID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("deleted lock err=%v", err)
	}
	if err := repo.Disable(ctx, job.ID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("deleted disable err=%v", err)
	}
	if err := repo.Update(ctx, job.ID+100, job); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("missing update err=%v", err)
	}
}

func TestJobRepositoryCreateDoesNotReuseIdentityOrAuditFields(t *testing.T) {
	db, job := repositoryFixture(t)
	ctx := context.Background()
	repo, err := NewJobDefinitionRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	originalID := job.ID
	oldAuditTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	job.CreatedAt, job.UpdatedAt = oldAuditTime, oldAuditTime
	job.DeletedAt = gorm.DeletedAt{Time: oldAuditTime, Valid: true}
	previousID := originalID
	for range 2 {
		id, err := repo.Create(ctx, job)
		if err != nil || id <= 0 || id == originalID || id == previousID {
			t.Fatalf("reused model create id=%d previous=%d original=%d err=%v", id, previousID, originalID, err)
		}
		var stored model.JobDefinition
		if err := db.First(&stored, id).Error; err != nil {
			t.Fatalf("new Job is not visible in the default scope: %v", err)
		}
		if stored.DeletedAt.Valid || !stored.CreatedAt.After(oldAuditTime) || !stored.UpdatedAt.After(oldAuditTime) {
			t.Fatalf("new Job reused audit fields: %+v", stored.Base)
		}
		if stored.Name != job.Name || stored.ExecutorGroupID != job.ExecutorGroupID || stored.NextTriggerAt == nil || !stored.NextTriggerAt.Equal(*job.NextTriggerAt) {
			t.Fatalf("new Job lost supplied configuration: %+v", stored)
		}
		previousID = id
	}
	var count int64
	if err := db.Model(&model.JobDefinition{}).Count(&count).Error; err != nil || count != 3 {
		t.Fatalf("repeat create overwrote an existing Job: count=%d err=%v", count, err)
	}
}

func TestJobRepositoryReadsExcludeDeletedEnabledJobs(t *testing.T) {
	db, job := repositoryFixture(t)
	ctx := context.Background()
	repo, err := NewJobDefinitionRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Enable(ctx, job.ID, *job.NextTriggerAt); err != nil {
		t.Fatal(err)
	}
	deleted := *job
	deleted.Status = basemodel.RecordStatusEnabled
	deletedID, err := repo.Create(ctx, &deleted)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the deleted row enabled and due so status or time filters cannot mask
	// a missing soft-delete scope in the scheduler's configuration queries.
	if err := db.Delete(&model.JobDefinition{}, deletedID).Error; err != nil {
		t.Fatal(err)
	}
	items, total, err := repo.List(ctx, JobFilter{Page: 1, PageSize: 20})
	if err != nil || total != 1 || len(items) != 1 || items[0].ID != job.ID {
		t.Fatalf("list includes a deleted Job: items=%+v total=%d err=%v", items, total, err)
	}
	if _, err := repo.Get(ctx, deletedID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("deleted detail err=%v", err)
	}
	rules, err := repo.LoadRules(ctx)
	if err != nil || len(rules) != 1 || rules[0].ID != job.ID {
		t.Fatalf("rules include a deleted Job: %+v err=%v", rules, err)
	}
	due, err := repo.ListDue(ctx, job.NextTriggerAt.Add(time.Second), 20)
	if err != nil || len(due) != 1 || due[0].ID != job.ID {
		t.Fatalf("due candidates include a deleted Job: %+v err=%v", due, err)
	}
}

func TestJobRepositoryPropagatesCanceledContext(t *testing.T) {
	db, job := repositoryFixture(t)
	repo, err := NewJobDefinitionRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"list", func() error { _, _, err := repo.List(ctx, JobFilter{Page: 1, PageSize: 20}); return err }},
		{"get", func() error { _, err := repo.Get(ctx, job.ID); return err }},
		{"lock", func() error { _, err := repo.Lock(ctx, job.ID); return err }},
		{"create", func() error { _, err := repo.Create(ctx, job); return err }},
		{"update", func() error { return repo.Update(ctx, job.ID, job) }},
		{"enable", func() error { return repo.Enable(ctx, job.ID, *job.NextTriggerAt) }},
		{"disable", func() error { return repo.Disable(ctx, job.ID) }},
		{"delete", func() error { return repo.Delete(ctx, job.ID) }},
		{"advance", func() error { return repo.Advance(ctx, job.ID, *job.NextTriggerAt) }},
		{"rules", func() error { _, err := repo.LoadRules(ctx); return err }},
		{"due", func() error { _, err := repo.ListDue(ctx, time.Now(), 20); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled operation err=%v", err)
			}
		})
	}
}

func TestJobRepositoryRulesAreCompleteAndDueCandidatesBounded(t *testing.T) {
	db, job := repositoryFixture(t)
	ctx := context.Background()
	if err := db.Exec(`INSERT INTO job_definition (name, remark, executor_group_id, processor_name, execution_mode, post_enabled, params, cron_expr, status, misfire_policy, block_policy, max_retries, retry_interval_seconds, timeout_seconds, next_trigger_at)
	SELECT 'rule-' || n, '', ?, 'p', 1, FALSE, repeat('large payload', 100), '0 * * * * *', 1, 0, 0, 0, 10, 900, ?::timestamptz + n * INTERVAL '1 microsecond' FROM generate_series(1, 1030) n`, job.ExecutorGroupID, job.NextTriggerAt).Error; err != nil {
		t.Fatal(err)
	}
	repo, err := NewJobDefinitionRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	rules, err := repo.LoadRules(ctx)
	if err != nil || len(rules) != 1030 {
		t.Fatalf("rules=%d err=%v", len(rules), err)
	}
	due, err := repo.ListDue(ctx, job.NextTriggerAt.Add(time.Second), 1024)
	if err != nil || len(due) != 1024 {
		t.Fatalf("due=%d err=%v", len(due), err)
	}
	for i := 1; i < len(due); i++ {
		if !due[i].NextTriggerAt.After(due[i-1].NextTriggerAt) {
			t.Fatalf("unordered due batch at %d", i)
		}
	}
	if larger, err := repo.ListDue(ctx, job.NextTriggerAt.Add(time.Second), 1030); err != nil || len(larger) != 1030 {
		t.Fatalf("deployment recovery limit was truncated: count=%d err=%v", len(larger), err)
	}
	if _, err := repo.ListDue(ctx, time.Now(), 0); err == nil {
		t.Fatal("accepted nonpositive recovery limit")
	}
}

func TestInstanceRepositoryIdentityAndSnapshot(t *testing.T) {
	db, job := repositoryFixture(t)
	ctx := context.Background()
	repo, err := NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	instance := repositoryInstance(job)
	id, created, err := repo.Insert(ctx, instance)
	if err != nil || !created || id <= 0 {
		t.Fatalf("insert id=%d created=%v err=%v", id, created, err)
	}
	instance.TriggerType = model.TriggerMisfire
	if duplicate, created, err := repo.Insert(ctx, instance); err != nil || created || duplicate != 0 {
		t.Fatalf("automatic cross-type duplicate id=%d created=%v err=%v", duplicate, created, err)
	}
	request := uuid.MustParse("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
	operator := int64(42)
	instance.TriggerType, instance.ScheduledAt, instance.RequestID, instance.TriggeredBy = model.TriggerManual, nil, &request, &operator
	instance.ParamsOverridden, instance.ExecutionConfig.Params = true, ""
	manualID, created, err := repo.Insert(ctx, instance)
	if err != nil || !created {
		t.Fatalf("manual created=%v err=%v", created, err)
	}
	if _, created, err := repo.Insert(ctx, instance); err != nil || created {
		t.Fatalf("manual duplicate created=%v err=%v", created, err)
	}
	prior, err := repo.FindManual(ctx, job.ID, request)
	if err != nil || prior == nil || prior.ID != manualID || prior.OverrideParams == nil || *prior.OverrideParams != "" {
		t.Fatalf("manual identity=%+v err=%v", prior, err)
	}
	var stored model.JobInstance
	if err := db.First(&stored, manualID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.InstanceWaiting || stored.ExecutionConfig != instance.ExecutionConfig || stored.StartedAt != nil || stored.DeadlineAt != nil || stored.FinishedAt != nil || stored.EndReason != nil || stored.ResultSummary != nil {
		t.Fatalf("waiting snapshot=%+v", stored)
	}
	ids, err := repo.ListWaitingIDs(ctx, 0, 1024)
	if err != nil || !reflect.DeepEqual(ids, []int64{id, manualID}) {
		t.Fatalf("waiting ids=%v err=%v", ids, err)
	}
	items, err := repo.LoadWaiting(ctx, ids)
	if err != nil || len(items) != 2 {
		t.Fatalf("waiting snapshots=%+v err=%v", items, err)
	}
	if err := db.Exec("UPDATE job_instance SET status = 3 WHERE job_id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if active, err := repo.HasActive(ctx, job.ID); err != nil || active {
		t.Fatalf("terminal active=%v err=%v", active, err)
	}
	if items, err := repo.LoadWaiting(ctx, ids); err != nil || len(items) != 0 {
		t.Fatalf("terminal loaded=%v err=%v", items, err)
	}
}

func TestInstanceRepositoryDoesNotSwallowOtherConstraints(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		mutate     func(*model.JobInstance)
	}{
		{"identity shape", "23514", func(i *model.JobInstance) { i.ParamsOverridden = true }},
		{"snapshot name capacity", "22001", func(i *model.JobInstance) { i.JobName = strings.Repeat("界", 129) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, job := repositoryFixture(t)
			i := repositoryInstance(job)
			tc.mutate(i)
			repo, err := NewJobInstanceRepository(db)
			if err != nil {
				t.Fatal(err)
			}
			_, created, err := repo.Insert(context.Background(), i)
			var pgErr *pgconn.PgError
			if created || !errors.As(err, &pgErr) || pgErr.Code != tc.code {
				t.Fatalf("created=%v err=%v want SQLSTATE %s", created, err, tc.code)
			}
		})
	}
	t.Run("unrelated unique constraint", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			trigger model.TriggerType
		}{
			{"scheduled", model.TriggerScheduled},
			{"misfire", model.TriggerMisfire},
			{"manual", model.TriggerManual},
		} {
			t.Run(tc.name, func(t *testing.T) {
				db, job := repositoryFixture(t)
				// This fixture-only constraint tests that INSERT ignores only its
				// identity target, rather than suppressing every uniqueness violation.
				if err := db.Exec("CREATE UNIQUE INDEX test_instance_name_unique ON job_instance (job_name)").Error; err != nil {
					t.Fatal(err)
				}
				repo, err := NewJobInstanceRepository(db)
				if err != nil {
					t.Fatal(err)
				}
				i := repositoryInstance(job)
				i.TriggerType = tc.trigger
				if tc.trigger == model.TriggerManual {
					request := uuid.MustParse("11111111-1111-4111-8111-111111111111")
					operator := int64(42)
					i.ScheduledAt, i.RequestID, i.TriggeredBy = nil, &request, &operator
				}
				if _, _, err := repo.Insert(context.Background(), i); err != nil {
					t.Fatal(err)
				}
				if tc.trigger == model.TriggerManual {
					request := uuid.MustParse("22222222-2222-4222-8222-222222222222")
					i.RequestID = &request
				} else {
					next := i.ScheduledAt.Add(time.Minute)
					i.ScheduledAt = &next
				}
				_, created, err := repo.Insert(context.Background(), i)
				var pgErr *pgconn.PgError
				if created || !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "test_instance_name_unique" {
					t.Fatalf("created=%v err=%v", created, err)
				}
			})
		}
	})
}

func TestExecutorGroupRepositoryRejectsDeletedAndMissingGroups(t *testing.T) {
	db, job := repositoryFixture(t)
	repo, err := NewExecutorGroupRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := repo.LockAvailable(ctx, job.ExecutorGroupID); err != nil {
		t.Fatal(err)
	}
	if ok, err := repo.Available(ctx, job.ExecutorGroupID); err != nil || !ok {
		t.Fatalf("available=%v err=%v", ok, err)
	}
	if err := db.Exec("UPDATE job_executor_group SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?", job.ExecutorGroupID).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{job.ExecutorGroupID, job.ExecutorGroupID + 100} {
		if err := repo.LockAvailable(ctx, id); !errors.Is(err, ErrExecutorGroupUnavailable) {
			t.Fatalf("id=%d lock err=%v", id, err)
		}
		if ok, err := repo.Available(ctx, id); err != nil || ok {
			t.Fatalf("id=%d available=%v err=%v", id, ok, err)
		}
	}
}

func repositoryInstance(job *model.JobDefinition) *model.JobInstance {
	return &model.JobInstance{JobID: job.ID, JobName: job.Name, ExecutorGroupID: job.ExecutorGroupID, TriggerType: model.TriggerScheduled, ScheduledAt: job.NextTriggerAt,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: job.ProcessorName, ExecutionMode: job.ExecutionMode, Params: job.Params, RetryIntervalSeconds: 10, TimeoutSeconds: 900}}
}

type queryCounter struct {
	logger.Interface
	count      int
	statements []string
}

func (l *queryCounter) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	l.count++
	sql, _ := fc()
	l.statements = append(l.statements, sql)
}
