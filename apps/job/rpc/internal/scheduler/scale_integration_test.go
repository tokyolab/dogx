//go:build integration

package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/schedule"
	"github.com/tokyolab/dogx/apps/job/internal/service"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
)

type scaleCompletion struct {
	id  int64
	err error
}

// Observe completed real calls without replacing the SQL, clock, permits, or
// trigger results. Completion lets the assertion include all 400 future Jobs.
type scaleTriggerObserver struct {
	TriggerService
	loaded    chan []repository.Rule
	completed chan scaleCompletion
}

func (o scaleTriggerObserver) LoadRules(ctx context.Context) ([]repository.Rule, error) {
	rules, err := o.TriggerService.LoadRules(ctx)
	if err == nil {
		select {
		case o.loaded <- rules:
		default:
		}
	}
	return rules, err
}

func (o scaleTriggerObserver) TriggerAutomatic(ctx context.Context, id int64) (service.AutomaticResult, error) {
	result, err := o.TriggerService.TriggerAutomatic(ctx, id)
	select {
	case o.completed <- scaleCompletion{id: id, err: err}:
	default:
	}
	return result, err
}

func TestEtcdPostgresFiveHundredRulesHundredDueSmoke(t *testing.T) {
	const ruleCount, dueCount = 500, 100
	f := newEtcdFixture(t)
	db, _ := testutil.OpenMigratedPostgres(t)
	group := model.ExecutorGroup{Name: "scale smoke group"}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	// Select an annual occurrence months away, including when CI runs around
	// New Year. Every trigger in this test then comes from loading/recovery.
	month := (int(now.In(time.Local).Month())+5)%12 + 1
	expr := fmt.Sprintf("0 0 0 1 %d *", month)
	future, err := schedule.Next(expr, now)
	if err != nil {
		t.Fatal(err)
	}
	scheduledAt := future.AddDate(-1, 0, 0).UTC().Truncate(time.Microsecond)
	definitions := make([]model.JobDefinition, ruleCount)
	for i := range definitions {
		next := future
		if i < dueCount {
			next = scheduledAt
		}
		params := ""
		if i > 0 {
			params = fmt.Sprintf("payload for Job %d", i)
		}
		definitions[i] = model.JobDefinition{
			Name: fmt.Sprintf("scale Job %d", i), ExecutorGroupID: group.ID,
			ProcessorName: "scale.processor", ExecutionMode: model.ExecutionModeNormal,
			Params: params, CronExpr: expr, Status: model.StatusEnabled,
			MisfirePolicy: model.MisfireOnce, BlockPolicy: model.BlockDiscard,
			MaxRetries: int32(i % 4), RetryIntervalSeconds: 10, TimeoutSeconds: 900,
			NextTriggerAt: &next, CreatedAt: now, UpdatedAt: now,
		}
	}
	// Fixture setup is bounded batch SQL; normal scheduling below uses only
	// the real service and repositories against the migrated test schema.
	if err := db.CreateInBatches(&definitions, 100).Error; err != nil {
		t.Fatal(err)
	}
	byID := make(map[int64]model.JobDefinition, ruleCount)
	dueIDs := make(map[int64]bool, dueCount)
	for i, definition := range definitions {
		byID[definition.ID] = definition
		if i < dueCount {
			dueIDs[definition.ID] = true
		}
	}
	jobs, err := service.New(db)
	if err != nil {
		t.Fatal(err)
	}
	observer := scaleTriggerObserver{TriggerService: jobs,
		loaded: make(chan []repository.Rule, 4), completed: make(chan scaleCompletion, ruleCount*2)}
	r, err := New(f.replicaClient(t), observer, Config{Prefix: "/test/scale-smoke"})
	if err != nil {
		t.Fatal(err)
	}
	createdCron := make(chan cronEngine, 1)
	newCron := r.newCron
	r.newCron = func() cronEngine {
		engine := newCron()
		select {
		case createdCron <- engine:
		default:
		}
		return engine
	}
	cancelRuntime, _ := startRuntime(t, r)
	var engine cronEngine
	select {
	case engine = <-createdCron:
	case <-time.After(10 * time.Second):
		t.Fatal("elected runtime did not create its real Cron engine")
	}
	select {
	case rules := <-observer.loaded:
		if len(rules) != ruleCount {
			t.Fatalf("database configuration load truncated rules: %d", len(rules))
		}
		loadedIDs := make(map[int64]bool, ruleCount)
		for _, rule := range rules {
			if _, exists := byID[rule.ID]; !exists || rule.CronExpr != expr || loadedIDs[rule.ID] {
				t.Fatalf("unexpected or duplicate loaded rule: %+v", rule)
			}
			loadedIDs[rule.ID] = true
		}
	case <-time.After(10 * time.Second):
		t.Fatal("real database configuration load did not complete")
	}
	waitCondition(t, 10*time.Second, "all 500 real Cron registrations to initialize", func() bool {
		entries := engine.Entries()
		if len(entries) != ruleCount {
			return false
		}
		for _, entry := range entries {
			if entry.Next.IsZero() || !entry.Next.Equal(future) {
				return false
			}
		}
		return true
	})
	checked := make(map[int64]bool, ruleCount)
	waitCondition(t, 20*time.Second, "real registration/recovery checks for all 500 Jobs", func() bool {
		for {
			select {
			case completion := <-observer.completed:
				if completion.err != nil {
					t.Fatalf("real automatic check for Job %d failed: %v", completion.id, completion.err)
				}
				if _, exists := byID[completion.id]; !exists {
					t.Fatalf("runtime checked an unknown Job %d", completion.id)
				}
				checked[completion.id] = true
			default:
				return len(checked) == ruleCount
			}
		}
	})
	cancelRuntime()
	waitCondition(t, 10*time.Second, "scale smoke runtime cleanup", func() bool { return !r.running.Load() })
	var instances []model.JobInstance
	if err := db.Order("job_id").Find(&instances).Error; err != nil {
		t.Fatal(err)
	}
	if len(instances) != dueCount {
		t.Fatalf("100 due Jobs produced %d Instances", len(instances))
	}
	seen := make(map[int64]bool, dueCount)
	for _, instance := range instances {
		if !dueIDs[instance.JobID] || seen[instance.JobID] {
			t.Fatalf("future Job triggered or due Job duplicated: Job %d", instance.JobID)
		}
		seen[instance.JobID] = true
		definition := byID[instance.JobID]
		wantConfig := model.ExecutionConfig{SchemaVersion: 1, ProcessorName: definition.ProcessorName,
			ExecutionMode: definition.ExecutionMode, PostEnabled: definition.PostEnabled, Params: definition.Params,
			MaxRetries: definition.MaxRetries, RetryIntervalSeconds: definition.RetryIntervalSeconds, TimeoutSeconds: definition.TimeoutSeconds}
		if instance.ExecutionConfig != wantConfig || instance.JobName != definition.Name || instance.ExecutorGroupID != group.ID {
			t.Fatalf("Job %d lost its immutable execution snapshot: %+v", instance.JobID, instance)
		}
		if instance.ScheduledAt == nil || !instance.ScheduledAt.Equal(scheduledAt) || instance.TriggerType != model.TriggerMisfire ||
			instance.RequestID != nil || instance.ParamsOverridden || instance.TriggeredBy != nil {
			t.Fatalf("Job %d lost its automatic identity: %+v", instance.JobID, instance)
		}
		if instance.Status != model.InstanceWaiting || instance.StartedAt != nil || instance.DeadlineAt != nil ||
			instance.FinishedAt != nil || instance.EndReason != nil || instance.ResultSummary != nil {
			t.Fatalf("Job %d crossed the WAITING handoff boundary: %+v", instance.JobID, instance)
		}
	}
	var persisted []model.JobDefinition
	if err := db.Find(&persisted).Error; err != nil {
		t.Fatal(err)
	}
	if len(persisted) != ruleCount {
		t.Fatalf("expected all 500 Job definitions, got %d", len(persisted))
	}
	for _, definition := range persisted {
		if definition.NextTriggerAt == nil || !definition.NextTriggerAt.Equal(future) || !definition.UpdatedAt.Equal(now) {
			t.Fatalf("Job %d advanced inconsistently or changed management time: %+v", definition.ID, definition)
		}
	}
	if stats := r.Stats(); stats.QueueRejected != 0 || stats.TriggerErrors != 0 || stats.LoadErrors != 0 || stats.RecoveryErrors != 0 {
		t.Fatalf("functional smoke dropped or failed work: %+v", stats)
	}
}
