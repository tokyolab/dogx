//go:build integration

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/service"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
)

type observedTrigger struct {
	TriggerService
	loads    chan ruleObservation
	due      chan dueObservation
	triggers chan int64
	finished chan string
	source   string
}

type ruleObservation struct {
	at    time.Time
	rules []repository.Rule
}

type dueObservation struct {
	at   time.Time
	rows []repository.DueJob
}

func (o observedTrigger) LoadRules(ctx context.Context) ([]repository.Rule, error) {
	rules, err := o.TriggerService.LoadRules(ctx)
	if err == nil {
		select {
		case o.loads <- ruleObservation{at: time.Now(), rules: rules}:
		default:
		}
	}
	return rules, err
}

func (o observedTrigger) ListDue(ctx context.Context, at time.Time, limit int) ([]repository.DueJob, error) {
	rows, err := o.TriggerService.ListDue(ctx, at, limit)
	if err == nil {
		select {
		case o.due <- dueObservation{at: time.Now(), rows: rows}:
		default:
		}
	}
	return rows, err
}

func (o observedTrigger) TriggerAutomatic(ctx context.Context, id int64) (service.AutomaticResult, error) {
	select {
	case o.triggers <- id:
	default:
	}
	result, err := o.TriggerService.TriggerAutomatic(ctx, id)
	select {
	case o.finished <- o.source:
	default:
	}
	return result, err
}

func TestEtcdPostgresFailoverAndRealMinuteFallback(t *testing.T) {
	f := newEtcdFixture(t)
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	group := model.ExecutorGroup{Name: "offline but valid test group"}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	// Each replica has its own real service and process concurrency permits.
	jobsA, err := service.New(db)
	if err != nil {
		t.Fatal(err)
	}
	jobsB, err := service.New(db)
	if err != nil {
		t.Fatal(err)
	}
	definition := model.JobDefinition{Name: "fixed snapshot", ExecutorGroupID: group.ID, ProcessorName: "test.processor",
		ExecutionMode: model.ExecutionModeNormal, Params: "original parameters", CronExpr: "0 0 0 1 JAN *",
		Status: model.StatusDisabled, MisfirePolicy: model.MisfireOnce, BlockPolicy: model.BlockParallel,
		RetryIntervalSeconds: 10, TimeoutSeconds: 900}
	jobID, err := jobsA.CreateJob(context.Background(), &definition)
	if err != nil {
		t.Fatal(err)
	}
	scheduledAt := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Second)
	if err := db.Model(&model.JobDefinition{}).Where("id = ?", jobID).
		Updates(map[string]any{"status": model.StatusEnabled, "next_trigger_at": scheduledAt}).Error; err != nil {
		t.Fatal(err)
	}
	// A real PostgreSQL row lock keeps an automatic operation in flight at
	// lease revocation. Both replicas still run the real transactional service.
	blocker, err := sqlDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Rollback() })
	if _, err := blocker.Exec("SELECT id FROM job_definition WHERE id = $1 FOR UPDATE", jobID); err != nil {
		t.Fatal(err)
	}
	loads, due, triggers := make(chan ruleObservation, 32), make(chan dueObservation, 32), make(chan int64, 32)
	finished := make(chan string, 32)
	prefix := "/test/persisted-failover"
	a, err := New(f.replicaClient(t), observedTrigger{TriggerService: jobsA, loads: loads, due: due, triggers: triggers, finished: finished, source: "a"}, Config{Prefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	startRuntime(t, a)
	select {
	case id := <-triggers:
		if id != jobID {
			t.Fatalf("unexpected initial job %d", id)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first replica did not start the locked automatic operation")
	}
	b, err := New(f.replicaClient(t), observedTrigger{TriggerService: jobsB, loads: loads, due: due, triggers: triggers, finished: finished, source: "b"}, Config{Prefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	startRuntime(t, b)
	revokeLeader(t, f.client, prefix)
	waitCondition(t, 10*time.Second, "second replica's elected term", func() bool { return b.Stats().Terms == 1 })
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	countInstances := func(id int64, scheduled *time.Time) int64 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		query := db.WithContext(ctx).Model(&model.JobInstance{}).Where("job_id = ?", id)
		if scheduled != nil {
			query = query.Where("scheduled_at = ?", *scheduled)
		}
		var count int64
		if err := query.Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		return count
	}
	waitCondition(t, 10*time.Second, "one persisted automatic identity", func() bool { return countInstances(jobID, &scheduledAt) == 1 })
	for range 2 {
		for len(loads) > 0 {
			<-loads
		}
		for len(due) > 0 {
			<-due
		}
		for len(finished) > 0 {
			<-finished
		}
		previousA := a.Stats().Terms
		terms := previousA + b.Stats().Terms
		revokeLeader(t, f.client, prefix)
		waitCondition(t, 15*time.Second, "repeated leader takeover", func() bool { return a.Stats().Terms+b.Stats().Terms > terms })
		if got := countInstances(jobID, &scheduledAt); got != 1 {
			t.Fatalf("automatic identity duplicated across lease transitions: %d", got)
		}
		select {
		case <-due:
		case <-time.After(10 * time.Second):
			t.Fatal("fresh leader did not complete its initial recovery query")
		}
		newLeader := "b"
		if a.Stats().Terms > previousA {
			newLeader = "a"
		}
		waitCondition(t, 10*time.Second, "fresh leader's registration check to finish", func() bool {
			select {
			case source := <-finished:
				return source == newLeader
			default:
				return false
			}
		})
	}
	var original model.JobInstance
	if err := db.Where("job_id = ? AND scheduled_at = ?", jobID, scheduledAt).Take(&original).Error; err != nil {
		t.Fatal(err)
	}
	if original.Status != model.InstanceWaiting || original.StartedAt != nil || original.DeadlineAt != nil || original.FinishedAt != nil || original.ExecutionConfig.Params != definition.Params {
		t.Fatalf("trigger crossed the WAITING handoff boundary: %+v", original)
	}
	// Wait until the fresh leader has completed its immediate scans before
	// injecting missed notifications. Neither following Job can fire via Cron
	// during this test (both rules are annual); the unchanged one needs recovery.
	waitCondition(t, 10*time.Second, "fresh election's initial loads", func() bool {
		select {
		case <-loads:
			return true
		default:
			return false
		}
	})
	for len(loads) > 0 {
		<-loads
	}
	fallbackStarted := time.Now()
	definition.Name = "committed without notification"
	secondID, err := jobsB.CreateJob(context.Background(), &definition)
	if err != nil {
		t.Fatal(err)
	}
	missedAt := time.Now().UTC().Truncate(time.Microsecond).Add(-10 * time.Second)
	if err := db.Model(&model.JobDefinition{}).Where("id IN ?", []int64{jobID, secondID}).
		Updates(map[string]any{"status": model.StatusEnabled, "next_trigger_at": missedAt}).Error; err != nil {
		t.Fatal(err)
	}
	// No Notify call and no injected clock/tick: this exercises both production
	// one-minute loops against real PostgreSQL while the etcd leader stays live.
	var sawNewConfiguration, sawMinuteRecovery bool
	waitCondition(t, 80*time.Second, "real minute configuration/recovery fallback", func() bool {
		for {
			select {
			case observation := <-loads:
				for _, rule := range observation.rules {
					if rule.ID == secondID && observation.at.Sub(fallbackStarted) > 50*time.Second {
						sawNewConfiguration = true
					}
				}
			case observation := <-due:
				for _, row := range observation.rows {
					if row.ID == jobID && observation.at.Sub(fallbackStarted) > 50*time.Second {
						sawMinuteRecovery = true
					}
				}
			default:
				return sawNewConfiguration && sawMinuteRecovery && countInstances(jobID, &missedAt) == 1 && countInstances(secondID, &missedAt) == 1
			}
		}
	})
	var recovered []model.JobInstance
	if err := db.Where("scheduled_at = ?", missedAt).Find(&recovered).Error; err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 2 {
		t.Fatalf("missed interval did not create exactly one instance per Job: %+v", recovered)
	}
	for _, instance := range recovered {
		if instance.TriggerType != model.TriggerMisfire || instance.Status != model.InstanceWaiting || instance.StartedAt != nil || instance.DeadlineAt != nil {
			t.Fatalf("minute fallback lost misfire or handoff semantics: %+v", instance)
		}
	}
}
