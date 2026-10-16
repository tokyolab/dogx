package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"github.com/zeromicro/go-zero/core/logx/logtest"
	"gorm.io/gorm"
)

var testNow = time.Date(2026, 10, 16, 12, 0, 5, 0, time.Local)

func validDefinition() *model.JobDefinition {
	due := testNow.Add(-5 * time.Second)
	return &model.JobDefinition{Base: basemodel.Base{ID: 7}, Name: "job", ExecutorGroupID: 2, ProcessorName: "processor", ExecutionMode: model.ExecutionModeNormal,
		Params: "current", CronExpr: "0 * * * * *", Status: model.StatusEnabled, MisfirePolicy: model.MisfireIgnore, BlockPolicy: model.BlockDiscard,
		MaxRetries: 3, RetryIntervalSeconds: 10, TimeoutSeconds: 900, NextTriggerAt: &due}
}

func testService(t *testing.T) (*Service, *repositoryStub) {
	t.Helper()
	s, err := New(&gorm.DB{}, WithClock(func() time.Time { return testNow }))
	if err != nil {
		t.Fatal(err)
	}
	r := &repositoryStub{job: validDefinition(), available: true, created: true, failures: map[string]error{}}
	s.repos = repositories{r, r, r}
	s.transact = func(ctx context.Context, fn func(repositories) error) error { return fn(s.repos) }
	return s, r
}

func TestAutomaticDecisionAndAtomicResult(t *testing.T) {
	for _, tc := range []struct {
		name      string
		lag       time.Duration
		misfire   model.MisfirePolicy
		active    bool
		duplicate bool
		reason    string
		trigger   model.TriggerType
		created   bool
	}{
		{"exact tolerance", 5 * time.Second, model.MisfireIgnore, false, false, "created", model.TriggerScheduled, true},
		{"outside tolerance ignored", 5*time.Second + time.Microsecond, model.MisfireIgnore, true, false, "misfire_ignored", 0, false},
		{"many missed coalesced", 72 * time.Hour, model.MisfireOnce, false, false, "created", model.TriggerMisfire, true},
		{"blocked", time.Second, model.MisfireOnce, true, false, "blocked", 0, false},
		{"duplicate", time.Second, model.MisfireOnce, false, true, "duplicate", model.TriggerScheduled, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r := testService(t)
			due := testNow.Add(-tc.lag)
			r.job.NextTriggerAt, r.job.MisfirePolicy, r.active, r.created = &due, tc.misfire, tc.active, !tc.duplicate
			hints := 0
			s.hint = func(int64) { hints++ }
			result, err := s.TriggerAutomatic(context.Background(), 7)
			if err != nil || result.Reason != tc.reason || result.Created != tc.created {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if r.advanced == nil || !r.advanced.After(testNow) {
				t.Fatalf("missing future progress: %v", r.advanced)
			}
			if tc.created && hints != 1 || !tc.created && hints != 0 {
				t.Fatalf("hints=%d", hints)
			}
			if r.inserted != nil && (r.inserted.TriggerType != tc.trigger || !r.inserted.ScheduledAt.Equal(due)) {
				t.Fatalf("snapshot=%+v", r.inserted)
			}
			if tc.reason == "misfire_ignored" && strings.Contains(strings.Join(r.calls, ","), "active") {
				t.Fatal("ignored misfire queried active instances")
			}
		})
	}
}

func TestAutomaticFailureDoesNotPublish(t *testing.T) {
	for _, step := range []string{"lock", "active", "insert", "advance", "commit"} {
		t.Run(step, func(t *testing.T) {
			s, r := testService(t)
			failure := errors.New("database transport failure")
			if step == "commit" {
				s.transact = func(ctx context.Context, fn func(repositories) error) error {
					if err := fn(s.repos); err != nil {
						return err
					}
					return failure
				}
			} else {
				r.failures[step] = failure
			}
			s.hint = func(int64) { t.Error("unconfirmed transaction published a hint") }
			result, err := s.TriggerAutomatic(context.Background(), 7)
			if !errors.Is(err, failure) || result != (AutomaticResult{}) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestAutomaticNoopAndInvalidSchedule(t *testing.T) {
	for _, kind := range []string{"deleted", "disabled", "not_due", "no_progress", "invalid_cron"} {
		t.Run(kind, func(t *testing.T) {
			s, r := testService(t)
			switch kind {
			case "deleted":
				r.failures["lock"] = ErrNotFound
			case "disabled":
				r.job.Status = model.StatusDisabled
			case "not_due":
				future := testNow.Add(time.Second)
				r.job.NextTriggerAt = &future
			case "no_progress":
				r.job.NextTriggerAt = nil
			case "invalid_cron":
				r.job.CronExpr = "broken"
			}
			_, err := s.TriggerAutomatic(context.Background(), 7)
			if (err != nil) != (kind == "invalid_cron") {
				t.Fatalf("err=%v", err)
			}
			if r.advanced != nil || r.inserted != nil {
				t.Fatal("no-op changed persistence")
			}
		})
	}
}

func TestManualReplayAndConflicts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		prior    *repository.ManualIdentity
		override bool
		params   string
		conflict bool
		fallback bool
	}{
		{"inherit ignores edited params", &repository.ManualIdentity{ID: 44}, false, "", false, false},
		{"same normalized override", &repository.ManualIdentity{ID: 44, ParamsOverridden: true, OverrideParams: stringPtr("x")}, true, " x ", false, false},
		{"different flag", &repository.ManualIdentity{ID: 44}, true, "", true, false},
		{"different content", &repository.ManualIdentity{ID: 44, ParamsOverridden: true, OverrideParams: stringPtr("x")}, true, "y", true, false},
		{"insert conflict fresh read", &repository.ManualIdentity{ID: 44}, false, "", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r := testService(t)
			r.identities = []*repository.ManualIdentity{tc.prior}
			if tc.fallback {
				r.identities = []*repository.ManualIdentity{nil, tc.prior}
				r.created = false
			} else {
				r.available = false
				r.active = true
			}
			id, err := s.TriggerManual(context.Background(), ManualTrigger{JobID: 7, RequestID: uuid.New(), OperatorID: 8, ParamsOverridden: tc.override, Params: tc.params})
			if tc.conflict {
				if !errors.Is(err, ErrRequestConflict) {
					t.Fatalf("err=%v", err)
				}
				return
			}
			if err != nil || id != 44 {
				t.Fatalf("id=%d err=%v", id, err)
			}
			if !tc.fallback && strings.Contains(strings.Join(r.calls, ","), "available") {
				t.Fatal("replay checked current group")
			}
		})
	}
}

func TestManualSnapshotAndRejections(t *testing.T) {
	s, r := testService(t)
	r.job.Status = model.StatusDisabled
	request := ManualTrigger{JobID: 7, RequestID: uuid.Nil, OperatorID: 8, ParamsOverridden: true, Params: "   "}
	id, err := s.TriggerManual(context.Background(), request)
	if err != nil || id != 99 {
		t.Fatalf("id=%d err=%v", id, err)
	}
	if r.inserted.ExecutionConfig.Params != "" || r.inserted.TriggeredBy == nil || *r.inserted.TriggeredBy != 8 || r.inserted.Status != model.InstanceWaiting || r.inserted.ScheduledAt != nil {
		t.Fatalf("snapshot=%+v", r.inserted)
	}
	if r.advanced != nil {
		t.Fatal("manual creation changed automatic progress")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*repositoryStub)
		want   error
	}{
		{"deleted", func(r *repositoryStub) { r.failures["lock"] = ErrNotFound }, ErrNotFound},
		{"unavailable group", func(r *repositoryStub) { r.available = false }, ErrExecutorGroupUnavailable},
		{"blocked", func(r *repositoryStub) { r.active = true }, ErrBlocked},
		{"unresolved conflict", func(r *repositoryStub) { r.created = false }, ErrIndeterminate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r := testService(t)
			tc.mutate(r)
			if _, err := s.TriggerManual(context.Background(), request); !errors.Is(err, tc.want) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestManualFailuresNeverPublishUnconfirmedInstance(t *testing.T) {
	for _, step := range []string{"lock", "find", "available", "active", "insert", "commit"} {
		t.Run(step, func(t *testing.T) {
			s, r := testService(t)
			failure := errors.New("manual transaction failed")
			if step == "commit" {
				s.transact = func(ctx context.Context, fn func(repositories) error) error {
					if err := fn(s.repos); err != nil {
						return err
					}
					return failure
				}
			} else {
				r.failures[step] = failure
			}
			s.hint = func(int64) { t.Error("failed manual transaction published a hint") }
			id, err := s.TriggerManual(context.Background(), ManualTrigger{JobID: 7, OperatorID: 8})
			if id != 0 || !errors.Is(err, failure) {
				t.Fatalf("id=%d err=%v", id, err)
			}
		})
	}
}

func TestManagementTransactionsAndNotifications(t *testing.T) {
	s, r := testService(t)
	notifications := 0
	s.notify = func(ctx context.Context) error {
		notifications++
		if _, ok := ctx.Deadline(); !ok {
			t.Error("notification lacks budget")
		}
		return errors.New("etcd unavailable")
	}
	job := validDefinition()
	job.Status = model.StatusDisabled
	job.Name = "  中文  "
	job.MaxRetries = 0
	id, err := s.CreateJob(context.Background(), job)
	if err != nil || id != 99 || r.saved.Name != "中文" || r.saved.NextTriggerAt != nil || notifications != 1 {
		t.Fatalf("id=%d err=%v saved=%+v notify=%d", id, err, r.saved, notifications)
	}
	if strings.Join(r.calls, ",") != "group_lock,create" {
		t.Fatalf("lock order=%v", r.calls)
	}
	r.calls = nil
	if err := s.UpdateJob(context.Background(), 7, job); err != nil {
		t.Fatal(err)
	}
	if strings.Join(r.calls, ",") != "group_lock,update" || r.saved.MaxRetries != 0 {
		t.Fatalf("calls=%v saved=%+v", r.calls, r.saved)
	}
	r.job.Status = model.StatusDisabled
	if err := s.UpdateJobStatus(context.Background(), 7, model.StatusEnabled); err != nil || r.enabled == nil || !r.enabled.After(testNow) {
		t.Fatalf("enable=%v err=%v", r.enabled, err)
	}
	r.enabled = nil
	r.job.Status = model.StatusEnabled
	if err := s.UpdateJobStatus(context.Background(), 7, model.StatusEnabled); err != nil || r.enabled != nil {
		t.Fatalf("idempotent enable err=%v", err)
	}
	r.failures["update"] = errors.New("write failed")
	before := notifications
	if err := s.UpdateJob(context.Background(), 7, job); err == nil || notifications != before {
		t.Fatal("failed write notified")
	}
}

func TestDeleteRequiresDisabledAndNoActiveInstances(t *testing.T) {
	s, r := testService(t)
	if err := s.DeleteJob(context.Background(), 7); !errors.Is(err, ErrStateNotAllowed) {
		t.Fatalf("err=%v", err)
	}
	r.job.Status = model.StatusDisabled
	r.active = true
	if err := s.DeleteJob(context.Background(), 7); !errors.Is(err, ErrActiveInstances) {
		t.Fatalf("err=%v", err)
	}
	r.active = false
	if err := s.DeleteJob(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
	if r.calls[len(r.calls)-1] != "delete" {
		t.Fatal("delete not persisted")
	}
}

func TestCreationCapacitySharedAndHeldUntilUnwind(t *testing.T) {
	s, _ := testService(t)
	s.slots = make(chan struct{}, 1)
	entered, allowReturn, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	s.transact = func(ctx context.Context, fn func(repositories) error) error {
		close(entered)
		<-allowReturn
		return ctx.Err()
	}
	go func() { _, err := s.TriggerAutomatic(ctx, 7); done <- err }()
	<-entered
	cancel()
	_, err := s.TriggerManual(context.Background(), ManualTrigger{JobID: 7, RequestID: uuid.New(), OperatorID: 8})
	if !errors.Is(err, ErrCapacityBusy) {
		t.Fatalf("manual exceeded shared capacity: %v", err)
	}
	close(allowReturn)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if len(s.slots) != 0 {
		t.Fatal("creation slot leaked")
	}
}

func TestDefinitionValidationAndLimits(t *testing.T) {
	for _, value := range []string{"\x00", string([]byte{0xff}), strings.Repeat("界", 129)} {
		j := validDefinition()
		j.Name = value
		if _, err := normalizedDefinition(j); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("accepted invalid name %q", value)
		}
	}
	j := validDefinition()
	j.Name = " \t" + strings.Repeat("界", 128) + " \n"
	if _, err := normalizedDefinition(j); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*model.JobDefinition){func(j *model.JobDefinition) { j.PostEnabled = true }, func(j *model.JobDefinition) { j.MaxRetries = -1 }, func(j *model.JobDefinition) { j.TimeoutSeconds = 0 }, func(j *model.JobDefinition) { j.ExecutionMode = 3 }, func(j *model.JobDefinition) { j.BlockPolicy = 3 }, func(j *model.JobDefinition) { j.MisfirePolicy = 3 }} {
		j := validDefinition()
		mutate(j)
		if _, err := normalizedDefinition(j); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("accepted %+v", j)
		}
	}
	if _, err := New(nil); err == nil {
		t.Fatal("accepted nil database")
	}
	if _, err := New(&gorm.DB{}, WithCreationLimits(0, time.Second)); err == nil {
		t.Fatal("accepted zero capacity")
	}
	if _, err := New(&gorm.DB{}, WithCreationLimits(1, 0)); err == nil {
		t.Fatal("accepted zero budget")
	}
	s, _ := testService(t)
	if _, _, err := s.ListJobs(context.Background(), repository.JobFilter{Page: 1<<63 - 1, PageSize: 200}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("accepted overflowing page: %v", err)
	}
}

func TestAutomaticCapacityWaitHonorsProcessingBudget(t *testing.T) {
	s, r := testService(t)
	s.slots = make(chan struct{}, 1)
	s.slots <- struct{}{}
	s.budget = 20 * time.Millisecond
	_, err := s.TriggerAutomatic(context.Background(), 7)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("automatic quota wait err=%v", err)
	}
	if len(r.calls) != 0 || len(s.slots) != 1 {
		t.Fatalf("waiting worker touched persistence or released another slot: calls=%v slots=%d", r.calls, len(s.slots))
	}
	<-s.slots
	if result, err := s.TriggerAutomatic(context.Background(), 7); err != nil || !result.Created {
		t.Fatalf("capacity did not recover: result=%+v err=%v", result, err)
	}
}

func TestCreationCancellationDoesNotEnterTransaction(t *testing.T) {
	s, r := testService(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.TriggerAutomatic(ctx, 7); !errors.Is(err, context.Canceled) {
		t.Fatalf("automatic cancellation=%v", err)
	}
	if _, err := s.TriggerManual(ctx, ManualTrigger{JobID: 7, OperatorID: 8}); !errors.Is(err, context.Canceled) {
		t.Fatalf("manual cancellation=%v", err)
	}
	if len(r.calls) != 0 || len(s.slots) != 0 {
		t.Fatalf("canceled creation calls=%v slots=%d", r.calls, len(s.slots))
	}
}

func TestNotificationRespectsUpstreamDeadlineAndCancellation(t *testing.T) {
	s, _ := testService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	wantDeadline, _ := ctx.Deadline()
	calls := 0
	s.notify = func(ctx context.Context) error {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok || !deadline.Equal(wantDeadline) {
			t.Errorf("notification deadline=%v want=%v", deadline, wantDeadline)
		}
		return errors.New("notification failed after known commit")
	}
	if _, err := s.CreateJob(ctx, validDefinition()); err != nil {
		t.Fatalf("notification changed save result: %v", err)
	}
	cancel()
	s.changed(ctx)
	if calls != 1 {
		t.Fatalf("canceled request still notified: calls=%d", calls)
	}
}

func TestAutomaticSkipLogsOnlyConfirmedCommitWithDecisionFacts(t *testing.T) {
	for _, commitFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "committed", true: "unknown commit"}[commitFails], func(t *testing.T) {
			collector := logtest.NewCollector(t)
			s, r := testService(t)
			r.active = true
			if commitFails {
				s.transact = func(ctx context.Context, fn func(repositories) error) error {
					if err := fn(s.repos); err != nil {
						return err
					}
					return errors.New("commit response lost")
				}
			}
			_, _ = s.TriggerAutomatic(context.Background(), 7)
			logs := collector.String()
			if commitFails {
				if strings.Contains(logs, "job automatic trigger skipped") {
					t.Fatalf("unconfirmed commit logged successful skip: %s", logs)
				}
				return
			}
			for _, field := range []string{"job_id", "scheduled_at", "decision_at", "next_trigger_at", "BLOCK_DISCARDED"} {
				if !strings.Contains(logs, field) {
					t.Errorf("skip log missing %s: %s", field, logs)
				}
			}
			if strings.Count(logs, "job automatic trigger skipped") != 1 {
				t.Fatalf("skip events=%s", logs)
			}
		})
	}
}

func stringPtr(s string) *string { return &s }

type repositoryStub struct {
	job                        *model.JobDefinition
	available, active, created bool
	identities                 []*repository.ManualIdentity
	finds                      int
	inserted                   *model.JobInstance
	saved                      *model.JobDefinition
	advanced, enabled          *time.Time
	calls                      []string
	failures                   map[string]error
}

func (r *repositoryStub) call(name string) error {
	r.calls = append(r.calls, name)
	return r.failures[name]
}
func (r *repositoryStub) List(context.Context, repository.JobFilter) ([]repository.JobListItem, int64, error) {
	return []repository.JobListItem{}, 0, r.call("list")
}
func (r *repositoryStub) Get(context.Context, int64) (*repository.JobDetail, error) {
	return &repository.JobDetail{JobDefinition: *r.job}, r.call("get")
}
func (r *repositoryStub) Lock(context.Context, int64) (*model.JobDefinition, error) {
	return r.job, r.call("lock")
}
func (r *repositoryStub) Create(_ context.Context, j *model.JobDefinition) (int64, error) {
	r.saved = j
	return 99, r.call("create")
}
func (r *repositoryStub) Update(_ context.Context, _ int64, j *model.JobDefinition) error {
	r.saved = j
	return r.call("update")
}
func (r *repositoryStub) Enable(_ context.Context, _ int64, next time.Time) error {
	r.enabled = &next
	return r.call("enable")
}
func (r *repositoryStub) Disable(context.Context, int64) error { return r.call("disable") }
func (r *repositoryStub) Delete(context.Context, int64) error  { return r.call("delete") }
func (r *repositoryStub) Advance(_ context.Context, _ int64, next time.Time) error {
	r.advanced = &next
	return r.call("advance")
}
func (r *repositoryStub) LoadRules(context.Context) ([]repository.Rule, error) {
	return []repository.Rule{{ID: r.job.ID, CronExpr: r.job.CronExpr}}, r.call("rules")
}
func (r *repositoryStub) ListDue(context.Context, time.Time, int) ([]repository.DueJob, error) {
	return []repository.DueJob{}, r.call("due")
}
func (r *repositoryStub) LockAvailable(context.Context, int64) error {
	if err := r.call("group_lock"); err != nil {
		return err
	}
	if !r.available {
		return ErrExecutorGroupUnavailable
	}
	return nil
}
func (r *repositoryStub) Available(context.Context, int64) (bool, error) {
	return r.available, r.call("available")
}
func (r *repositoryStub) HasActive(context.Context, int64) (bool, error) {
	return r.active, r.call("active")
}
func (r *repositoryStub) FindManual(context.Context, int64, uuid.UUID) (*repository.ManualIdentity, error) {
	var identity *repository.ManualIdentity
	if r.finds < len(r.identities) {
		identity = r.identities[r.finds]
	}
	r.finds++
	return identity, r.call("find")
}
func (r *repositoryStub) Insert(_ context.Context, i *model.JobInstance) (int64, bool, error) {
	r.inserted = i
	return 99, r.created, r.call("insert")
}
func (r *repositoryStub) ListWaitingIDs(context.Context, int64, int) ([]int64, error) {
	return nil, r.call("waiting")
}
func (r *repositoryStub) LoadWaiting(context.Context, []int64) ([]repository.WaitingInstance, error) {
	return nil, r.call("load_waiting")
}
