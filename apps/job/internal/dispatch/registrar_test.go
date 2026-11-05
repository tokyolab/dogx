package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestRegistrarReturnsOnlyConfirmedNewAttempt(t *testing.T) {
	f := newRegistrationFixture(t)
	got, err := f.registrar.StartOrdinary(context.Background(), 11)
	if err != nil || got == nil || got.TaskID != 31 || got.AttemptNo != 1 ||
		got.JobID != 21 || got.ExecutorGroupID != 41 || got.InstanceID != 11 ||
		got.ExecutionConfig.ProcessorName != "system.cleanup" || !got.RegisteredAt.Equal(f.now) ||
		!got.DeadlineAt.Equal(f.now.Add(900*time.Second)) {
		t.Fatalf("registration=%+v err=%v", got, err)
	}
	if !f.started || !f.inserted {
		t.Fatal("registration omitted the parent or child write")
	}

	f = newRegistrationFixture(t)
	f.commitErr = errors.New("synthetic lost commit confirmation")
	got, err = f.registrar.StartOrdinary(context.Background(), 11)
	if got != nil || !errors.Is(err, f.commitErr) || !f.inserted || f.callbackErr != nil {
		t.Fatalf("unconfirmed registration=%+v err=%v callback=%v", got, err, f.callbackErr)
	}
}

func TestRegistrarSkipsOldCandidatesBeforeDecoding(t *testing.T) {
	for _, state := range []model.InstanceStatus{model.InstanceRunning, model.InstanceStopping,
		model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled} {
		t.Run(fmt.Sprint(state), func(t *testing.T) {
			f := newRegistrationFixture(t)
			f.instance.Status = state
			f.instance.ExecutionConfig = json.RawMessage(`{"invalid`)
			f.assertSkipped(t)
		})
	}
	t.Run("missing", func(t *testing.T) {
		f := newRegistrationFixture(t)
		f.instance = nil
		f.assertSkipped(t)
	})
	t.Run("sharded", func(t *testing.T) {
		f := newRegistrationFixture(t)
		f.instance.ExecutionConfig = json.RawMessage(`{"schema_version":1,"execution_mode":2}`)
		f.assertSkipped(t)
	})
	t.Run("lock conflict", func(t *testing.T) {
		f := newRegistrationFixture(t)
		f.lockErr = fmt.Errorf("lock: %w", repository.ErrInstanceLocked)
		got, err := f.registrar.StartOrdinary(context.Background(), 11)
		if got != nil || !errors.Is(err, repository.ErrInstanceLocked) ||
			!errors.Is(f.callbackErr, repository.ErrInstanceLocked) || f.clockRead || f.started || f.inserted {
			t.Fatal("lock conflict was lost or continued registration instead of requesting rollback")
		}
	})
}

func TestRegistrarFailuresNeverPublishAnAttempt(t *testing.T) {
	broken := errors.New("synthetic database error")
	for _, scenario := range []struct {
		name      string
		configure func(*registrationFixture)
		want      error
	}{
		{"lock", func(f *registrationFixture) { f.lockErr = broken }, broken},
		{"clock", func(f *registrationFixture) { f.clockErr = broken }, broken},
		{"start", func(f *registrationFixture) { f.startErr = broken }, broken},
		{"missed parent update", func(f *registrationFixture) { f.startChanged = false }, ErrInvalidInstanceState},
		{"insert", func(f *registrationFixture) { f.insertErr = broken }, broken},
		{"invalid insert identity", func(f *registrationFixture) { f.taskID = 0 }, nil},
		{"invalid clock", func(f *registrationFixture) { f.now = time.Time{} }, nil},
		{"existing start", func(f *registrationFixture) { f.instance.StartedAt = &f.now }, ErrInvalidInstanceState},
		{"existing deadline", func(f *registrationFixture) { f.instance.DeadlineAt = &f.now }, ErrInvalidInstanceState},
		{"existing finish", func(f *registrationFixture) { f.instance.FinishedAt = &f.now }, ErrInvalidInstanceState},
		{"broken snapshot", func(f *registrationFixture) { f.instance.ExecutionConfig = json.RawMessage(`{`) }, ErrInvalidExecutionConfig},
		{"unknown schema", func(f *registrationFixture) { f.instance.ExecutionConfig = json.RawMessage(`{"schema_version":2}`) }, ErrInvalidExecutionConfig},
		{"missing job", func(f *registrationFixture) { f.instance.JobID = 0 }, ErrInvalidInstanceState},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newRegistrationFixture(t)
			scenario.configure(f)
			got, err := f.registrar.StartOrdinary(context.Background(), 11)
			if got != nil || err == nil || (scenario.want != nil && !errors.Is(err, scenario.want)) {
				t.Fatalf("registration=%+v err=%v want=%v", got, err, scenario.want)
			}
			if f.callbackErr == nil {
				t.Fatal("failed registration did not request rollback")
			}
			if !f.startChanged && f.inserted {
				t.Fatal("inserted a Task after the parent conditional update missed")
			}
		})
	}
}

func TestRegistrarPreservesEarlierBudgetAndCancellation(t *testing.T) {
	if _, err := NewRegistrar(nil); err == nil {
		t.Fatal("nil runner accepted")
	}
	if _, err := NewRegistrar(func(context.Context, func(repository.Repositories) error) error { return nil }, nil); err == nil {
		t.Fatal("nil registrar option accepted")
	}
	f := newRegistrationFixture(t)
	deadline := time.Now().Add(time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if _, err := f.registrar.StartOrdinary(ctx, 11); err != nil {
		t.Fatal(err)
	}
	if !f.deadline.Equal(deadline) {
		t.Fatalf("caller deadline extended to %v from %v", f.deadline, deadline)
	}
	for _, id := range []int64{0, -1} {
		if got, err := f.registrar.StartOrdinary(context.Background(), id); got != nil || err == nil {
			t.Fatalf("invalid ID %d: %+v %v", id, got, err)
		}
	}
	f = newRegistrationFixture(t)
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if got, err := f.registrar.StartOrdinary(canceled, 11); got != nil || !errors.Is(err, context.Canceled) || f.called {
		t.Fatalf("canceled registration=%+v err=%v runnerCalled=%v", got, err, f.called)
	}
}

func TestRegistrarRejectsInvalidOrdinarySnapshot(t *testing.T) {
	for _, mutate := range []func(*model.ExecutionConfig){
		func(c *model.ExecutionConfig) { c.ExecutionMode = 0 },
		func(c *model.ExecutionConfig) { c.PostEnabled = true },
		func(c *model.ExecutionConfig) { c.TimeoutSeconds = 0 },
		func(c *model.ExecutionConfig) { c.MaxRetries = -1 },
		func(c *model.ExecutionConfig) { c.RetryIntervalSeconds = 0 },
		func(c *model.ExecutionConfig) { c.ProcessorName = " " },
		func(c *model.ExecutionConfig) { c.Params = "bad\x00input" },
	} {
		f := newRegistrationFixture(t)
		var config model.ExecutionConfig
		if err := json.Unmarshal(f.instance.ExecutionConfig, &config); err != nil {
			t.Fatal(err)
		}
		mutate(&config)
		data, err := json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		f.instance.ExecutionConfig = data
		if got, err := f.registrar.StartOrdinary(context.Background(), 11); got != nil || !errors.Is(err, ErrInvalidExecutionConfig) || f.clockRead {
			t.Fatalf("invalid snapshot registered=%+v err=%v clockRead=%v", got, err, f.clockRead)
		}
	}
}

// registrationFixture simulates the runner boundary separately from callback
// success so a generated Task ID cannot escape an unconfirmed transaction.
type registrationFixture struct {
	registrar                                                      *Registrar
	instance                                                       *repository.ExecutionInstance
	now, deadline                                                  time.Time
	taskID                                                         int64
	called, clockRead, started, inserted, startChanged             bool
	transactionReturned                                            bool
	lockErr, clockErr, startErr, insertErr, commitErr, callbackErr error
	retryTask                                                      *repository.RegisteredTask
	retryWrite                                                     repository.RetryRegistration
	retryCalls                                                     int
	retryErr                                                       error
}

func newRegistrationFixture(t *testing.T, options ...RegistrarOption) *registrationFixture {
	t.Helper()
	f := &registrationFixture{
		now: time.Date(2026, 11, 4, 1, 2, 3, 123456000, time.UTC), taskID: 31, startChanged: true,
		instance: &repository.ExecutionInstance{ID: 11, JobID: 21, ExecutorGroupID: 41, Status: model.InstanceWaiting,
			ExecutionConfig: json.RawMessage(`{"schema_version":1,"execution_mode":1,"processor_name":"system.cleanup","params":"","post_enabled":false,"max_retries":0,"retry_interval_seconds":10,"timeout_seconds":900}`)},
	}
	runner := func(ctx context.Context, fn func(repository.Repositories) error) error {
		defer func() { f.transactionReturned = true }()
		f.called = true
		var ok bool
		f.deadline, ok = ctx.Deadline()
		if !ok || time.Until(f.deadline) > registrationTimeout {
			t.Fatal("registration transaction has no bounded deadline")
		}
		f.callbackErr = fn(repository.Repositories{Instances: &registrationInstanceStub{f: f, t: t}, Tasks: &registrationTaskStub{f: f, t: t}})
		if f.callbackErr != nil {
			return f.callbackErr
		}
		return f.commitErr
	}
	var err error
	f.registrar, err = NewRegistrar(runner, options...)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *registrationFixture) assertSkipped(t *testing.T) {
	t.Helper()
	got, err := f.registrar.StartOrdinary(context.Background(), 11)
	if got != nil || err != nil || f.clockRead || f.started || f.inserted {
		t.Fatalf("skipped registration=%+v err=%v clock=%v started=%v inserted=%v", got, err, f.clockRead, f.started, f.inserted)
	}
}

type registrationInstanceStub struct {
	f *registrationFixture
	t *testing.T
}

func (*registrationInstanceStub) Exists(context.Context, int64) (bool, error) {
	panic("unexpected Exists")
}
func (*registrationInstanceStub) FindCompletion(context.Context, int64) (*repository.InstanceCompletion, error) {
	panic("unexpected FindCompletion")
}
func (*registrationInstanceStub) FindByID(context.Context, int64) (*model.JobInstance, error) {
	panic("unexpected FindByID")
}
func (*registrationInstanceStub) Complete(context.Context, int64, model.InstanceStatus, *string) (bool, error) {
	panic("unexpected Complete")
}

func (*registrationInstanceStub) LockForStop(context.Context, int64, bool) (*repository.StoppingInstance, error) {
	panic("unexpected LockForStop")
}
func (*registrationInstanceStub) RecordStop(context.Context, int64, model.InstanceStatus, string, string, time.Time) (bool, error) {
	panic("unexpected RecordStop")
}
func (*registrationInstanceStub) FinishStop(context.Context, int64, string) (bool, error) {
	panic("unexpected FinishStop")
}

func (*registrationInstanceStub) StoppingUpperID(context.Context) (int64, error) {
	panic("unexpected StoppingUpperID")
}
func (*registrationInstanceStub) ListStoppingCandidates(context.Context, int64, int64, int) ([]int64, error) {
	panic("unexpected ListStoppingCandidates")
}

func (*registrationInstanceStub) ListRunningCandidates(context.Context, time.Time, *repository.RunningCandidate, int) ([]repository.RunningCandidate, error) {
	panic("unexpected ListRunningCandidates")
}

func (*registrationTaskStub) ListCancelTargets(context.Context, int64, string, int) ([]string, error) {
	panic("unexpected ListCancelTargets")
}

func (s *registrationInstanceStub) LockForExecution(context.Context, int64) (*repository.ExecutionInstance, error) {
	return s.f.instance, s.f.lockErr
}
func (s *registrationInstanceStub) DatabaseTime(context.Context) (time.Time, error) {
	s.f.clockRead = true
	return s.f.now, s.f.clockErr
}
func (s *registrationInstanceStub) StartExecution(_ context.Context, id int64, start, deadline time.Time) (bool, error) {
	if !s.f.clockRead || id != 11 || !start.Equal(s.f.now) || !deadline.Equal(start.Add(900*time.Second)) {
		s.t.Fatal("invalid parent execution times or ordering")
	}
	s.f.started = true
	return s.f.startChanged, s.f.startErr
}
func (*registrationInstanceStub) HasActive(context.Context, int64) (bool, error) {
	panic("unexpected HasActive")
}
func (*registrationInstanceStub) HasActiveForGroup(context.Context, int64) (bool, error) {
	panic("unexpected HasActiveForGroup")
}
func (*registrationInstanceStub) FindManual(context.Context, int64, uuid.UUID) (*repository.ManualIdentity, error) {
	panic("unexpected FindManual")
}
func (*registrationInstanceStub) Insert(context.Context, *model.JobInstance) (int64, bool, error) {
	panic("unexpected Insert")
}
func (*registrationInstanceStub) MaxWaitingID(context.Context) (int64, error) {
	panic("unexpected MaxWaitingID")
}
func (*registrationInstanceStub) ListWaitingIDs(context.Context, int64, int64, int) ([]int64, error) {
	panic("unexpected ListWaitingIDs")
}
func (*registrationInstanceStub) LoadWaiting(context.Context, []int64) ([]repository.WaitingInstance, error) {
	panic("unexpected LoadWaiting")
}

type registrationTaskStub struct {
	f *registrationFixture
	t *testing.T
}

func (*registrationTaskStub) FindUnboundTargets(context.Context, []int64) ([]repository.UnboundTarget, error) {
	panic("unexpected FindUnboundTargets")
}

func (*registrationTaskStub) ListUnfinishedIDs(context.Context, int64, int) ([]int64, error) {
	panic("unexpected ListUnfinishedIDs")
}
func (*registrationTaskStub) CancelUnfinished(context.Context, repository.TaskCancellation) (int64, error) {
	panic("unexpected CancelUnfinished")
}

func (*registrationTaskStub) ListUnboundCandidates(context.Context, time.Time, time.Time, *repository.UnboundCursor, int) ([]repository.UnboundCandidate, error) {
	panic("unexpected ListUnboundCandidates")
}

func (*registrationTaskStub) SettleOrdinaryUnknown(context.Context, repository.UnknownSettlement) (*repository.SettledTask, error) {
	panic("unexpected SettleOrdinaryUnknown")
}

func (*registrationTaskStub) ListRetryCandidates(context.Context, time.Time, *repository.RetryCursor, int) ([]repository.RetryCandidate, error) {
	panic("unexpected ListRetryCandidates")
}

func (s *registrationTaskStub) RegisterOrdinaryRetry(_ context.Context, write repository.RetryRegistration) (*repository.RegisteredTask, error) {
	if !s.f.clockRead || !write.RegisteredAt.Equal(s.f.now) {
		s.t.Fatal("retry registration did not use the fresh database time")
	}
	s.f.retryCalls++
	s.f.retryWrite = write
	return s.f.retryTask, s.f.retryErr
}

func (*registrationTaskStub) ListByInstance(context.Context, int64, int64, int64) ([]repository.TaskSummary, int64, error) {
	panic("unexpected ListByInstance")
}

func (s *registrationTaskStub) InsertOrdinary(_ context.Context, id int64, start time.Time) (int64, error) {
	if !s.f.started || id != 11 || !start.Equal(s.f.now) {
		s.t.Fatal("Task insert did not follow the parent start")
	}
	s.f.inserted = true
	return s.f.taskID, s.f.insertErr
}
func (*registrationTaskStub) BindExecutor(context.Context, repository.ExecutorBinding) (bool, error) {
	panic("unexpected BindExecutor")
}

func (*registrationTaskStub) SaveDispatchBusy(context.Context, repository.DispatchBusyWrite) (bool, error) {
	panic("unexpected SaveDispatchBusy")
}

func (*registrationTaskStub) SaveDispatchRejection(context.Context, repository.DispatchRejectionWrite) (bool, error) {
	panic("unexpected SaveDispatchRejection")
}

func (*registrationTaskStub) FindResultTarget(context.Context, int64) (*repository.TaskResultTarget, error) {
	panic("unexpected FindResultTarget")
}

func (*registrationTaskStub) SaveResult(context.Context, repository.TaskResultWrite) (bool, error) {
	panic("unexpected SaveResult")
}

func (*registrationTaskStub) CheckResultData(context.Context, model.TaskResultData) (bool, error) {
	panic("unexpected CheckResultData")
}
