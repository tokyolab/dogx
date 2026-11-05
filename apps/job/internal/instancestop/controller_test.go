package instancestop

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestManualStopUsesLockedStateAndDatabaseTimeWithoutCancelingChildren(t *testing.T) {
	for _, state := range []string{"waiting", "running", "expired"} {
		t.Run(state, func(t *testing.T) {
			f := newStopFixture(t)
			if state == "waiting" {
				f.instance.Status, f.instance.StartedAt, f.instance.DeadlineAt = model.InstanceWaiting, nil, nil
			}
			if state == "expired" {
				deadline := f.now.Add(-time.Second)
				f.instance.DeadlineAt = &deadline
			}
			before := *f.instance
			decision, err := f.controller.RequestManual(context.Background(), 11)
			if err != nil || !decision.New || decision.InstanceID != 11 || decision.ExecutorGroupID != 17 || decision.Status != model.InstanceStopping ||
				decision.EndReason == nil || *decision.EndReason != model.EndReasonManualStop || f.nowait {
				t.Fatalf("manual decision=%+v err=%v", decision, err)
			}
			if !reflect.DeepEqual(f.calls, []string{"lock", "clock", "record"}) || f.record.id != 11 || f.record.expected != before.Status ||
				f.record.reason != model.EndReasonManualStop || f.record.summary != manualStopSummary || !f.record.at.Equal(f.now) || !reflect.DeepEqual(*f.instance, before) {
				t.Fatal("manual stop changed execution times, used another cause, or touched children")
			}
		})
	}
}

func TestManualStopReturnsExistingDecisionWithoutRewritingItsReason(t *testing.T) {
	for _, state := range []model.InstanceStatus{model.InstanceStopping, model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled} {
		f := newStopFixture(t)
		reason := model.EndReasonInstanceTimeout
		f.instance.Status, f.instance.EndReason = state, &reason
		f.now = time.Time{}
		decision, err := f.controller.RequestManual(context.Background(), 11)
		if err != nil || decision.New || decision.Status != state || decision.EndReason == nil || *decision.EndReason != reason || !reflect.DeepEqual(f.calls, []string{"lock"}) {
			t.Fatalf("existing decision=%+v err=%v", decision, err)
		}
		*decision.EndReason = "caller-owned copy"
		if *f.instance.EndReason != model.EndReasonInstanceTimeout {
			t.Fatal("decision exposed the repository projection's mutable reason")
		}
	}
	f := newStopFixture(t)
	f.instance.Status = model.InstanceSucceeded
	if decision, err := f.controller.RequestManual(context.Background(), 11); err != nil || decision.EndReason != nil || decision.New {
		t.Fatalf("nullable existing reason=%+v err=%v", decision, err)
	}
}

func TestManualStopFailuresNeverReturnAnAcceptedDecision(t *testing.T) {
	failure := errors.New("synthetic stop persistence failure")
	for _, scenario := range []struct {
		name   string
		change func(*stopFixture)
		want   error
	}{
		{"missing", func(f *stopFixture) { f.instance = nil }, repository.ErrInstanceNotFound},
		{"lock", func(f *stopFixture) { f.failures["lock"] = failure }, failure},
		{"clock", func(f *stopFixture) { f.failures["clock"] = failure }, failure},
		{"record", func(f *stopFixture) { f.failures["record"] = failure }, failure},
		{"commit unknown", func(f *stopFixture) { f.commitErr = failure }, failure},
		{"no parent transition", func(f *stopFixture) { f.recordChanged = false }, ErrInvalidState},
		{"empty database time", func(f *stopFixture) { f.now = time.Time{} }, ErrInvalidState},
		{"wrong identity", func(f *stopFixture) { f.instance.ID++ }, ErrInvalidState},
		{"missing executor group", func(f *stopFixture) { f.instance.ExecutorGroupID = 0 }, ErrInvalidState},
		{"unknown state", func(f *stopFixture) { f.instance.Status = 99 }, ErrInvalidState},
		{"missing start", func(f *stopFixture) { f.instance.StartedAt = nil }, ErrInvalidState},
		{"empty start", func(f *stopFixture) { f.instance.StartedAt = new(time.Time) }, ErrInvalidState},
		{"missing deadline", func(f *stopFixture) { f.instance.DeadlineAt = nil }, ErrInvalidState},
		{"deadline equals start", func(f *stopFixture) { f.instance.DeadlineAt = f.instance.StartedAt }, ErrInvalidState},
		{"already finished active parent", func(f *stopFixture) { f.instance.FinishedAt = &f.now }, ErrInvalidState},
		{"waiting but already timed", func(f *stopFixture) { f.instance.Status = model.InstanceWaiting }, ErrInvalidState},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newStopFixture(t)
			scenario.change(f)
			decision, err := f.controller.RequestManual(context.Background(), 11)
			if decision != (Decision{}) || !errors.Is(err, scenario.want) {
				t.Fatalf("unconfirmed decision=%+v err=%v", decision, err)
			}
			if f.commitErr != nil {
				if f.callbackErr != nil || f.record.reason == "" {
					t.Fatal("fixture did not lose confirmation after the write")
				}
			} else if f.callbackErr == nil {
				t.Fatal("failure did not request transaction rollback")
			}
		})
	}
}

func TestStopBatchCancelsOneBoundedPageAndRequiresAnotherTransactionToFinish(t *testing.T) {
	for _, reason := range []string{model.EndReasonManualStop, model.EndReasonInstanceTimeout} {
		for _, affected := range []int64{0, 1, 2} {
			f := newStoppingFixture(t, reason)
			f.canceled = affected
			result, err := f.controller.ProcessBatch(context.Background(), 11)
			if err != nil || result != (BatchResult{InstanceID: 11, Status: model.InstanceStopping, Canceled: affected, More: true}) || !f.nowait ||
				!reflect.DeepEqual(f.calls, []string{"lock", "list", "clock", "cancel"}) || f.limit != DefaultBatchSize {
				t.Fatalf("child batch=%+v err=%v calls=%v", result, err, f.calls)
			}
			_, summary, _ := stopOutcome(&reason)
			write := f.cancellation
			if write.InstanceID != 11 || !reflect.DeepEqual(write.TaskIDs, []int64{5, 8}) || write.Reason != reason || write.Summary != summary || !write.ProcessedAt.Equal(f.now) {
				t.Fatalf("child cancellation lost the frozen reason or batch time: %+v", write)
			}
			if f.instance.Status != model.InstanceStopping {
				t.Fatal("child cancellation changed the parent before an empty query")
			}
		}
	}
	f := newStoppingFixture(t, model.EndReasonManualStop, WithBatchSize(1))
	f.ids, f.canceled = []int64{5}, 1
	if result, err := f.controller.ProcessBatch(context.Background(), 11); err != nil || !result.More || f.limit != 1 || len(f.cancellation.TaskIDs) != 1 {
		t.Fatalf("configured child batch=%+v err=%v", result, err)
	}
}

func TestStopBatchFinishesOnlyAfterEmptyQueryAndConfirmedCommit(t *testing.T) {
	for _, reason := range []string{model.EndReasonManualStop, model.EndReasonInstanceTimeout} {
		for _, changed := range []bool{false, true} {
			f := newStoppingFixture(t, reason)
			f.ids, f.finishChanged, f.now = nil, changed, time.Time{}
			result, err := f.controller.ProcessBatch(context.Background(), 11)
			status := model.InstanceStopping
			if changed {
				status, _, _ = stopOutcome(&reason)
			}
			if err != nil || result != (BatchResult{InstanceID: 11, Status: status, Completed: changed}) ||
				!reflect.DeepEqual(f.calls, []string{"lock", "list", "finish"}) || f.finishReason != reason {
				t.Fatalf("final parent transition=%+v err=%v calls=%v", result, err, f.calls)
			}
		}
	}
	f := newStoppingFixture(t, model.EndReasonManualStop)
	f.ids = nil
	f.commitErr = errors.New("synthetic final commit acknowledgement lost")
	if result, err := f.controller.ProcessBatch(context.Background(), 11); result != (BatchResult{}) || !errors.Is(err, f.commitErr) || f.finishReason == "" {
		t.Fatalf("unconfirmed parent transition=%+v err=%v", result, err)
	}
}

func TestStopBatchPreservesMissingAndNonStoppingParents(t *testing.T) {
	for _, state := range []model.InstanceStatus{model.InstanceWaiting, model.InstanceRunning, model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled} {
		f := newStopFixture(t)
		f.instance.Status = state
		f.instance.EndReason, f.instance.FinishedAt = nil, &f.now
		result, err := f.controller.ProcessBatch(context.Background(), 11)
		if err != nil || result != (BatchResult{InstanceID: 11, Status: state}) || !reflect.DeepEqual(f.calls, []string{"lock"}) {
			t.Fatalf("non-stopping parent=%+v err=%v", result, err)
		}
	}
	f := newStopFixture(t)
	f.instance = nil
	if result, err := f.controller.ProcessBatch(context.Background(), 11); err != nil || result != (BatchResult{}) || !reflect.DeepEqual(f.calls, []string{"lock"}) {
		t.Fatalf("missing parent batch=%+v err=%v", result, err)
	}
}

func TestStopBatchErrorsDoNotPublishCanceledRowsOrMoreWork(t *testing.T) {
	failure := errors.New("synthetic cancellation failure")
	for _, scenario := range []struct {
		name   string
		change func(*stopFixture)
		want   error
	}{
		{"lock conflict", func(f *stopFixture) { f.failures["lock"] = fmt.Errorf("lock: %w", repository.ErrInstanceLocked) }, repository.ErrInstanceLocked},
		{"list failure", func(f *stopFixture) { f.failures["list"] = failure }, failure},
		{"clock failure", func(f *stopFixture) { f.failures["clock"] = failure }, failure},
		{"cancel failure", func(f *stopFixture) { f.failures["cancel"] = failure }, failure},
		{"commit unknown", func(f *stopFixture) { f.commitErr = failure }, failure},
		{"finish failure", func(f *stopFixture) { f.ids = nil; f.failures["finish"] = failure }, failure},
		{"invalid parent identity", func(f *stopFixture) { f.instance.ID++ }, ErrInvalidState},
		{"unknown parent state", func(f *stopFixture) { f.instance.Status = 99 }, ErrInvalidState},
		{"missing frozen reason", func(f *stopFixture) { f.instance.EndReason = nil }, ErrInvalidState},
		{"invalid frozen reason", func(f *stopFixture) { reason := model.EndReasonSuccess; f.instance.EndReason = &reason }, ErrInvalidState},
		{"finished stopping parent", func(f *stopFixture) { f.instance.FinishedAt = &f.now }, ErrInvalidState},
		{"invalid child id", func(f *stopFixture) { f.ids = []int64{0} }, ErrInvalidState},
		{"duplicate child id", func(f *stopFixture) { f.ids = []int64{5, 5} }, ErrInvalidState},
		{"unordered child ids", func(f *stopFixture) { f.ids = []int64{8, 5} }, ErrInvalidState},
		{"too many child ids", func(f *stopFixture) { f.ids = make([]int64, DefaultBatchSize+1) }, ErrInvalidState},
		{"negative affected count", func(f *stopFixture) { f.canceled = -1 }, ErrInvalidState},
		{"excess affected count", func(f *stopFixture) { f.canceled = 3 }, ErrInvalidState},
		{"empty batch clock", func(f *stopFixture) { f.now = time.Time{} }, ErrInvalidState},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newStoppingFixture(t, model.EndReasonManualStop)
			scenario.change(f)
			result, err := f.controller.ProcessBatch(context.Background(), 11)
			if result != (BatchResult{}) || !errors.Is(err, scenario.want) {
				t.Fatalf("failed batch=%+v err=%v", result, err)
			}
			if f.commitErr == nil && !errors.Is(f.callbackErr, scenario.want) {
				t.Fatal("batch error was hidden from transaction rollback")
			}
			if errors.Is(err, repository.ErrInstanceLocked) && !reflect.DeepEqual(f.calls, []string{"lock"}) {
				t.Fatal("NOWAIT conflict proceeded to child reads")
			}
		})
	}
}

func TestStopControllerRejectsInvalidOptionsAndCanceledCallsBeforeTransaction(t *testing.T) {
	if _, err := NewController(nil); err == nil {
		t.Fatal("nil runner accepted")
	}
	runner := func(context.Context, func(repository.Repositories) error) error {
		t.Fatal("construction invoked the runner")
		return nil
	}
	for _, option := range []Option{nil, WithSQLTimeout(0), WithSQLTimeout(-time.Second), WithBatchSize(0), WithBatchSize(-1)} {
		if _, err := NewController(runner, option); err == nil {
			t.Fatal("invalid stop option accepted")
		}
	}
	f := newStopFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, id := range []int64{0, -1, 11} {
		if result, err := f.controller.RequestManual(ctx, id); result != (Decision{}) || err == nil {
			t.Fatal("invalid or canceled manual request accepted")
		}
		if result, err := f.controller.ProcessBatch(ctx, id); result != (BatchResult{}) || err == nil {
			t.Fatal("invalid or canceled batch accepted")
		}
	}
	if f.runs != 0 {
		t.Fatal("invalid or canceled operation began a transaction")
	}
}

func TestStopStatementsUseIndependentBudgetsWithinTheOriginalCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newStopFixture(t, WithSQLTimeout(100*time.Millisecond))
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		callerDeadline, _ := ctx.Deadline()
		var contexts []context.Context
		f.onSQL = func(_ string, sqlCtx context.Context) error {
			want := time.Now().Add(100 * time.Millisecond)
			if callerDeadline.Before(want) {
				want = callerDeadline
			}
			if got, ok := sqlCtx.Deadline(); !ok || !got.Equal(want) {
				t.Fatal("SQL budget was renewed beyond the caller or shared across statements")
			}
			contexts = append(contexts, sqlCtx)
			time.Sleep(80 * time.Millisecond)
			return sqlCtx.Err()
		}
		decision, err := f.controller.RequestManual(ctx, 11)
		if err != nil || !decision.New || f.runnerContext != ctx || len(contexts) != 3 {
			t.Fatalf("bounded manual decision=%+v err=%v", decision, err)
		}
		for _, sqlCtx := range contexts {
			if sqlCtx.Err() == nil {
				t.Fatal("SQL child context was not released")
			}
		}
	})
	synctest.Test(t, func(t *testing.T) {
		f := newStoppingFixture(t, model.EndReasonManualStop, WithSQLTimeout(100*time.Millisecond))
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		f.onSQL = func(_ string, sqlCtx context.Context) error { time.Sleep(80 * time.Millisecond); return sqlCtx.Err() }
		if result, err := f.controller.ProcessBatch(ctx, 11); result != (BatchResult{}) || !errors.Is(err, context.DeadlineExceeded) || f.runnerContext != ctx || len(f.calls) != 4 {
			t.Fatalf("expired caller batch=%+v err=%v", result, err)
		}
	})
}

func TestStopExpiredSQLRequestsRollbackEvenWhenDependencyReportsSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newStopFixture(t, WithSQLTimeout(100*time.Millisecond))
		f.onSQL = func(string, context.Context) error { time.Sleep(110 * time.Millisecond); return nil }
		if result, err := f.controller.RequestManual(context.Background(), 11); result != (Decision{}) || !errors.Is(err, context.DeadlineExceeded) ||
			!errors.Is(f.callbackErr, context.DeadlineExceeded) || !reflect.DeepEqual(f.calls, []string{"lock"}) {
			t.Fatalf("late SQL decision=%+v err=%v", result, err)
		}
	})
}

func TestStopCancellationWaitsForActualSQLReturnBeforeReleasingTransaction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newStoppingFixture(t, model.EndReasonManualStop)
		release := make(chan struct{})
		releaseOnce := sync.OnceFunc(func() { close(release) })
		defer releaseOnce()
		f.onSQL = func(name string, ctx context.Context) error {
			if name == "cancel" {
				<-release
			}
			return ctx.Err()
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			result, err := f.controller.ProcessBatch(ctx, 11)
			if result != (BatchResult{}) {
				t.Error("canceled batch published success")
			}
			done <- err
		}()
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("controller returned before the canceled SQL call")
		default:
		}
		releaseOnce()
		if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(f.callbackErr, context.Canceled) {
			t.Fatalf("canceled batch error=%v", err)
		}
		if !reflect.DeepEqual(f.calls, []string{"lock", "list", "clock", "cancel"}) {
			t.Fatal("cancellation started another statement")
		}
	})
}

type stopRecord struct {
	id              int64
	expected        model.InstanceStatus
	reason, summary string
	at              time.Time
}

type stopFixture struct {
	controller                   *Controller
	instance                     *repository.StoppingInstance
	now                          time.Time
	ids                          []int64
	canceled                     int64
	recordChanged, finishChanged bool
	failures                     map[string]error
	commitErr, callbackErr       error
	runs                         int
	runnerContext                context.Context
	calls                        []string
	nowait                       bool
	limit                        int
	record                       stopRecord
	cancellation                 repository.TaskCancellation
	finishReason                 string
	onSQL                        func(string, context.Context) error
}

func newStopFixture(t *testing.T, options ...Option) *stopFixture {
	t.Helper()
	now := time.Date(2026, 11, 5, 1, 2, 3, 456000, time.UTC)
	start, deadline := now.Add(-time.Minute), now.Add(4*time.Minute)
	f := &stopFixture{now: now, ids: []int64{5, 8}, canceled: 2, recordChanged: true, finishChanged: true, failures: make(map[string]error),
		instance: &repository.StoppingInstance{ID: 11, ExecutorGroupID: 17, Status: model.InstanceRunning, StartedAt: &start, DeadlineAt: &deadline}}
	runner := func(ctx context.Context, fn func(repository.Repositories) error) error {
		f.runs++
		f.runnerContext = ctx
		f.callbackErr = fn(repository.Repositories{Instances: &stopInstanceStub{f: f}, Tasks: &stopTaskStub{f: f}})
		if f.callbackErr != nil {
			return f.callbackErr
		}
		return f.commitErr
	}
	var err error
	f.controller, err = NewController(runner, options...)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func newStoppingFixture(t *testing.T, reason string, options ...Option) *stopFixture {
	t.Helper()
	f := newStopFixture(t, options...)
	f.instance.Status, f.instance.EndReason = model.InstanceStopping, &reason
	return f
}

func (f *stopFixture) statement(name string, ctx context.Context) error {
	f.calls = append(f.calls, name)
	if f.onSQL != nil {
		if err := f.onSQL(name, ctx); err != nil {
			return err
		}
	}
	return f.failures[name]
}

type stopInstanceStub struct {
	repository.JobInstanceRepository
	f *stopFixture
}

func (s *stopInstanceStub) LockForStop(ctx context.Context, _ int64, nowait bool) (*repository.StoppingInstance, error) {
	s.f.nowait = nowait
	return s.f.instance, s.f.statement("lock", ctx)
}

func (s *stopInstanceStub) DatabaseTime(ctx context.Context) (time.Time, error) {
	return s.f.now, s.f.statement("clock", ctx)
}

func (s *stopInstanceStub) RecordStop(ctx context.Context, id int64, expected model.InstanceStatus, reason, summary string, at time.Time) (bool, error) {
	s.f.record = stopRecord{id: id, expected: expected, reason: reason, summary: summary, at: at}
	return s.f.recordChanged, s.f.statement("record", ctx)
}

func (s *stopInstanceStub) FinishStop(ctx context.Context, _ int64, reason string) (bool, error) {
	s.f.finishReason = reason
	return s.f.finishChanged, s.f.statement("finish", ctx)
}

type stopTaskStub struct {
	repository.JobTaskRepository
	f *stopFixture
}

func (s *stopTaskStub) ListUnfinishedIDs(ctx context.Context, _ int64, limit int) ([]int64, error) {
	s.f.limit = limit
	return s.f.ids, s.f.statement("list", ctx)
}

func (s *stopTaskStub) CancelUnfinished(ctx context.Context, write repository.TaskCancellation) (int64, error) {
	s.f.cancellation = write
	s.f.cancellation.TaskIDs = append([]int64(nil), write.TaskIDs...)
	return s.f.canceled, s.f.statement("cancel", ctx)
}
