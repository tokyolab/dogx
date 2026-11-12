package taskrecovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestSettlerValidatesConstructionAndCandidatesWithoutIO(t *testing.T) {
	var nilClock *settlementClockStub
	var nilStore *settlementStoreStub
	for _, dependencies := range []struct {
		clock databaseClock
		store taskStore
	}{{nil, &settlementStoreStub{}}, {nilClock, &settlementStoreStub{}}, {&settlementClockStub{}, nil}, {&settlementClockStub{}, nilStore}} {
		if got, err := NewSettler(dependencies.clock, dependencies.store); got != nil || err == nil {
			t.Fatal("nil dependency accepted")
		}
	}
	for _, option := range []Option{nil, WithObservationPeriod(0), WithObservationPeriod(-time.Second), WithOperationTimeout(0)} {
		if got, err := NewSettler(&settlementClockStub{}, &settlementStoreStub{}, option); got != nil || err == nil {
			t.Fatal("invalid option accepted")
		}
	}
	f := newSettlementFixture(t, 1)
	if result, err := f.settler.SettleBatch(context.Background(), nil); result != (BatchResult{}) || err != nil {
		t.Fatalf("empty batch=%+v err=%v", result, err)
	}
	for _, candidates := range [][]repository.UnboundCandidate{
		make([]repository.UnboundCandidate, MaxBatchSize+1),
		{{TaskID: 0, InstanceID: 101, AttemptNo: 7, AttemptRegisteredAt: f.now}},
		{{TaskID: 1, InstanceID: -1, AttemptNo: 7, AttemptRegisteredAt: f.now}},
		{{TaskID: 1, InstanceID: 101, AttemptNo: 0, AttemptRegisteredAt: f.now}},
		{{TaskID: 1, InstanceID: 101, AttemptNo: 7}},
	} {
		if result, err := f.settler.SettleBatch(context.Background(), candidates); result != (BatchResult{}) || err == nil {
			t.Fatalf("invalid batch=%+v err=%v", result, err)
		}
	}
	if f.clock.calls != 0 || f.store.reads != 0 || len(f.store.writes) != 0 {
		t.Fatal("validation or empty batch started database work")
	}
}

func TestSettlerUsesOneFreshClockAndOneReadThenOriginalOrderIndependentWrites(t *testing.T) {
	f := newSettlementFixture(t, MaxBatchSize)
	f.candidates[0], f.candidates[2] = f.candidates[2], f.candidates[0]
	result, err := f.settler.SettleBatch(context.Background(), f.candidates)
	if err != nil || result != (BatchResult{Processed: MaxBatchSize, Applied: MaxBatchSize}) || f.clock.calls != 1 || f.store.reads != 1 {
		t.Fatalf("batch=%+v err=%v clock=%d reads=%d", result, err, f.clock.calls, f.store.reads)
	}
	for index, write := range f.store.writes {
		candidate := f.candidates[index]
		if write.TaskID != candidate.TaskID || write.InstanceID != candidate.InstanceID || write.ExpectedAttemptNo != candidate.AttemptNo ||
			write.MaxAttempts != 4 || !write.ProcessedAt.Equal(f.now) || !write.ObservationCutoff.Equal(f.now.Add(-30*time.Second)) ||
			!write.RetryAt.Equal(f.now.Add(10*time.Second)) || f.hints[index] != candidate.InstanceID || f.store.ids[index] != candidate.TaskID {
			t.Fatalf("write changed original identity, order or the batch clock: %+v", write)
		}
	}
	f.now = f.now.Add(time.Minute)
	f.targets = f.targets[2:3]
	if _, err := f.settler.SettleBatch(context.Background(), f.candidates[:1]); err != nil || f.clock.calls != 2 || f.store.reads != 2 ||
		!f.store.writes[MaxBatchSize].ProcessedAt.Equal(f.now) {
		t.Fatal("a later batch reused prior time or target facts")
	}
}

func TestSettlerExcludesObsoleteFactsBeforeDecodingSnapshots(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		mutate func(*settlementFixture)
		hints  int
	}{
		{"missing", func(f *settlementFixture) { f.targets = nil }, 0},
		{"changed relation", func(f *settlementFixture) { f.targets[0].InstanceID++ }, 0},
		{"new attempt", func(f *settlementFixture) { f.targets[0].AttemptNo++ }, 0},
		{"task already closed", func(f *settlementFixture) { f.targets[0].Status = model.TaskSucceeded }, 0},
		{"identity binding only", func(f *settlementFixture) { value := uuid.New(); f.targets[0].ExecutorInstanceID = &value }, 0},
		{"address binding only", func(f *settlementFixture) { value := "127.0.0.1:12345"; f.targets[0].ExecutorAddress = &value }, 0},
		{"observation not due", func(f *settlementFixture) {
			value := f.now.Add(-29 * time.Second)
			f.targets[0].AttemptRegisteredAt = &value
		}, 0},
		{"missing registration", func(f *settlementFixture) { f.targets[0].AttemptRegisteredAt = nil }, 0},
		{"empty registration", func(f *settlementFixture) { f.targets[0].AttemptRegisteredAt = new(time.Time) }, 0},
		{"parent terminal", func(f *settlementFixture) { f.targets[0].InstanceStatus = model.InstanceFailed }, 0},
		{"parent waiting", func(f *settlementFixture) { f.targets[0].InstanceStatus = model.InstanceWaiting }, 0},
		{"parent stopping", func(f *settlementFixture) { f.targets[0].InstanceStatus = model.InstanceStopping }, 1},
		{"deadline reached", func(f *settlementFixture) { f.targets[0].DeadlineAt = &f.now }, 1},
		{"deadline passed", func(f *settlementFixture) { value := f.now.Add(-time.Second); f.targets[0].DeadlineAt = &value }, 1},
		{"missing start", func(f *settlementFixture) { f.targets[0].StartedAt = nil }, 0},
		{"missing deadline", func(f *settlementFixture) { f.targets[0].DeadlineAt = nil }, 0},
		{"empty start", func(f *settlementFixture) { f.targets[0].StartedAt = new(time.Time) }, 0},
		{"deadline before start", func(f *settlementFixture) {
			value := f.targets[0].StartedAt.Add(-time.Second)
			f.targets[0].DeadlineAt = &value
		}, 0},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newSettlementFixture(t, 1)
			f.targets[0].ExecutionConfig = json.RawMessage(`{"invalid`)
			scenario.mutate(f)
			result, err := f.settler.SettleBatch(context.Background(), f.candidates)
			if err != nil || result != (BatchResult{Processed: 1}) || len(f.store.writes) != 0 || len(f.hints) != scenario.hints {
				t.Fatalf("skip=%+v err=%v hints=%v", result, err, f.hints)
			}
		})
	}
}

func TestSettlerRequiredSnapshotFailuresStopBeforeTheAffectedCandidate(t *testing.T) {
	for _, snapshot := range []string{
		`{`, `null`, `{}`, `{"schema_version":2,"execution_mode":1,"post_enabled":false}`, `{"schema_version":1,"execution_mode":0}`,
		`{"schema_version":1,"execution_mode":1,"post_enabled":false,"retry_interval_seconds":10}`,
		`{"schema_version":1,"execution_mode":1,"post_enabled":false,"max_retries":null,"retry_interval_seconds":10}`,
		`{"schema_version":1,"execution_mode":1,"post_enabled":false,"max_retries":-1,"retry_interval_seconds":10}`,
		`{"schema_version":1,"execution_mode":1,"post_enabled":false,"max_retries":2147483648,"retry_interval_seconds":10}`,
		`{"schema_version":1,"execution_mode":1,"post_enabled":false,"max_retries":0}`,
		`{"schema_version":1,"execution_mode":1,"post_enabled":false,"max_retries":0,"retry_interval_seconds":0}`,
		`{"schema_version":1,"execution_mode":1,"post_enabled":false,"max_retries":0,"retry_interval_seconds":1.5}`,
	} {
		f := newSettlementFixture(t, 3)
		f.targets[1].ExecutionConfig = json.RawMessage(snapshot)
		result, err := f.settler.SettleBatch(context.Background(), f.candidates)
		if !errors.Is(err, ErrInvalidConfig) || result != (BatchResult{Processed: 1, Applied: 1}) || len(f.store.writes) != 1 || len(f.hints) != 1 {
			t.Fatalf("invalid snapshot batch=%+v err=%v", result, err)
		}
	}
	f := newSettlementFixture(t, 1)
	f.targets[0].ExecutionConfig = json.RawMessage(`{"schema_version":1,"execution_mode":2,"max_retries":"unused"}`)
	if result, err := f.settler.SettleBatch(context.Background(), f.candidates); !errors.Is(err, ErrInvalidConfig) || result != (BatchResult{}) || len(f.store.writes) != 0 {
		t.Fatalf("mismatched sharded snapshot=%+v err=%v", result, err)
	}
	f = newSettlementFixture(t, 1)
	f.targets[0].ExecutionConfig = json.RawMessage(`{"schema_version":1,"execution_mode":1,"post_enabled":false,"max_retries":2147483647,"retry_interval_seconds":10,"params":{"ignored":true}}`)
	if result, err := f.settler.SettleBatch(context.Background(), f.candidates); err != nil || result.Applied != 1 || f.store.writes[0].MaxAttempts != int64(math.MaxInt32)+1 {
		t.Fatalf("widened budget batch=%+v err=%v", result, err)
	}
}

func TestSettlerReadFailuresAndMalformedProjectionsHaveNoProcessedPrefix(t *testing.T) {
	failure := errors.New("synthetic database read failure")
	for _, scenario := range []struct {
		name   string
		mutate func(*settlementFixture)
		want   error
	}{
		{"clock failed", func(f *settlementFixture) {
			f.clock.read = func(context.Context) (time.Time, error) { return f.now, failure }
		}, failure},
		{"clock empty", func(f *settlementFixture) { f.now = time.Time{} }, ErrInvalidFacts},
		{"targets failed", func(f *settlementFixture) {
			f.store.read = func(context.Context, []int64) ([]repository.UnboundTarget, error) { return f.targets, failure }
		}, failure},
		{"unexpected target", func(f *settlementFixture) { f.targets[0].TaskID = 99 }, ErrInvalidFacts},
		{"duplicate target", func(f *settlementFixture) { f.targets = append(f.targets, f.targets[0]) }, ErrInvalidFacts},
		{"unknown task state", func(f *settlementFixture) { f.targets[0].Status = 99 }, ErrInvalidFacts},
		{"unknown task type", func(f *settlementFixture) { f.targets[0].TaskType = 99 }, ErrInvalidFacts},
		{"unknown parent state", func(f *settlementFixture) { f.targets[0].InstanceStatus = 99 }, ErrInvalidFacts},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newSettlementFixture(t, 1)
			scenario.mutate(f)
			if result, err := f.settler.SettleBatch(context.Background(), f.candidates); !errors.Is(err, scenario.want) || result != (BatchResult{}) ||
				len(f.store.writes) != 0 || len(f.hints) != 0 {
				t.Fatalf("failed read batch=%+v err=%v", result, err)
			}
		})
	}
}

func TestSettlerAllStagesRequireTheirFrozenModeAndPostContract(t *testing.T) {
	for _, stage := range []model.TaskType{model.TaskTypeOrdinary, model.TaskTypePrepare, model.TaskTypeShard, model.TaskTypePost} {
		for _, scenario := range []struct {
			name string
			mode int
			post string
		}{
			{"normal", 1, "false"}, {"normal with post", 1, "true"},
			{"sharded", 2, "false"}, {"sharded with post", 2, "true"},
			{"missing post", 2, ""}, {"null post", 2, "null"}, {"wrong post type", 2, `"false"`},
		} {
			t.Run(fmt.Sprintf("stage_%d/%s", stage, scenario.name), func(t *testing.T) {
				f := newSettlementFixture(t, 1)
				f.targets[0].TaskType = stage
				config := fmt.Sprintf(`{"schema_version":1,"execution_mode":%d,"max_retries":3,"retry_interval_seconds":10`, scenario.mode)
				if scenario.post != "" {
					config += `,"post_enabled":` + scenario.post
				}
				f.targets[0].ExecutionConfig = json.RawMessage(config + `}`)
				valid := (stage == model.TaskTypeOrdinary && scenario.mode == 1 && scenario.post == "false") ||
					(stage != model.TaskTypeOrdinary && scenario.mode == 2 &&
						(scenario.post == "true" || (stage != model.TaskTypePost && scenario.post == "false")))
				result, err := f.settler.SettleBatch(context.Background(), f.candidates)
				if valid {
					if err != nil || result != (BatchResult{Processed: 1, Applied: 1}) || len(f.store.writes) != 1 {
						t.Fatalf("stage settlement: result=%+v err=%v", result, err)
					}
				} else if !errors.Is(err, ErrInvalidConfig) || result != (BatchResult{}) || len(f.store.writes) != 0 || len(f.hints) != 0 {
					t.Fatalf("invalid stage advanced: result=%+v err=%v", result, err)
				}
			})
		}
	}
}

func TestSettlerZeroRowsContinueButAttemptedWriteFailuresStopWithPartialProgress(t *testing.T) {
	for _, fail := range []bool{false, true} {
		f := newSettlementFixture(t, 3)
		uncertain := errors.New("synthetic commit confirmation lost")
		f.store.write = func(_ context.Context, write repository.UnknownSettlement, number int) (*repository.SettledTask, error) {
			if number == 2 {
				if fail {
					// A returned row cannot override the runner's failed confirmation.
					return settledTask(write), uncertain
				}
				return nil, nil
			}
			return settledTask(write), nil
		}
		result, err := f.settler.SettleBatch(context.Background(), f.candidates)
		want, wantErr, hints := BatchResult{Processed: 3, Applied: 2}, error(nil), []int64{101, 103}
		if fail {
			want, wantErr, hints = BatchResult{Processed: 2, Applied: 1}, uncertain, []int64{101}
		}
		if result != want || !errors.Is(err, wantErr) || len(f.store.writes) != want.Processed || !reflect.DeepEqual(f.hints, hints) || f.store.reads != 1 {
			t.Fatalf("partial batch=%+v err=%v hints=%v", result, err, f.hints)
		}
	}
}

func TestSettlerRejectsIncorrectWriteEvidenceWithoutAnnouncingACommit(t *testing.T) {
	for _, mutate := range []func(*repository.SettledTask){
		func(row *repository.SettledTask) { row.TaskID++ }, func(row *repository.SettledTask) { row.InstanceID++ },
		func(row *repository.SettledTask) { row.AttemptNo++ }, func(row *repository.SettledTask) { row.Status = model.TaskRunning },
	} {
		f := newSettlementFixture(t, 2)
		f.store.write = func(_ context.Context, write repository.UnknownSettlement, _ int) (*repository.SettledTask, error) {
			row := settledTask(write)
			mutate(row)
			return row, nil
		}
		if result, err := f.settler.SettleBatch(context.Background(), f.candidates); result != (BatchResult{Processed: 1}) || !errors.Is(err, ErrInvalidFacts) || len(f.hints) != 0 {
			t.Fatalf("invalid write evidence batch=%+v err=%v", result, err)
		}
	}
}

func TestSettlerOperationBudgetsShareTheEarlierRunDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newSettlementFixture(t, 3, WithOperationTimeout(200*time.Millisecond), WithObservationPeriod(5*time.Second))
		ctx, cancel := context.WithTimeout(context.Background(), 450*time.Millisecond)
		defer cancel()
		deadline, _ := ctx.Deadline()
		var contexts []context.Context
		visit := func(op context.Context) {
			want := time.Now().Add(200 * time.Millisecond)
			if deadline.Before(want) {
				want = deadline
			}
			if actual, ok := op.Deadline(); !ok || !actual.Equal(want) {
				t.Fatal("operation reset the run budget or omitted its individual deadline")
			}
			contexts = append(contexts, op)
			time.Sleep(100 * time.Millisecond)
		}
		f.clock.read = func(op context.Context) (time.Time, error) { visit(op); return f.now, op.Err() }
		f.store.read = func(op context.Context, _ []int64) ([]repository.UnboundTarget, error) {
			visit(op)
			return f.targets, op.Err()
		}
		f.store.write = func(op context.Context, write repository.UnknownSettlement, _ int) (*repository.SettledTask, error) {
			visit(op)
			return settledTask(write), op.Err()
		}
		result, err := f.settler.SettleBatch(ctx, f.candidates)
		if !errors.Is(err, context.DeadlineExceeded) || result != (BatchResult{Processed: 3, Applied: 2}) || len(f.hints) != 2 ||
			!f.store.writes[0].ObservationCutoff.Equal(f.now.Add(-5*time.Second)) {
			t.Fatalf("deadline batch=%+v err=%v", result, err)
		}
		for _, op := range contexts {
			if op.Err() == nil {
				t.Fatal("operation child context remained live after return")
			}
		}
	})
}

func TestSettlerCancellationBeforeWorkOrAfterAConfirmedWriteDoesNotSkipUnstartedItems(t *testing.T) {
	f := newSettlementFixture(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := f.settler.SettleBatch(ctx, f.candidates); result != (BatchResult{}) || !errors.Is(err, context.Canceled) || f.clock.calls != 0 {
		t.Fatalf("canceled batch=%+v err=%v", result, err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	f.store.write = func(_ context.Context, write repository.UnknownSettlement, _ int) (*repository.SettledTask, error) {
		cancel()
		return settledTask(write), nil
	}
	if result, err := f.settler.SettleBatch(ctx, f.candidates); result != (BatchResult{Processed: 1, Applied: 1}) || !errors.Is(err, context.Canceled) || len(f.hints) != 1 || len(f.store.writes) != 1 {
		t.Fatalf("post-commit cancellation batch=%+v err=%v", result, err)
	}
}

func TestSettlerLateConfirmedWriteRemainsAppliedButStartsNoFurtherOperation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newSettlementFixture(t, 2)
		f.store.write = func(_ context.Context, write repository.UnknownSettlement, _ int) (*repository.SettledTask, error) {
			time.Sleep(1100 * time.Millisecond)
			return settledTask(write), nil
		}
		if result, err := f.settler.SettleBatch(context.Background(), f.candidates); result != (BatchResult{Processed: 1, Applied: 1}) ||
			!errors.Is(err, context.DeadlineExceeded) || len(f.hints) != 1 || len(f.store.writes) != 1 {
			t.Fatalf("late confirmed write batch=%+v err=%v", result, err)
		}
	})
}

func TestSettlerExpiredReadDoesNotAdvanceCandidatesOrStartWrites(t *testing.T) {
	for _, expireClock := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			f := newSettlementFixture(t, 2)
			if expireClock {
				f.clock.read = func(ctx context.Context) (time.Time, error) {
					<-ctx.Done()
					return f.now, nil
				}
			} else {
				f.store.read = func(ctx context.Context, _ []int64) ([]repository.UnboundTarget, error) {
					<-ctx.Done()
					return f.targets, nil
				}
			}
			result, err := f.settler.SettleBatch(context.Background(), f.candidates)
			if result != (BatchResult{}) || !errors.Is(err, context.DeadlineExceeded) || len(f.store.writes) != 0 || len(f.hints) != 0 ||
				(expireClock && f.store.reads != 0) {
				t.Fatalf("expired read batch=%+v err=%v", result, err)
			}
		})
	}
}

func TestSettlerWaitsForCanceledDatabaseWorkToActuallyReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newSettlementFixture(t, 2)
		release := make(chan struct{})
		releaseOnce := sync.OnceFunc(func() { close(release) })
		defer releaseOnce()
		f.store.write = func(ctx context.Context, _ repository.UnknownSettlement, _ int) (*repository.SettledTask, error) {
			<-release
			return nil, ctx.Err()
		}
		done := make(chan BatchResult, 1)
		go func() {
			result, err := f.settler.SettleBatch(context.Background(), f.candidates)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Error("late dependency did not retain its cancellation cause")
			}
			done <- result
		}()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("settlement returned before the canceled write actually exited")
		default:
		}
		releaseOnce()
		if result := <-done; result != (BatchResult{Processed: 1}) || len(f.store.writes) != 1 || len(f.hints) != 0 {
			t.Fatalf("canceled write batch=%+v", result)
		}
	})
}

type settlementFixture struct {
	settler    *Settler
	clock      *settlementClockStub
	store      *settlementStoreStub
	now        time.Time
	candidates []repository.UnboundCandidate
	targets    []repository.UnboundTarget
	hints      []int64
}

func newSettlementFixture(t *testing.T, count int, options ...Option) *settlementFixture {
	t.Helper()
	f := &settlementFixture{now: time.Date(2026, 11, 5, 1, 2, 3, 456000, time.UTC)}
	for index := range count {
		id := int64(index + 1)
		registered, started, deadline := f.now.Add(-30*time.Second), f.now.Add(-5*time.Minute), f.now.Add(5*time.Minute)
		f.candidates = append(f.candidates, repository.UnboundCandidate{TaskID: id, InstanceID: 100 + id, AttemptNo: 7, AttemptRegisteredAt: f.now.Add(-time.Minute)})
		f.targets = append(f.targets, repository.UnboundTarget{TaskID: id, InstanceID: 100 + id, AttemptNo: 7, TaskType: model.TaskTypeOrdinary,
			Status: model.TaskRunning, AttemptRegisteredAt: &registered, InstanceStatus: model.InstanceRunning, StartedAt: &started, DeadlineAt: &deadline,
			ExecutionConfig: json.RawMessage(`{"schema_version":1,"execution_mode":1,"post_enabled":false,"max_retries":3,"retry_interval_seconds":10}`)})
	}
	f.clock = &settlementClockStub{read: func(context.Context) (time.Time, error) { return f.now, nil }}
	f.store = &settlementStoreStub{read: func(context.Context, []int64) ([]repository.UnboundTarget, error) {
		if f.clock.calls == 0 {
			t.Fatal("batch read preceded fresh database time")
		}
		return f.targets, nil
	}}
	options = append(options, WithInstanceHint(func(id int64) {
		if f.store.writing {
			t.Fatal("parent hint ran before the independent write actually returned")
		}
		f.hints = append(f.hints, id)
	}))
	var err error
	f.settler, err = NewSettler(f.clock, f.store, options...)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

type settlementClockStub struct {
	calls int
	read  func(context.Context) (time.Time, error)
}

func (s *settlementClockStub) DatabaseTime(ctx context.Context) (time.Time, error) {
	s.calls++
	return s.read(ctx)
}

type settlementStoreStub struct {
	reads   int
	ids     []int64
	writes  []repository.UnknownSettlement
	writing bool
	read    func(context.Context, []int64) ([]repository.UnboundTarget, error)
	write   func(context.Context, repository.UnknownSettlement, int) (*repository.SettledTask, error)
}

func (s *settlementStoreStub) FindUnboundTargets(ctx context.Context, ids []int64) ([]repository.UnboundTarget, error) {
	s.reads++
	s.ids = append([]int64(nil), ids...)
	return s.read(ctx, ids)
}

func (s *settlementStoreStub) SettleUnknown(ctx context.Context, write repository.UnknownSettlement) (*repository.SettledTask, error) {
	s.writing = true
	defer func() { s.writing = false }()
	s.writes = append(s.writes, write)
	if s.write != nil {
		return s.write(ctx, write, len(s.writes))
	}
	return settledTask(write), nil
}

func settledTask(write repository.UnknownSettlement) *repository.SettledTask {
	return &repository.SettledTask{TaskID: write.TaskID, InstanceID: write.InstanceID, AttemptNo: write.ExpectedAttemptNo, Status: model.TaskRetryWaiting}
}
