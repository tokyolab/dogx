package taskresult

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

func TestNewQueueRejectsInvalidSettingsAndDoesNotStart(t *testing.T) {
	var typedNil *queueSaverStub
	var nilFunction queueSaverFunc
	for _, saver := range []ResultSaver{nil, typedNil, nilFunction} {
		if queue, err := NewQueue(saver, QueueConfig{}); queue != nil || err == nil {
			t.Fatalf("nil saver accepted: queue = %v, error = %v", queue, err)
		}
	}
	for _, config := range []QueueConfig{{Capacity: -1}, {Concurrency: -1}} {
		if queue, err := NewQueue(&queueSaverStub{}, config); queue != nil || err == nil {
			t.Fatalf("negative settings accepted: queue = %v, error = %v", queue, err)
		}
	}
	queue, err := NewQueue(&queueSaverStub{}, QueueConfig{Capacity: 1, Concurrency: 8})
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := queue.Accept(context.Background(), queueReport(1)); accepted || !errors.Is(err, ErrNotReady) {
		t.Fatalf("unstarted admission: accepted = %v, error = %v", accepted, err)
	}
	queue.Stop()
	queue.Stop()
	if err := queue.Start(); err == nil {
		t.Fatal("stopped queue was restarted")
	}
	if err := queue.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQueueDefaultsBoundRetentionAndActualSaveConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		saver := &queueSaverStub{save: func(ctx context.Context, _ Report, _ int) (SaveResult, error) {
			<-ctx.Done()
			return SaveResult{}, ctx.Err()
		}}
		queue := startTestQueue(t, saver, QueueConfig{})
		for id := int64(1); id <= 512; id++ {
			acceptQueueReport(t, queue, queueReport(id))
		}
		synctest.Wait()
		if got := len(saver.snapshot()); got != 8 {
			t.Fatalf("default save concurrency = %d, want 8", got)
		}
		if accepted, err := queue.Accept(context.Background(), queueReport(513)); err != nil || accepted {
			t.Fatalf("default capacity excluded in-flight results: accepted = %v, error = %v", accepted, err)
		}
	})
}

func TestQueueCapacityIncludesInFlightAndNilErrorsReleaseIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		saver := &queueSaverStub{save: func(ctx context.Context, report Report, _ int) (SaveResult, error) {
			if report.TaskID == 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return SaveResult{}, ctx.Err()
				}
				return SaveResult{Applied: true}, nil
			}
			return SaveResult{Reason: "no_match"}, nil
		}}
		queue := startTestQueue(t, saver, QueueConfig{Capacity: 2, Concurrency: 1})
		acceptQueueReport(t, queue, queueReport(1))
		synctest.Wait()
		acceptQueueReport(t, queue, queueReport(2))
		if accepted, err := queue.Accept(context.Background(), queueReport(3)); err != nil || accepted {
			t.Fatalf("full retention accepted another report: accepted = %v, error = %v", accepted, err)
		}
		if got := len(saver.snapshot()); got != 1 {
			t.Fatalf("save concurrency exceeded one: %d", got)
		}
		close(release)
		synctest.Wait()
		acceptQueueReport(t, queue, queueReport(3))
		synctest.Wait()
		if calls := saver.snapshot(); len(calls) != 3 || calls[0].report.TaskID != 1 || calls[1].report.TaskID != 2 || calls[2].report.TaskID != 3 {
			t.Fatalf("nil save errors did not release capacity: %+v", calls)
		}
		if err := queue.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestQueueBackoffRetainsCapacityWithoutBlockingFreshResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		saver := &queueSaverStub{save: func(_ context.Context, report Report, attempt int) (SaveResult, error) {
			if report.TaskID == 1 && attempt == 1 {
				// Applied with an error still does not confirm commit.
				return SaveResult{Applied: true}, errors.New("commit confirmation lost")
			}
			return SaveResult{}, nil
		}}
		queue := startTestQueue(t, saver, QueueConfig{Capacity: 2, Concurrency: 1})
		original := queueReport(1)
		acceptQueueReport(t, queue, original)
		synctest.Wait()
		acceptQueueReport(t, queue, queueReport(2))
		synctest.Wait()
		calls := saver.snapshot()
		if len(calls) != 2 || calls[0].report.TaskID != 1 || calls[1].report.TaskID != 2 || !calls[0].at.Equal(calls[1].at) {
			t.Fatalf("backoff occupied the only save worker: %+v", calls)
		}
		time.Sleep(799 * time.Millisecond)
		synctest.Wait()
		if len(saver.snapshot()) != 2 {
			t.Fatal("retry ignored the lower jitter bound")
		}
		time.Sleep(402 * time.Millisecond)
		synctest.Wait()
		calls = saver.snapshot()
		if len(calls) != 3 || !reflect.DeepEqual(calls[2].report, original) || calls[2].report.AttemptNo != original.AttemptNo {
			t.Fatalf("retry replaced the original report: %+v", calls)
		}
		if err := queue.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	synctest.Test(t, func(t *testing.T) {
		saver := &queueSaverStub{save: func(_ context.Context, _ Report, attempt int) (SaveResult, error) {
			if attempt == 1 {
				return SaveResult{}, errors.New("retry later")
			}
			return SaveResult{}, nil
		}}
		queue := startTestQueue(t, saver, QueueConfig{Capacity: 1, Concurrency: 4})
		acceptQueueReport(t, queue, queueReport(1))
		synctest.Wait()
		if accepted, err := queue.Accept(context.Background(), queueReport(2)); err != nil || accepted {
			t.Fatalf("backoff released the retained slot: accepted = %v, error = %v", accepted, err)
		}
		time.Sleep(1201 * time.Millisecond)
		synctest.Wait()
		acceptQueueReport(t, queue, queueReport(2))
		if err := queue.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestQueueKeepsRetryingPastTheBackoffCeiling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		saver := &queueSaverStub{save: func(_ context.Context, _ Report, attempt int) (SaveResult, error) {
			if attempt <= 8 {
				return SaveResult{}, errors.New("storage remains unavailable")
			}
			return SaveResult{}, nil
		}}
		queue := startTestQueue(t, saver, QueueConfig{Capacity: 1, Concurrency: 1})
		original := queueReport(1)
		acceptQueueReport(t, queue, original)
		synctest.Wait()
		for index, base := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second} {
			calls := saver.snapshot()
			last := calls[len(calls)-1]
			// Advance from the actual preceding attempt, so jitter cannot make
			// accumulated sleeps skip an attempt or manufacture a retry count.
			upper := time.Duration(float64(base) * 1.2)
			time.Sleep(time.Until(last.at.Add(upper + time.Nanosecond)))
			synctest.Wait()
			calls = saver.snapshot()
			if len(calls) != index+2 {
				t.Fatalf("report was lost or retried too frequently: calls = %d, want %d", len(calls), index+2)
			}
			latest := calls[len(calls)-1]
			gap := latest.at.Sub(last.at)
			if gap < time.Duration(float64(base)*0.8) || gap > upper || !reflect.DeepEqual(latest.report, original) {
				t.Fatalf("retry budget or identity changed: delay = %v, nominal = %v, report = %+v", gap, base, latest.report)
			}
		}
		if err := queue.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestQueueDueRetryPrecedesNewlyAdmittedResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		releaseSave := sync.OnceFunc(func() { close(release) })
		defer releaseSave()
		saver := &queueSaverStub{save: func(ctx context.Context, report Report, attempt int) (SaveResult, error) {
			if report.TaskID == 1 && attempt == 1 {
				return SaveResult{}, errors.New("temporary storage failure")
			}
			if report.TaskID == 2 {
				select {
				case <-release:
				case <-ctx.Done():
					return SaveResult{}, ctx.Err()
				}
			}
			return SaveResult{}, nil
		}}
		queue := startTestQueue(t, saver, QueueConfig{Capacity: 3, Concurrency: 1})
		originals := map[int64]Report{1: queueReport(1), 2: queueReport(2), 3: queueReport(3)}
		acceptQueueReport(t, queue, originals[1])
		synctest.Wait()
		acceptQueueReport(t, queue, originals[2])
		synctest.Wait()
		// B occupies the worker while A becomes eligible, before C is admitted.
		// This distinguishes earliest-due scheduling from prioritizing new reports.
		time.Sleep(1201 * time.Millisecond)
		synctest.Wait()
		if got := len(saver.snapshot()); got != 2 {
			t.Fatalf("blocked worker started another save: calls = %d", got)
		}
		acceptQueueReport(t, queue, originals[3])
		releaseSave()
		if err := queue.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		var ids []int64
		for _, call := range saver.snapshot() {
			ids = append(ids, call.report.TaskID)
			if !reflect.DeepEqual(call.report, originals[call.report.TaskID]) {
				t.Fatalf("scheduling changed the retained report: %+v", call.report)
			}
		}
		if !reflect.DeepEqual(ids, []int64{1, 2, 1, 3}) {
			t.Fatalf("new result overtook an eligible retry: %v", ids)
		}
	})
}

func TestQueueKeepsAdmissionOrderAndDoesNotDeduplicate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		saver := &queueSaverStub{save: func(ctx context.Context, report Report, _ int) (SaveResult, error) {
			if report.TaskID == 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return SaveResult{}, ctx.Err()
				}
			}
			return SaveResult{}, nil
		}}
		queue := startTestQueue(t, saver, QueueConfig{Capacity: 4, Concurrency: 1})
		acceptQueueReport(t, queue, queueReport(1))
		synctest.Wait()
		acceptQueueReport(t, queue, queueReport(30))
		acceptQueueReport(t, queue, queueReport(20))
		acceptQueueReport(t, queue, queueReport(20))
		close(release)
		if err := queue.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		var ids []int64
		for _, call := range saver.snapshot() {
			ids = append(ids, call.report.TaskID)
		}
		if !reflect.DeepEqual(ids, []int64{1, 30, 20, 20}) {
			t.Fatalf("equal-due results lost admission order or were deduplicated: %v", ids)
		}
	})
}

func TestQueueAdmissionContextDoesNotCancelAcceptedWorkAndWaitDoesNotStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		saver := &queueSaverStub{save: func(ctx context.Context, _ Report, _ int) (SaveResult, error) {
			select {
			case <-release:
				return SaveResult{}, nil
			case <-ctx.Done():
				return SaveResult{}, ctx.Err()
			}
		}}
		queue := startTestQueue(t, saver, QueueConfig{Capacity: 2, Concurrency: 1})
		canceled, stop := context.WithCancel(context.Background())
		stop()
		if accepted, err := queue.Accept(canceled, queueReport(99)); accepted || !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled admission retained a report: accepted = %v, error = %v", accepted, err)
		}
		request, cancelRequest := context.WithCancel(context.Background())
		if accepted, err := queue.Accept(request, queueReport(1)); !accepted || err != nil {
			t.Fatalf("admission failed: accepted = %v, error = %v", accepted, err)
		}
		cancelRequest()
		synctest.Wait()
		calls := saver.snapshot()
		if len(calls) != 1 || calls[0].ctx.Err() != nil {
			t.Fatal("completed admission context canceled retained work")
		}
		if err := queue.Wait(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait claimed the blocked saver had finished: %v", err)
		}
		acceptQueueReport(t, queue, queueReport(2))
		if err := queue.Start(); err == nil {
			t.Fatal("queue started duplicate workers")
		}
		queue.Stop()
		queue.Stop()
		if accepted, err := queue.Accept(context.Background(), queueReport(3)); accepted || !errors.Is(err, ErrNotReady) {
			t.Fatalf("stopped admission: accepted = %v, error = %v", accepted, err)
		}
		close(release)
		if err := queue.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(saver.snapshot()) != 2 {
			t.Fatal("Stop discarded an already accepted report")
		}
		if err := queue.Start(); err == nil {
			t.Fatal("drained queue was restarted")
		}
	})
}

func TestQueueDrainDeadlineDoesNotClaimWorkerCompletionOrExtendOnRepeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		releaseSave := sync.OnceFunc(func() { close(release) })
		saver := &queueSaverStub{save: func(context.Context, Report, int) (SaveResult, error) {
			// Deliberately ignore cancellation to distinguish a shutdown budget
			// expiring from actual completion of dependency use.
			<-release
			return SaveResult{}, nil
		}}
		queue := startTestQueue(t, saver, QueueConfig{Capacity: 2, Concurrency: 1})
		defer releaseSave()
		acceptQueueReport(t, queue, queueReport(1))
		synctest.Wait()
		acceptQueueReport(t, queue, queueReport(2))
		first, cancelFirst := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelFirst()
		firstResult := make(chan error, 1)
		go func() { firstResult <- queue.Drain(first) }()
		synctest.Wait()
		later, cancelLater := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelLater()
		laterResult := make(chan error, 1)
		go func() { laterResult <- queue.Drain(later) }()
		synctest.Wait()
		time.Sleep(11 * time.Second)
		synctest.Wait()
		if err := <-firstResult; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first drain result = %v", err)
		}
		calls := saver.snapshot()
		if len(calls) != 1 || calls[0].ctx.Err() == nil {
			t.Fatal("later Drain extended the shared shutdown budget or started pending work")
		}
		if err := queue.Wait(first); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Wait claimed an unreturned Save was finished: %v", err)
		}
		select {
		case err := <-laterResult:
			t.Fatalf("later Drain claimed completion while the saver was still blocked: %v", err)
		default:
		}
		if accepted, err := queue.Accept(context.Background(), queueReport(3)); accepted || !errors.Is(err, ErrNotReady) {
			t.Fatalf("Drain left admission open: accepted = %v, error = %v", accepted, err)
		}
		releaseSave()
		if err := queue.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := <-laterResult; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("later Drain forgot the shared deadline: %v", err)
		}
		if len(saver.snapshot()) != 1 {
			t.Fatal("expired shutdown retried or began pending work")
		}
	})
}

func TestQueueStopRacingAdmissionRetainsExactlyTheAcceptedReports(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		saver := &queueSaverStub{save: func(ctx context.Context, _ Report, _ int) (SaveResult, error) {
			select {
			case <-release:
				return SaveResult{}, nil
			case <-ctx.Done():
				return SaveResult{}, ctx.Err()
			}
		}}
		queue := startTestQueue(t, saver, QueueConfig{Capacity: 17, Concurrency: 2})
		acceptQueueReport(t, queue, queueReport(1))
		synctest.Wait()
		type admission struct {
			id       int64
			accepted bool
			err      error
		}
		start := make(chan struct{})
		results := make(chan admission, 16)
		for id := int64(2); id <= 17; id++ {
			go func() {
				<-start
				accepted, err := queue.Accept(context.Background(), queueReport(id))
				results <- admission{id: id, accepted: accepted, err: err}
			}()
		}
		go func() { <-start; queue.Stop() }()
		close(start)
		synctest.Wait()
		accepted := map[int64]bool{1: true}
		for range 16 {
			result := <-results
			if result.accepted {
				if result.err != nil {
					t.Fatal("admission returned both accepted and an error")
				}
				accepted[result.id] = true
			} else if !errors.Is(result.err, ErrNotReady) {
				t.Fatalf("racing stop lost an admission outside its atomic boundary: %v", result.err)
			}
		}
		close(release)
		if err := queue.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		calls := saver.snapshot()
		if len(calls) != len(accepted) {
			t.Fatalf("stop lost accepted reports: saved = %d, accepted = %d", len(calls), len(accepted))
		}
		for _, call := range calls {
			if !accepted[call.report.TaskID] {
				t.Fatalf("saved a rejected or repeated report: %d", call.report.TaskID)
			}
			delete(accepted, call.report.TaskID)
		}
	})
}

func TestQueueRetainsPanicsAndLogsOnlySafeFailureClassifications(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		previous := logx.Reset()
		logx.SetWriter(logx.NewWriter(&logs))
		t.Cleanup(func() { logx.SetWriter(previous) })
		saver := &queueSaverStub{save: func(_ context.Context, report Report, attempt int) (SaveResult, error) {
			if attempt == 1 {
				if report.TaskID == 1 {
					panic("private-panic-marker")
				}
				return SaveResult{}, errors.New("private-driver-marker")
			}
			return SaveResult{}, nil
		}}
		queue := startTestQueue(t, saver, QueueConfig{Capacity: 2, Concurrency: 1})
		for _, id := range []int64{1, 2} {
			report := queueReport(id)
			report.Summary, report.Data = "private-summary-marker", json.RawMessage(`"private-data-marker"`)
			acceptQueueReport(t, queue, report)
		}
		synctest.Wait()
		if !strings.Contains(logs.String(), "save_panic") || !strings.Contains(logs.String(), "storage_error") {
			t.Fatal("retry diagnostics omitted safe failure classifications")
		}
		for _, secret := range []string{"private-panic-marker", "private-driver-marker", "private-summary-marker", "private-data-marker"} {
			if strings.Contains(logs.String(), secret) {
				t.Fatalf("retry diagnostics leaked %s", secret)
			}
		}
		time.Sleep(1201 * time.Millisecond)
		synctest.Wait()
		calls := saver.snapshot()
		if len(calls) != 4 {
			t.Fatalf("a panic lost the result or worker: attempts = %d", len(calls))
		}
		for _, id := range []int64{1, 2} {
			var attempts []queueSaveCall
			for _, call := range calls {
				if call.report.TaskID == id {
					attempts = append(attempts, call)
				}
			}
			if len(attempts) != 2 || !reflect.DeepEqual(attempts[0].report, attempts[1].report) {
				t.Fatalf("platform failure changed the retained report: %+v", attempts)
			}
		}
		if err := queue.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestQueueRetryLoggingDoesNotHoldTheAdmissionLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := &queueLogGate{entered: make(chan struct{}), release: make(chan struct{})}
		releaseLogs := sync.OnceFunc(func() { close(writer.release) })
		defer releaseLogs()
		previous := logx.Reset()
		logx.SetWriter(logx.NewWriter(writer))
		t.Cleanup(func() { logx.SetWriter(previous) })
		saver := &queueSaverStub{save: func(_ context.Context, report Report, attempt int) (SaveResult, error) {
			if report.TaskID == 1 && attempt == 1 {
				return SaveResult{}, errors.New("temporary failure")
			}
			return SaveResult{}, nil
		}}
		queue := startTestQueue(t, saver, QueueConfig{Capacity: 2, Concurrency: 1})
		acceptQueueReport(t, queue, queueReport(1))
		<-writer.entered
		result := make(chan error, 1)
		go func() {
			accepted, err := queue.Accept(context.Background(), queueReport(2))
			if err == nil && !accepted {
				err = errors.New("capacity was unexpectedly full")
			}
			result <- err
		}()
		synctest.Wait()
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("blocked logging prevented independent admission")
		}
		releaseLogs()
		if err := queue.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func startTestQueue(t *testing.T, saver ResultSaver, config QueueConfig) *Queue {
	t.Helper()
	queue, err := NewQueue(saver, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = queue.Drain(ctx)
		if err := queue.Wait(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return queue
}

func acceptQueueReport(t *testing.T, queue *Queue, report Report) {
	t.Helper()
	if accepted, err := queue.Accept(context.Background(), report); !accepted || err != nil {
		t.Fatalf("report %d admission: accepted = %v, error = %v", report.TaskID, accepted, err)
	}
}

func queueReport(id int64) Report {
	return Report{GroupID: 41, JobID: 21, InstanceID: 11, TaskID: id, AttemptNo: 2,
		ExecutorInstanceID: saverExecutorID, Outcome: OutcomeSuccess,
		Summary: "completed", Data: json.RawMessage(`{"value":9223372036854775807}`)}
}

type queueSaveCall struct {
	ctx     context.Context
	report  Report
	at      time.Time
	attempt int
}

type queueSaverFunc func(context.Context, Report) (SaveResult, error)

func (save queueSaverFunc) Save(ctx context.Context, report Report) (SaveResult, error) {
	return save(ctx, report)
}

type queueLogGate struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (writer *queueLogGate) Write(data []byte) (int, error) {
	writer.once.Do(func() { close(writer.entered) })
	<-writer.release
	return len(data), nil
}

// queueSaverStub records attempts before calling the configured behavior. Its
// synchronization supports concurrent workers without depending on queue internals.
type queueSaverStub struct {
	mu       sync.Mutex
	calls    []queueSaveCall
	attempts map[int64]int
	save     func(context.Context, Report, int) (SaveResult, error)
}

func (s *queueSaverStub) Save(ctx context.Context, report Report) (SaveResult, error) {
	s.mu.Lock()
	if s.attempts == nil {
		s.attempts = make(map[int64]int)
	}
	s.attempts[report.TaskID]++
	attempt := s.attempts[report.TaskID]
	s.calls = append(s.calls, queueSaveCall{ctx: ctx, report: report, at: time.Now(), attempt: attempt})
	s.mu.Unlock()
	if s.save != nil {
		return s.save(ctx, report, attempt)
	}
	return SaveResult{}, nil
}

func (s *queueSaverStub) snapshot() []queueSaveCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]queueSaveCall(nil), s.calls...)
}
