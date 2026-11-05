package instancestop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/zeromicro/go-zero/core/logx"
)

func TestQueueValidatesWithoutStartingOrAdmitting(t *testing.T) {
	var typedNil *queueProcessorStub
	for _, processor := range []processor{nil, typedNil} {
		if q, err := NewQueue(processor, queueConfig()); err == nil || q != nil {
			t.Fatal("nil processor was accepted")
		}
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.Capacity = 0 },
		func(c *Config) { c.Concurrency = -1 },
		func(c *Config) { c.RunTimeout = 0 },
		func(c *Config) { c.MaxBatches = -1 },
	} {
		config := queueConfig()
		change(&config)
		if q, err := NewQueue(&queueProcessorStub{}, config); err == nil || q != nil {
			t.Fatal("nonpositive settings were accepted")
		}
	}
	processor := &queueProcessorStub{}
	q := newTestQueue(t, processor, queueConfig())
	if q.Hint(1) || len(processor.snapshot()) != 0 || q.Drain(nil) == nil || q.Wait(nil) == nil {
		t.Fatal("construction admitted work or nil lifecycle contexts were accepted")
	}
	q.Stop()
	q.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q.Wait(ctx); err != nil {
		t.Fatal("unused stopped queue did not complete")
	}
	if err := q.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if q.Start() == nil || q.Hint(1) {
		t.Fatal("stopped queue reopened admission")
	}
}

func TestQueueMergesWaitingAndProcessingHintsWithoutFollowups(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn := make(chan struct{})
		processor := &queueProcessorStub{call: func(context.Context, int64, int) (BatchResult, error) {
			<-firstReturn
			return BatchResult{}, nil
		}}
		q := startTestQueue(t, processor, Config{Capacity: 2, Concurrency: 1, RunTimeout: 2 * time.Second, MaxBatches: 4})
		if q.Hint(0) || q.Hint(-1) || q.Start() == nil || !q.Hint(1) {
			t.Fatal("invalid admission or repeated start behavior")
		}
		synctest.Wait()
		if len(processor.snapshot()) != 1 || !q.Hint(2) {
			t.Fatal("first hint was delayed or the second position was unavailable")
		}
		for range 100 {
			if !q.Hint(1) || !q.Hint(2) {
				t.Fatal("existing ID did not merge while full")
			}
		}
		if q.Hint(3) {
			t.Fatal("processing and waiting were counted as separate capacities")
		}
		close(firstReturn)
		synctest.Wait()
		if got := queueCallIDs(processor.snapshot()); !reflect.DeepEqual(got, []int64{1, 2}) {
			t.Fatalf("duplicate hints created extra work: %v", got)
		}
		if !q.Hint(3) {
			t.Fatal("completed work did not release capacity")
		}
		stopTestQueue(t, q)
	})
}

func TestQueueBoundsConcurrentCallsAndNeverDuplicatesAnInstance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		returnCalls := make(chan struct{})
		var active, maximum atomic.Int32
		var mu sync.Mutex
		activeIDs := make(map[int64]bool)
		processor := &queueProcessorStub{call: func(_ context.Context, id int64, _ int) (BatchResult, error) {
			mu.Lock()
			if activeIDs[id] {
				t.Error("same Instance was processed concurrently")
			}
			activeIDs[id] = true
			mu.Unlock()
			current := active.Add(1)
			for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
			}
			<-returnCalls
			active.Add(-1)
			mu.Lock()
			delete(activeIDs, id)
			mu.Unlock()
			return BatchResult{}, nil
		}}
		q := startTestQueue(t, processor, Config{Capacity: 8, Concurrency: 3, RunTimeout: 2 * time.Second, MaxBatches: 4})
		for id := int64(1); id <= 8; id++ {
			if !q.Hint(id) {
				t.Fatal("configured capacity was unavailable")
			}
		}
		synctest.Wait()
		if active.Load() != 3 || len(processor.snapshot()) != 3 || q.Hint(9) {
			t.Fatal("worker or retained-ID bound was exceeded")
		}
		for id := int64(1); id <= 8; id++ {
			if !q.Hint(id) {
				t.Fatal("duplicate hint was rejected")
			}
		}
		close(returnCalls)
		stopTestQueue(t, q)
		if maximum.Load() != 3 || active.Load() != 0 || len(processor.snapshot()) != 8 {
			t.Fatal("duplicate hints expanded work or workers were released early")
		}
	})
}

func TestQueueBatchAllowanceRotatesAtTailWithoutReleasingItsSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn, secondReturn := make(chan struct{}), make(chan struct{})
		largeBatches := 0
		processor := &queueProcessorStub{call: func(_ context.Context, id int64, _ int) (BatchResult, error) {
			if id == 2 {
				<-secondReturn
				return BatchResult{}, nil
			}
			largeBatches++
			if largeBatches == 1 {
				<-firstReturn
			}
			// Even a committed batch that updated zero rows consumes a turn.
			return BatchResult{More: largeBatches <= 4}, nil
		}}
		q := startTestQueue(t, processor, Config{Capacity: 2, Concurrency: 1, RunTimeout: 2 * time.Second, MaxBatches: 2})
		q.Hint(1)
		synctest.Wait()
		q.Hint(2)
		close(firstReturn)
		synctest.Wait()
		if got := queueCallIDs(processor.snapshot()); !reflect.DeepEqual(got, []int64{1, 1, 2}) || q.Hint(3) || !q.Hint(1) {
			t.Fatalf("rotation did not preserve the position or yield to the smaller Instance: %v", got)
		}
		close(secondReturn)
		stopTestQueue(t, q)
		if got := queueCallIDs(processor.snapshot()); !reflect.DeepEqual(got, []int64{1, 1, 2, 1, 1, 1}) {
			t.Fatalf("successful remaining batches were lost or duplicated: %v", got)
		}
	})
}

func TestQueueTimeAllowanceYieldsOnlyAfterActualBatchReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		processor := &queueProcessorStub{call: func(ctx context.Context, _ int64, call int) (BatchResult, error) {
			if _, hasDeadline := ctx.Deadline(); hasDeadline {
				t.Error("turn allowance became a transaction deadline")
			}
			if call == 1 {
				time.Sleep(3 * time.Second)
				if ctx.Err() != nil {
					t.Error("turn allowance canceled an in-flight transaction")
				}
				return BatchResult{More: true}, nil
			}
			return BatchResult{}, nil
		}}
		q := startTestQueue(t, processor, Config{Capacity: 2, Concurrency: 1, RunTimeout: 2 * time.Second, MaxBatches: 4})
		q.Hint(1)
		synctest.Wait()
		q.Hint(2)
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if len(processor.snapshot()) != 1 || processor.snapshot()[0].ctx.Err() != nil || q.Hint(3) {
			t.Fatal("elapsed turn allowance released active resources")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if got := queueCallIDs(processor.snapshot()); !reflect.DeepEqual(got, []int64{1, 2, 1}) {
			t.Fatalf("time allowance did not yield before starting the next batch: %v", got)
		}
		stopTestQueue(t, q)
	})
}

func TestQueueTimeAllowanceCoversAllBatchesInOneTurn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		largeBatches := 0
		processor := &queueProcessorStub{call: func(_ context.Context, id int64, _ int) (BatchResult, error) {
			if id == 1 {
				largeBatches++
				time.Sleep(600 * time.Millisecond)
				return BatchResult{More: largeBatches <= 2}, nil
			}
			return BatchResult{}, nil
		}}
		q := startTestQueue(t, processor, Config{Capacity: 2, Concurrency: 1, RunTimeout: time.Second, MaxBatches: 10})
		q.Hint(1)
		synctest.Wait()
		q.Hint(2)
		stopTestQueue(t, q)
		calls := processor.snapshot()
		if got := queueCallIDs(calls); !reflect.DeepEqual(got, []int64{1, 1, 2, 1}) || calls[2].started.Sub(calls[0].started) != 1200*time.Millisecond {
			t.Fatalf("time allowance restarted for each batch: %v", got)
		}
	})
}

func TestQueueConflictFailureAndPanicDiscardMergedHints(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		err        error
		panicValue bool
	}{
		{name: "parent conflict", err: fmt.Errorf("wrapped conflict: %w", repository.ErrInstanceLocked)},
		{name: "technical failure", err: errors.New("synthetic private database failure")},
		{name: "unconfirmed commit", err: errors.New("synthetic private commit confirmation lost")},
		{name: "panic", panicValue: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				firstReturn := make(chan struct{})
				processor := &queueProcessorStub{call: func(_ context.Context, _ int64, call int) (BatchResult, error) {
					if call == 1 {
						<-firstReturn
						if scenario.panicValue {
							panic("synthetic private panic diagnostic")
						}
						return BatchResult{More: true}, scenario.err
					}
					return BatchResult{}, nil
				}}
				q := startTestQueue(t, processor, Config{Capacity: 2, Concurrency: 1, RunTimeout: 2 * time.Second, MaxBatches: 4})
				q.Hint(1)
				synctest.Wait()
				for range 50 {
					q.Hint(1)
				}
				q.Hint(2)
				close(firstReturn)
				time.Sleep(time.Minute)
				synctest.Wait()
				if got := queueCallIDs(processor.snapshot()); !reflect.DeepEqual(got, []int64{1, 2}) {
					t.Fatalf("failed turn immediately retried from a merged hint: %v", got)
				}
				if !q.Hint(1) {
					t.Fatal("later recovery could not admit a failed Instance")
				}
				stopTestQueue(t, q)
				if got := queueCallIDs(processor.snapshot()); !reflect.DeepEqual(got, []int64{1, 2, 1}) {
					t.Fatalf("failure was treated as a permanent retry limit: %v", got)
				}
			})
		})
	}
}

func TestQueueStopDrainsSuccessfulRotationsWithoutCancelingBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn := make(chan struct{})
		processor := &queueProcessorStub{call: func(ctx context.Context, _ int64, call int) (BatchResult, error) {
			if call == 1 {
				<-firstReturn
			}
			if ctx.Err() != nil {
				t.Error("Stop canceled admitted work")
			}
			return BatchResult{More: call == 1}, nil
		}}
		q := startTestQueue(t, processor, Config{Capacity: 2, Concurrency: 1, RunTimeout: time.Second, MaxBatches: 1})
		q.Hint(1)
		synctest.Wait()
		q.Hint(1)
		q.Hint(2)
		q.Stop()
		q.Stop()
		if q.Hint(1) || q.Hint(3) {
			t.Fatal("Stop admitted new or duplicate hints")
		}
		close(firstReturn)
		if err := q.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := queueCallIDs(processor.snapshot()); !reflect.DeepEqual(got, []int64{1, 2, 1}) {
			t.Fatalf("graceful shutdown discarded successful remaining work: %v", got)
		}
	})
}

func TestQueueDrainCancelsButWaitsForActualExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn := make(chan struct{})
		processor := &queueProcessorStub{call: func(ctx context.Context, _ int64, _ int) (BatchResult, error) {
			<-firstReturn
			return BatchResult{More: true}, ctx.Err()
		}}
		q := startTestQueue(t, processor, Config{Capacity: 2, Concurrency: 1, RunTimeout: time.Second, MaxBatches: 4})
		q.Hint(1)
		synctest.Wait()
		q.Hint(2)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := q.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("unexpected drain result: %v", err)
		}
		synctest.Wait()
		if processor.snapshot()[0].ctx.Err() == nil || !errors.Is(q.Wait(ctx), context.DeadlineExceeded) {
			t.Fatal("drain failed to cancel or reported completion before actual return")
		}
		q.mu.Lock()
		retained := len(q.entries)
		q.mu.Unlock()
		if retained != 2 || len(processor.snapshot()) != 1 {
			t.Fatal("shutdown released positions or started pending work before actual exit")
		}
		close(firstReturn)
		if err := q.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(processor.snapshot()) != 1 || !errors.Is(q.Drain(context.Background()), context.DeadlineExceeded) {
			t.Fatal("shutdown restarted queued work or forgot its original deadline")
		}
	})
}

func TestQueueDrainSharesItsDeadlineWithLaterBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn := make(chan struct{})
		processor := &queueProcessorStub{call: func(_ context.Context, _ int64, call int) (BatchResult, error) {
			if call == 1 {
				<-firstReturn
			}
			return BatchResult{}, nil
		}}
		q := startTestQueue(t, processor, Config{Capacity: 2, Concurrency: 1, RunTimeout: 2 * time.Second, MaxBatches: 4})
		q.Hint(1)
		synctest.Wait()
		q.Hint(2)
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		sharedDeadline, _ := ctx.Deadline()
		drained := make(chan error, 1)
		go func() { drained <- q.Drain(ctx) }()
		synctest.Wait()
		time.Sleep(100 * time.Millisecond)
		close(firstReturn)
		if err := <-drained; err != nil {
			t.Fatal(err)
		}
		calls := processor.snapshot()
		if len(calls) != 2 {
			t.Fatal("admitted work was not drained")
		}
		deadline, ok := calls[1].ctx.Deadline()
		if !ok || !deadline.Equal(sharedDeadline) {
			t.Fatal("later work received a fresh shutdown budget")
		}
	})
}

func TestQueueLaterDrainAndWaitDoNotCancelTheFirstDrainBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn := make(chan struct{})
		processor := &queueProcessorStub{call: func(context.Context, int64, int) (BatchResult, error) {
			<-firstReturn
			return BatchResult{}, nil
		}}
		q := startTestQueue(t, processor, queueConfig())
		q.Hint(1)
		synctest.Wait()
		canceled, cancelWait := context.WithCancel(context.Background())
		cancelWait()
		if !errors.Is(q.Wait(canceled), context.Canceled) || processor.snapshot()[0].ctx.Err() != nil {
			t.Fatal("Wait canceled processor work")
		}
		firstCtx, cancelFirst := context.WithTimeout(context.Background(), time.Second)
		defer cancelFirst()
		firstDrain := make(chan error, 1)
		go func() { firstDrain <- q.Drain(firstCtx) }()
		synctest.Wait()
		secondCtx, cancelSecond := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancelSecond()
		if !errors.Is(q.Drain(secondCtx), context.DeadlineExceeded) || processor.snapshot()[0].ctx.Err() != nil {
			t.Fatal("later waiter replaced the shared shutdown budget")
		}
		close(firstReturn)
		if err := <-firstDrain; err != nil {
			t.Fatal(err)
		}
	})
}

func TestQueueLogsSaturationAndFailureWithoutPrivateDiagnostics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs queueLogBuffer
		previous := logx.Reset()
		logx.SetWriter(logx.NewWriter(&logs))
		defer logx.SetWriter(previous)
		firstReturn := make(chan struct{})
		processor := &queueProcessorStub{call: func(_ context.Context, _ int64, call int) (BatchResult, error) {
			if call == 1 {
				<-firstReturn
				return BatchResult{}, errors.New("synthetic-private-database-diagnostic")
			}
			return BatchResult{}, repository.ErrInstanceLocked
		}}
		q := startTestQueue(t, processor, Config{Capacity: 1, Concurrency: 1, RunTimeout: time.Second, MaxBatches: 4})
		q.Hint(1)
		synctest.Wait()
		for range 20 {
			q.Hint(2)
		}
		close(firstReturn)
		synctest.Wait()
		q.Hint(2)
		stopTestQueue(t, q)
		output := logs.String()
		if strings.Count(output, "job instance stop hint dropped because the queue is full") != 1 ||
			strings.Count(output, "job instance stop processing failed") != 1 || strings.Contains(output, "synthetic-private") {
			t.Fatal("logging repeated saturation, classified lock contention as failure or exposed diagnostics")
		}
	})
}

func queueConfig() Config {
	return Config{Capacity: 256, Concurrency: 2, RunTimeout: 2 * time.Second, MaxBatches: 4}
}

func newTestQueue(t *testing.T, processor processor, config Config) *Queue {
	t.Helper()
	q, err := NewQueue(processor, config)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func startTestQueue(t *testing.T, processor processor, config Config) *Queue {
	t.Helper()
	q := newTestQueue(t, processor, config)
	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	return q
}

func stopTestQueue(t *testing.T, q *Queue) {
	t.Helper()
	q.Stop()
	if err := q.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func queueCallIDs(calls []queueBatchCall) []int64 {
	ids := make([]int64, len(calls))
	for index, call := range calls {
		ids[index] = call.id
	}
	return ids
}

type queueBatchCall struct {
	id      int64
	started time.Time
	ctx     context.Context
}

type queueProcessorStub struct {
	call  func(context.Context, int64, int) (BatchResult, error)
	mu    sync.Mutex
	calls []queueBatchCall
}

func (s *queueProcessorStub) ProcessBatch(ctx context.Context, id int64) (BatchResult, error) {
	s.mu.Lock()
	s.calls = append(s.calls, queueBatchCall{id: id, started: time.Now(), ctx: ctx})
	number := len(s.calls)
	s.mu.Unlock()
	if s.call != nil {
		return s.call(ctx, id, number)
	}
	return BatchResult{}, nil
}

func (s *queueProcessorStub) snapshot() []queueBatchCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]queueBatchCall(nil), s.calls...)
}

type queueLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *queueLogBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *queueLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}
