package taskrecovery

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestRecoveryRuntimeValidatesBeforeStartingAndCannotRestart(t *testing.T) {
	clock, source, settler := &recoveryClockStub{}, &recoveryCandidateStub{}, &recoveryBatchStub{}
	var nilClock *recoveryClockStub
	var nilSource *recoveryCandidateStub
	var nilSettler *recoveryBatchStub
	for _, dependencies := range []struct {
		clock   databaseClock
		source  candidateSource
		settler batchSettler
	}{{nil, source, settler}, {nilClock, source, settler}, {clock, nil, settler}, {clock, nilSource, settler}, {clock, source, nil}, {clock, source, nilSettler}} {
		if r, err := NewRuntime(dependencies.clock, dependencies.source, dependencies.settler, recoveryConfig()); r != nil || err == nil {
			t.Fatal("nil recovery dependency accepted")
		}
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.ScanInterval = 0 }, func(c *Config) { c.PageSize = 0 },
		func(c *Config) { c.RunTimeout = 0 }, func(c *Config) { c.OperationTimeout = 0 },
		func(c *Config) { c.ObservationPeriod = 0 }, func(c *Config) { c.ScanInterval = time.Duration(1<<63 - 1) },
	} {
		config := recoveryConfig()
		change(&config)
		if r, err := NewRuntime(clock, source, settler, config); r != nil || err == nil {
			t.Fatal("invalid recovery configuration accepted")
		}
	}
	r := recoveryRuntime(t, clock, source, settler, recoveryConfig())
	if clock.calls.Load() != 0 || len(source.snapshot()) != 0 || len(settler.snapshot()) != 0 {
		t.Fatal("construction started background work")
	}
	r.Stop()
	r.Stop()
	if err := r.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err == nil {
		t.Fatal("stopped runtime restarted")
	}
}

func TestRecoveryRuntimeKeepsOneTraversalAcrossPagesAndShortBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := &recoveryClockStub{}
		source := &recoveryCandidateStub{list: func(_ context.Context, q recoveryQuery) ([]repository.UnboundCandidate, error) {
			count := q.limit
			if q.cursor != nil {
				count = 4
			}
			return recoveryPage(q.cutoff, q.cursor, count), nil
		}}
		settler := &recoveryBatchStub{}
		config := recoveryConfig()
		config.PageSize = 35
		r := recoveryRuntime(t, clock, source, settler, config)
		startRecovery(t, r)
		defer stopRecovery(t, r)
		synctest.Wait()
		queries, batches := source.snapshot(), settler.snapshot()
		if clock.calls.Load() != 1 || len(queries) != 2 || len(batches) != 4 || queries[0].cursor != nil ||
			queries[1].cursor == nil || queries[1].cursor.TaskID != 35 || !queries[1].scanNow.Equal(queries[0].scanNow) ||
			!queries[0].cutoff.Equal(queries[0].scanNow.Add(-DefaultObservationPeriod)) {
			t.Fatal("runtime lost its fixed horizon, separate page bound or short-batch boundaries")
		}
		var ids []int64
		for index, batch := range batches {
			if len(batch) != []int{16, 16, 3, 4}[index] {
				t.Fatal("runtime treated the whole page as one settlement transaction batch")
			}
			for _, candidate := range batch {
				ids = append(ids, candidate.TaskID)
				if candidate.AttemptNo != 7 || !candidate.AttemptRegisteredAt.Equal(queries[0].cutoff) {
					t.Fatal("runtime rewrote the original attempt or pagination time")
				}
			}
		}
		if len(ids) != 39 {
			t.Fatal("page size became a whole-run processing limit")
		}
		for index, id := range ids {
			if id != int64(index+1) {
				t.Fatal("settlement changed the query order")
			}
		}
	})
}

func TestRecoveryRuntimeTailWaitsThenFindsOlderLateCandidatesWithFreshTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := &recoveryClockStub{}
		var reads atomic.Int32
		source := &recoveryCandidateStub{list: func(_ context.Context, q recoveryQuery) ([]repository.UnboundCandidate, error) {
			rows := recoveryPage(q.cutoff.Add(-time.Second), nil, 1)
			if reads.Add(1) == 1 {
				rows[0].TaskID = 100
			}
			return rows, nil
		}}
		settler := &recoveryBatchStub{}
		r := recoveryRuntime(t, clock, source, settler, recoveryConfig())
		startRecovery(t, r)
		defer stopRecovery(t, r)
		synctest.Wait()
		if len(source.snapshot()) != 1 {
			t.Fatal("tail immediately restarted the traversal")
		}
		time.Sleep(7999 * time.Millisecond)
		synctest.Wait()
		if len(source.snapshot()) != 1 {
			t.Fatal("next run ignored the minimum jittered interval")
		}
		time.Sleep(4002 * time.Millisecond)
		synctest.Wait()
		queries, batches := source.snapshot(), settler.snapshot()
		if clock.calls.Load() != 2 || len(queries) != 2 || queries[1].cursor != nil || !queries[1].scanNow.After(queries[0].scanNow) ||
			len(batches) != 2 || batches[1][0].TaskID != 1 {
			t.Fatal("new traversal missed older keys or reused its previous horizon")
		}
	})
}

func TestRecoveryRuntimeErrorAdvancesOnlyTheReportedOriginalPrefix(t *testing.T) {
	for _, processed := range []int{0, 2, 4} {
		t.Run(fmt.Sprint(processed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				clock := &recoveryClockStub{}
				var reads atomic.Int32
				source := &recoveryCandidateStub{list: func(_ context.Context, q recoveryQuery) ([]repository.UnboundCandidate, error) {
					if reads.Add(1) == 1 {
						return recoveryPage(q.cutoff, nil, 4), nil
					}
					return nil, nil
				}}
				settler := &recoveryBatchStub{settle: func(context.Context, []repository.UnboundCandidate) (BatchResult, error) {
					return BatchResult{Processed: processed}, errors.New("synthetic settlement failure")
				}}
				config := recoveryConfig()
				config.ScanInterval, config.PageSize = time.Second, 4
				r := recoveryRuntime(t, clock, source, settler, config)
				startRecovery(t, r)
				defer stopRecovery(t, r)
				synctest.Wait()
				if len(source.snapshot()) != 1 || len(settler.snapshot()) != 1 {
					t.Fatal("settlement failure immediately retried or processed later candidates")
				}
				time.Sleep(1201 * time.Millisecond)
				synctest.Wait()
				queries := source.snapshot()
				if len(queries) != 2 || clock.calls.Load() != 1 || !queries[1].scanNow.Equal(queries[0].scanNow) || !queries[1].cutoff.Equal(queries[0].cutoff) {
					t.Fatal("failed run widened or restarted the unfinished traversal")
				}
				cursor := queries[1].cursor
				if (processed == 0 && cursor != nil) || (processed > 0 && (cursor == nil || cursor.TaskID != int64(processed) || !cursor.AttemptRegisteredAt.Equal(queries[0].cutoff))) {
					t.Fatal("resume cursor bypassed an unprocessed item or retried the attempted failed item")
				}
			})
		})
	}
}

func TestRecoveryRuntimeReadFailureRetainsHorizonAndDoesNotAdvance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := &recoveryClockStub{}
		var reads atomic.Int32
		source := &recoveryCandidateStub{list: func(_ context.Context, q recoveryQuery) ([]repository.UnboundCandidate, error) {
			if reads.Add(1) == 1 {
				return recoveryPage(q.cutoff, nil, 1), errors.New("synthetic candidate read failure")
			}
			return nil, nil
		}}
		settler := &recoveryBatchStub{}
		config := recoveryConfig()
		config.ScanInterval = time.Second
		r := recoveryRuntime(t, clock, source, settler, config)
		startRecovery(t, r)
		defer stopRecovery(t, r)
		synctest.Wait()
		time.Sleep(1201 * time.Millisecond)
		synctest.Wait()
		queries := source.snapshot()
		if len(queries) != 2 || clock.calls.Load() != 1 || queries[1].cursor != nil || !queries[0].scanNow.Equal(queries[1].scanNow) || len(settler.snapshot()) != 0 {
			t.Fatal("read failure consumed rows, moved the cursor or changed the traversal horizon")
		}
	})
}

func TestRecoveryRuntimeRejectsMalformedPagesBeforeAnySettlement(t *testing.T) {
	for _, mutate := range []func([]repository.UnboundCandidate) []repository.UnboundCandidate{
		func(rows []repository.UnboundCandidate) []repository.UnboundCandidate {
			rows[0].TaskID = 0
			return rows
		},
		func(rows []repository.UnboundCandidate) []repository.UnboundCandidate {
			rows[0].InstanceID = 0
			return rows
		},
		func(rows []repository.UnboundCandidate) []repository.UnboundCandidate {
			rows[0].AttemptNo = 0
			return rows
		},
		func(rows []repository.UnboundCandidate) []repository.UnboundCandidate {
			rows[0].AttemptRegisteredAt = time.Time{}
			return rows
		},
		func(rows []repository.UnboundCandidate) []repository.UnboundCandidate {
			rows[0].AttemptRegisteredAt = rows[0].AttemptRegisteredAt.Add(time.Second)
			return rows
		},
		func(rows []repository.UnboundCandidate) []repository.UnboundCandidate { rows[1] = rows[0]; return rows },
		func(rows []repository.UnboundCandidate) []repository.UnboundCandidate {
			rows[0], rows[1] = rows[1], rows[0]
			return rows
		},
		func(rows []repository.UnboundCandidate) []repository.UnboundCandidate { return append(rows, rows[0]) },
	} {
		synctest.Test(t, func(t *testing.T) {
			source := &recoveryCandidateStub{list: func(_ context.Context, q recoveryQuery) ([]repository.UnboundCandidate, error) {
				return mutate(recoveryPage(q.cutoff, nil, 2)), nil
			}}
			settler := &recoveryBatchStub{}
			config := recoveryConfig()
			config.PageSize = 2
			r := recoveryRuntime(t, &recoveryClockStub{}, source, settler, config)
			startRecovery(t, r)
			synctest.Wait()
			if len(source.snapshot()) != 1 || len(settler.snapshot()) != 0 {
				t.Fatal("invalid source page reached settlement or retried immediately")
			}
			stopRecovery(t, r)
		})
	}
}

func TestRecoveryRuntimeRejectsInvalidProgressWithoutAdvancing(t *testing.T) {
	for _, result := range []BatchResult{{Processed: -1}, {Processed: 5}, {Processed: 1, Applied: 2}, {Processed: 1}} {
		synctest.Test(t, func(t *testing.T) {
			var reads atomic.Int32
			source := &recoveryCandidateStub{list: func(_ context.Context, q recoveryQuery) ([]repository.UnboundCandidate, error) {
				if reads.Add(1) == 1 {
					return recoveryPage(q.cutoff, nil, 4), nil
				}
				return nil, nil
			}}
			settler := &recoveryBatchStub{settle: func(context.Context, []repository.UnboundCandidate) (BatchResult, error) { return result, nil }}
			config := recoveryConfig()
			config.ScanInterval = time.Second
			r := recoveryRuntime(t, &recoveryClockStub{}, source, settler, config)
			startRecovery(t, r)
			defer stopRecovery(t, r)
			synctest.Wait()
			time.Sleep(1201 * time.Millisecond)
			synctest.Wait()
			queries := source.snapshot()
			if len(queries) != 2 || queries[1].cursor != nil {
				t.Fatal("invalid batch progress became a resume cursor")
			}
		})
	}
}

func TestRecoveryRuntimeWholeRunBudgetIncludesClockQueryAndSettlement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		clock := &recoveryClockStub{read: func(ctx context.Context) (time.Time, error) {
			deadline, _ := ctx.Deadline()
			if !deadline.Equal(started.Add(500 * time.Millisecond)) {
				t.Fatal("clock call did not receive its operation budget")
			}
			time.Sleep(300 * time.Millisecond)
			return time.Now(), nil
		}}
		source := &recoveryCandidateStub{list: func(ctx context.Context, q recoveryQuery) ([]repository.UnboundCandidate, error) {
			deadline, _ := ctx.Deadline()
			if !deadline.Equal(started.Add(800 * time.Millisecond)) {
				t.Fatal("query reset the whole-run budget")
			}
			time.Sleep(300 * time.Millisecond)
			return recoveryPage(q.cutoff, nil, 3), nil
		}}
		settler := &recoveryBatchStub{settle: func(ctx context.Context, _ []repository.UnboundCandidate) (BatchResult, error) {
			deadline, _ := ctx.Deadline()
			if !deadline.Equal(started.Add(800*time.Millisecond)) || time.Until(deadline) != 200*time.Millisecond {
				t.Fatal("settlement received a fresh run budget")
			}
			<-ctx.Done()
			return BatchResult{Processed: 1}, ctx.Err()
		}}
		config := recoveryConfig()
		config.RunTimeout, config.OperationTimeout = 800*time.Millisecond, 500*time.Millisecond
		r := recoveryRuntime(t, clock, source, settler, config)
		startRecovery(t, r)
		time.Sleep(time.Second)
		synctest.Wait()
		if len(source.snapshot()) != 1 || len(settler.snapshot()) != 1 {
			t.Fatal("budget exhaustion started more work")
		}
		stopRecovery(t, r)
	})
}

func TestRecoveryRuntimeSlowReturnStartsDelayAfterActualExitWithoutOverlap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var reads atomic.Int32
		source := &recoveryCandidateStub{list: func(context.Context, recoveryQuery) ([]repository.UnboundCandidate, error) {
			if reads.Add(1) == 1 {
				time.Sleep(12 * time.Second)
			}
			return nil, nil
		}}
		r := recoveryRuntime(t, &recoveryClockStub{}, source, &recoveryBatchStub{}, recoveryConfig())
		startRecovery(t, r)
		defer stopRecovery(t, r)
		time.Sleep(12 * time.Second)
		synctest.Wait()
		if len(source.snapshot()) != 1 {
			t.Fatal("expired run spawned overlapping work before actual return")
		}
		time.Sleep(7999 * time.Millisecond)
		synctest.Wait()
		if len(source.snapshot()) != 1 {
			t.Fatal("slow operation consumed the subsequent idle interval")
		}
		time.Sleep(4002 * time.Millisecond)
		synctest.Wait()
		if len(source.snapshot()) != 2 {
			t.Fatal("next run failed to wait after actual exit or caught up old ticks")
		}
	})
}

func TestRecoveryRuntimeStopWaitsForEveryDependencyPhaseToActuallyReturn(t *testing.T) {
	for _, phase := range []string{"clock", "query", "settlement"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				releaseOnce := sync.OnceFunc(func() { close(release) })
				defer releaseOnce()
				clock := &recoveryClockStub{read: func(ctx context.Context) (time.Time, error) {
					if phase == "clock" {
						<-release
					}
					return time.Now(), ctx.Err()
				}}
				source := &recoveryCandidateStub{list: func(ctx context.Context, q recoveryQuery) ([]repository.UnboundCandidate, error) {
					if phase == "query" {
						<-release
					}
					return recoveryPage(q.cutoff, nil, 1), ctx.Err()
				}}
				settler := &recoveryBatchStub{settle: func(ctx context.Context, _ []repository.UnboundCandidate) (BatchResult, error) {
					<-release
					return BatchResult{Processed: 1}, ctx.Err()
				}}
				r := recoveryRuntime(t, clock, source, settler, recoveryConfig())
				startRecovery(t, r)
				synctest.Wait()
				if err := r.Start(); err == nil {
					t.Fatal("runtime started a second loop")
				}
				r.Stop()
				r.Stop()
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
				if err := r.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Wait returned before the dependency: %v", err)
				}
				before := []int{int(clock.calls.Load()), len(source.snapshot()), len(settler.snapshot())}
				time.Sleep(20 * time.Second)
				synctest.Wait()
				if after := []int{int(clock.calls.Load()), len(source.snapshot()), len(settler.snapshot())}; !reflect.DeepEqual(before, after) {
					t.Fatal("Stop left discovery or replacement work running")
				}
				releaseOnce()
				if err := r.Wait(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := r.Start(); err == nil {
					t.Fatal("completed runtime restarted")
				}
			})
		})
	}
}

func recoveryConfig() Config {
	return Config{ScanInterval: DefaultScanInterval, PageSize: DefaultPageSize, RunTimeout: DefaultRunTimeout,
		OperationTimeout: DefaultOperationTimeout, ObservationPeriod: DefaultObservationPeriod}
}

func recoveryRuntime(t *testing.T, clock databaseClock, source candidateSource, settler batchSettler, config Config) *Runtime {
	t.Helper()
	r, err := NewRuntime(clock, source, settler, config)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func startRecovery(t *testing.T, r *Runtime) {
	t.Helper()
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
}

func stopRecovery(t *testing.T, r *Runtime) {
	t.Helper()
	r.Stop()
	if err := r.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func recoveryPage(at time.Time, cursor *repository.UnboundCursor, count int) []repository.UnboundCandidate {
	var first int64
	if cursor != nil {
		first = cursor.TaskID
	}
	rows := make([]repository.UnboundCandidate, count)
	for index := range rows {
		id := first + int64(index) + 1
		rows[index] = repository.UnboundCandidate{TaskID: id, InstanceID: 1000 + id, AttemptNo: 7, AttemptRegisteredAt: at}
	}
	return rows
}

type recoveryClockStub struct {
	calls atomic.Int32
	read  func(context.Context) (time.Time, error)
}

func (s *recoveryClockStub) DatabaseTime(ctx context.Context) (time.Time, error) {
	s.calls.Add(1)
	if s.read != nil {
		return s.read(ctx)
	}
	return time.Now(), nil
}

type recoveryQuery struct {
	scanNow time.Time
	cutoff  time.Time
	cursor  *repository.UnboundCursor
	limit   int
}

type recoveryCandidateStub struct {
	mu      sync.Mutex
	queries []recoveryQuery
	list    func(context.Context, recoveryQuery) ([]repository.UnboundCandidate, error)
}

func (s *recoveryCandidateStub) ListUnboundCandidates(ctx context.Context, now, cutoff time.Time, cursor *repository.UnboundCursor, limit int) ([]repository.UnboundCandidate, error) {
	var copyCursor *repository.UnboundCursor
	if cursor != nil {
		value := *cursor
		copyCursor = &value
	}
	query := recoveryQuery{scanNow: now, cutoff: cutoff, cursor: copyCursor, limit: limit}
	s.mu.Lock()
	s.queries = append(s.queries, query)
	s.mu.Unlock()
	if s.list != nil {
		return s.list(ctx, query)
	}
	return nil, nil
}

func (s *recoveryCandidateStub) snapshot() []recoveryQuery {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recoveryQuery(nil), s.queries...)
}

type recoveryBatchStub struct {
	mu      sync.Mutex
	batches [][]repository.UnboundCandidate
	settle  func(context.Context, []repository.UnboundCandidate) (BatchResult, error)
}

func (s *recoveryBatchStub) SettleBatch(ctx context.Context, candidates []repository.UnboundCandidate) (BatchResult, error) {
	s.mu.Lock()
	s.batches = append(s.batches, append([]repository.UnboundCandidate(nil), candidates...))
	s.mu.Unlock()
	if s.settle != nil {
		return s.settle(ctx, candidates)
	}
	return BatchResult{Processed: len(candidates)}, nil
}

func (s *recoveryBatchStub) snapshot() [][]repository.UnboundCandidate {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]repository.UnboundCandidate(nil), s.batches...)
}
