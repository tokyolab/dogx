package dispatch

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestRuntimeValidatesBeforeStartingWork(t *testing.T) {
	source := &runtimeSourceStub{}
	registrar, sender, store := &dispatcherRegistrarStub{}, &dispatcherSenderStub{}, &dispatcherStoreStub{}
	tasks := &runtimeTaskStoreStub{responseStore: store}
	var nilSource *runtimeSourceStub
	for _, dependency := range []waitingSource{nil, nilSource} {
		if r, err := NewRuntime(dependency, registrar, sender, tasks, runtimeTestConfig(), nil); err == nil || r != nil {
			t.Fatal("nil source accepted")
		}
	}
	for name, change := range map[string]func(*RuntimeConfig){
		"concurrency":       func(c *RuntimeConfig) { c.Concurrency = 0 },
		"waiting":           func(c *RuntimeConfig) { c.WaitingCapacity = -1 },
		"interval":          func(c *RuntimeConfig) { c.ScanInterval = 0 },
		"interval overflow": func(c *RuntimeConfig) { c.ScanInterval = time.Duration(1<<63 - 1) },
		"query":             func(c *RuntimeConfig) { c.QueryTimeout = 0 },
		"total":             func(c *RuntimeConfig) { c.TotalTimeout = 0 },
		"busy":              func(c *RuntimeConfig) { c.BusyDelay = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			config := runtimeTestConfig()
			change(&config)
			if r, err := NewRuntime(source, registrar, sender, tasks, config, nil); err == nil || r != nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
	r := runtimeForTest(t, source, registrar, sender, store, runtimeTestConfig())
	if source.maxCalls.Load() != 0 || len(source.snapshot()) != 0 || registrar.calls.Load() != 0 || r.Hint(1) {
		t.Fatal("construction queried or opened hint admission")
	}
	r.Stop()
	r.Stop()
	if err := r.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err == nil || r.Hint(1) {
		t.Fatal("stopped runtime was restarted")
	}
}

func TestRuntimeHintsShareCapacityAndDoNotReplayMergedHints(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		registrar := &dispatcherRegistrarStub{call: func(context.Context, int64) (*RegisteredAttempt, error) {
			<-gate
			return nil, nil
		}}
		config := runtimeTestConfig()
		config.Concurrency, config.WaitingCapacity = 1, 2
		r := runtimeForTest(t, &runtimeSourceStub{}, registrar, &dispatcherSenderStub{}, &dispatcherStoreStub{}, config)
		startRuntime(t, r)
		synctest.Wait()
		if !r.Hint(1) {
			t.Fatal("first hint rejected")
		}
		synctest.Wait()
		if !r.Hint(1) || !r.Hint(2) || !r.Hint(2) || !r.Hint(3) || r.Hint(4) || r.Hint(0) {
			t.Fatal("duplicate hints or bounded waiting admission are incorrect")
		}
		r.Stop()
		if r.Hint(1) {
			t.Fatal("stopping runtime admitted work")
		}
		close(gate)
		if err := r.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if registrar.calls.Load() != 1 {
			t.Fatal("stop dispatched pending work or replayed a merged hint")
		}
	})
}

func TestRuntimeReservesQueryCapacityBeforeReadingAndReleasesUnusedSpace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queryGate, registrationGate := make(chan struct{}), make(chan struct{})
		source := &runtimeSourceStub{maximum: func(context.Context) (int64, error) { return 100, nil },
			list: func(_ context.Context, _, _ int64, limit int) ([]int64, error) {
				if limit != 4 {
					t.Errorf("query limit = %d, want available capacity 4", limit)
				}
				<-queryGate
				return []int64{1, 2}, nil
			}}
		registrar := &dispatcherRegistrarStub{call: func(context.Context, int64) (*RegisteredAttempt, error) {
			<-registrationGate
			return nil, nil
		}}
		config := runtimeTestConfig()
		config.Concurrency, config.WaitingCapacity = 1, 4
		r := runtimeForTest(t, source, registrar, &dispatcherSenderStub{}, &dispatcherStoreStub{}, config)
		startRuntime(t, r)
		synctest.Wait()
		if r.Hint(99) {
			t.Fatal("hint bypassed the in-flight query reservation")
		}
		close(queryGate)
		synctest.Wait()
		if !r.Hint(90) || !r.Hint(91) || !r.Hint(92) || r.Hint(93) {
			t.Fatal("temporary page, pending work and unused query space were miscounted")
		}
		r.Stop()
		close(registrationGate)
		if err := r.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRuntimeNoProgressStopsAfterFourPagesThenResumesItsCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := runtimeFullPages(4096)
		config := runtimeTestConfig()
		config.WaitingCapacity, config.ScanInterval = 1024, time.Second
		r := runtimeForTest(t, source, &dispatcherRegistrarStub{}, &dispatcherSenderStub{}, &dispatcherStoreStub{}, config)
		startRuntime(t, r)
		synctest.Wait()
		first := source.snapshot()
		if len(first) != 3 || runtimeTotalPages(r, source) != 4 {
			t.Fatalf("no-progress scan read %d waiting pages, want 3 plus an empty retry page", len(first))
		}
		for index, call := range first {
			if call.cursor != int64(index*128) || call.upperID != 4096 || call.limit != 128 {
				t.Fatalf("unexpected bounded page: %+v", call)
			}
		}
		time.Sleep(799 * time.Millisecond)
		if runtimeTotalPages(r, source) != 4 {
			t.Fatal("no-progress scan resumed before minimum jitter delay")
		}
		time.Sleep(402 * time.Millisecond)
		synctest.Wait()
		after := source.snapshot()
		if runtimeTotalPages(r, source) != 8 || len(after) < 6 || after[3].cursor != 384 || source.maxCalls.Load() != 1 {
			t.Fatalf("scan lost its traversal or ignored the refreshed four-page budget: %+v", after)
		}
		stopRuntime(t, r)
	})
}

func TestRuntimeRegistrationResumesScanBeforeRPCCompletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registrationGate, sendGate := make(chan struct{}), make(chan struct{})
		source := runtimeFullPages(4096)
		registrar := &dispatcherRegistrarStub{call: func(_ context.Context, id int64) (*RegisteredAttempt, error) {
			<-registrationGate
			attempt := dispatcherAttempt()
			attempt.InstanceID = id
			return &attempt, nil
		}}
		sender := &dispatcherSenderStub{call: func(context.Context, RegisteredAttempt) (Response, error) {
			<-sendGate
			return Response{}, nil
		}}
		config := runtimeTestConfig()
		config.Concurrency, config.WaitingCapacity = 1, 1024
		r := runtimeForTest(t, source, registrar, sender, &dispatcherStoreStub{}, config)
		startRuntime(t, r)
		synctest.Wait()
		if len(source.snapshot()) != 3 || runtimeTotalPages(r, source) != 4 || sender.calls.Load() != 0 {
			t.Fatal("scan did not pause while registration remained unconfirmed")
		}
		close(registrationGate)
		synctest.Wait()
		if len(source.snapshot()) != 7 || runtimeTotalPages(r, source) != 8 || sender.calls.Load() != 1 {
			t.Fatal("confirmed registration did not resume supply while the RPC remained blocked")
		}
		r.Stop()
		close(sendGate)
		if err := r.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRuntimeHintProgressDoesNotRefreshScanBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := runtimeFullPages(4096)
		registrar := &dispatcherRegistrarStub{call: func(_ context.Context, id int64) (*RegisteredAttempt, error) {
			if id != 9999 {
				return nil, nil
			}
			attempt := dispatcherAttempt()
			attempt.InstanceID = id
			return &attempt, nil
		}}
		config := runtimeTestConfig()
		config.WaitingCapacity = 1024
		r := runtimeForTest(t, source, registrar, &dispatcherSenderStub{}, &dispatcherStoreStub{}, config)
		startRuntime(t, r)
		synctest.Wait()
		if len(source.snapshot()) != 3 || !r.Hint(9999) {
			t.Fatal("initial scan or immediate hint did not run")
		}
		synctest.Wait()
		if len(source.snapshot()) != 3 {
			t.Fatal("unrelated local registration gave the scan progress credit")
		}
		stopRuntime(t, r)
	})
}

func TestRuntimeDeduplicatesTheSameCandidateThroughItsRPC(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registrationGate, sendGate := make(chan struct{}), make(chan struct{})
		var registrations atomic.Int32
		registrar := &dispatcherRegistrarStub{call: func(_ context.Context, id int64) (*RegisteredAttempt, error) {
			if registrations.Add(1) != 1 {
				return nil, nil
			}
			<-registrationGate
			attempt := dispatcherAttempt()
			attempt.InstanceID = id
			return &attempt, nil
		}}
		sender := &dispatcherSenderStub{call: func(context.Context, RegisteredAttempt) (Response, error) {
			<-sendGate
			return Response{}, nil
		}}
		r := runtimeForTest(t, &runtimeSourceStub{}, registrar, sender, &dispatcherStoreStub{}, runtimeTestConfig())
		startRuntime(t, r)
		synctest.Wait()
		r.Hint(22)
		synctest.Wait()
		r.Hint(22)
		synctest.Wait()
		if registrations.Load() != 1 {
			t.Fatal("same-Instance registration overlapped")
		}
		close(registrationGate)
		synctest.Wait()
		if registrations.Load() != 1 || sender.calls.Load() != 1 {
			t.Fatal("a previously merged hint was replayed after registration")
		}
		if !r.Hint(22) {
			t.Fatal("new hint was not admitted after registration")
		}
		synctest.Wait()
		if registrations.Load() != 1 {
			t.Fatal("the same candidate lost deduplication while its RPC was active")
		}
		r.Stop()
		close(sendGate)
		if err := r.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRuntimeTailWaitsAndNextTraversalFindsLowerLateCommits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var maximum atomic.Int32
		source := &runtimeSourceStub{maximum: func(context.Context) (int64, error) {
			return int64(maximum.Add(1)) * 10, nil
		}, list: func(_ context.Context, _, upperID int64, _ int) ([]int64, error) {
			if upperID == 10 {
				return []int64{10}, nil
			}
			return []int64{1, 20}, nil
		}}
		var mu sync.Mutex
		var registered []int64
		registrar := &dispatcherRegistrarStub{call: func(_ context.Context, id int64) (*RegisteredAttempt, error) {
			mu.Lock()
			registered = append(registered, id)
			mu.Unlock()
			attempt := dispatcherAttempt()
			attempt.InstanceID = id
			return &attempt, nil
		}}
		config := runtimeTestConfig()
		config.ScanInterval = time.Second
		r := runtimeForTest(t, source, registrar, &dispatcherSenderStub{}, &dispatcherStoreStub{}, config)
		startRuntime(t, r)
		synctest.Wait()
		time.Sleep(799 * time.Millisecond)
		if len(source.snapshot()) != 1 {
			t.Fatal("tail progress bypassed the next traversal delay")
		}
		time.Sleep(402 * time.Millisecond)
		synctest.Wait()
		calls := source.snapshot()
		if len(calls) != 2 || calls[1].cursor != 0 || calls[1].upperID != 20 {
			t.Fatalf("new traversal retained a permanent cursor or old upper bound: %+v", calls)
		}
		mu.Lock()
		found := false
		for _, id := range registered {
			found = found || id == 1
		}
		mu.Unlock()
		if !found {
			t.Fatal("lower-ID late commit was permanently skipped")
		}
		stopRuntime(t, r)
	})
}

func TestRuntimeQueryFailureDoesNotMoveCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var queries atomic.Int32
		source := &runtimeSourceStub{maximum: func(context.Context) (int64, error) { return 4096, nil },
			list: func(_ context.Context, cursor, _ int64, limit int) ([]int64, error) {
				switch queries.Add(1) {
				case 1:
					return runtimePage(cursor, limit), nil
				case 2:
					return []int64{999}, errors.New("synthetic database failure")
				default:
					return nil, nil
				}
			}}
		config := runtimeTestConfig()
		config.WaitingCapacity, config.ScanInterval = 1024, time.Second
		r := runtimeForTest(t, source, &dispatcherRegistrarStub{}, &dispatcherSenderStub{}, &dispatcherStoreStub{}, config)
		startRuntime(t, r)
		synctest.Wait()
		if len(source.snapshot()) != 2 {
			t.Fatal("query failure did not pause scanning")
		}
		for step := 0; len(source.snapshot()) < 3 && step < 40; step++ {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
		}
		calls := source.snapshot()
		if len(calls) != 3 || calls[2].cursor != 128 || calls[2].upperID != 4096 || source.maxCalls.Load() != 1 {
			t.Fatalf("query failure advanced or restarted traversal: %+v", calls)
		}
		stopRuntime(t, r)
	})
}

func TestRuntimeStopKeepsResponsePersistenceAlive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sendGate := make(chan struct{})
		var sendContext context.Context
		sender := &dispatcherSenderStub{call: func(ctx context.Context, _ RegisteredAttempt) (Response, error) {
			sendContext = ctx
			<-sendGate
			return dispatcherResponse(OutcomeAccepted), nil
		}}
		store := &dispatcherStoreStub{write: func(ctx context.Context, _ dispatcherWriteCall, _ int) (bool, error) {
			if ctx.Err() != nil {
				t.Error("stopping scans canceled ACK persistence")
			}
			return true, nil
		}}
		r := runtimeForTest(t, &runtimeSourceStub{}, dispatcherCommittedRegistrar(dispatcherAttempt()), sender, store, runtimeTestConfig())
		startRuntime(t, r)
		synctest.Wait()
		r.Hint(dispatcherAttempt().InstanceID)
		synctest.Wait()
		r.Stop()
		if sendContext == nil || sendContext.Err() != nil {
			t.Fatal("Stop canceled an already-authorized dispatch")
		}
		close(sendGate)
		if err := r.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(store.snapshot()) != 1 {
			t.Fatal("accepted ACK was not persisted during shutdown")
		}
	})
}

func TestRuntimeStopSkipsTakenCandidateBeforeRegistration(t *testing.T) {
	registrar := &dispatcherRegistrarStub{}
	r := runtimeForTest(t, &runtimeSourceStub{}, registrar, &dispatcherSenderStub{}, &dispatcherStoreStub{}, runtimeTestConfig())
	r.registering[22] = waitingCandidate{sequence: 1, instanceID: 22}
	r.Stop()
	wrapper := runtimeRegistrar{runtime: r, registrar: registrar}
	if attempt, err := wrapper.StartInstance(context.Background(), 22); err != nil || attempt != nil || registrar.calls.Load() != 0 {
		t.Fatal("a worker entered registration after stop won the admission race")
	}
}

func TestRuntimeOldDispatchCleanupDoesNotReleaseNewRegistration(t *testing.T) {
	r := runtimeForTest(t, &runtimeSourceStub{}, &dispatcherRegistrarStub{}, &dispatcherSenderStub{}, &dispatcherStoreStub{}, runtimeTestConfig())
	old := waitingCandidate{sequence: 1, instanceID: 22}
	current := waitingCandidate{sequence: 2, instanceID: 22}
	r.registering[22] = current
	r.registrationFinished(old, false, nil)
	if got, found := r.registering[22]; !found || got != current {
		t.Fatal("late RPC cleanup released a different registration for the same Instance")
	}
	r.Stop()
}

func TestRuntimeDrainDeadlineDoesNotPretendUncooperativeWorkReturned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sendGate := make(chan struct{})
		var sendContext context.Context
		sender := &dispatcherSenderStub{call: func(ctx context.Context, _ RegisteredAttempt) (Response, error) {
			sendContext = ctx
			<-sendGate
			return dispatcherResponse(OutcomeAccepted), nil
		}}
		store := &dispatcherStoreStub{}
		r := runtimeForTest(t, &runtimeSourceStub{}, dispatcherCommittedRegistrar(dispatcherAttempt()), sender, store, runtimeTestConfig())
		startRuntime(t, r)
		synctest.Wait()
		r.Hint(dispatcherAttempt().InstanceID)
		synctest.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if err := r.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("drain error = %v", err)
		}
		synctest.Wait()
		if sendContext.Err() == nil || !errors.Is(r.Wait(ctx), context.DeadlineExceeded) {
			t.Fatal("shutdown deadline did not cancel work or Wait claimed premature completion")
		}
		close(sendGate)
		if err := r.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(store.snapshot()) != 0 {
			t.Fatal("a new persistence operation began after shutdown budget exhaustion")
		}
	})
}

func TestRuntimeWaitIncludesCanceledQueryUntilItActuallyReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queryGate := make(chan struct{})
		source := &runtimeSourceStub{maximum: func(context.Context) (int64, error) { return 1, nil },
			list: func(context.Context, int64, int64, int) ([]int64, error) {
				<-queryGate
				return []int64{1}, nil
			}}
		registrar := &dispatcherRegistrarStub{}
		r := runtimeForTest(t, source, registrar, &dispatcherSenderStub{}, &dispatcherStoreStub{}, runtimeTestConfig())
		startRuntime(t, r)
		synctest.Wait()
		r.Stop()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if !errors.Is(r.Wait(ctx), context.Canceled) {
			t.Fatal("Wait ignored a query still using the source")
		}
		close(queryGate)
		if err := r.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if registrar.calls.Load() != 0 {
			t.Fatal("a late canceled page was admitted after stop")
		}
	})
}

func TestRuntimeRejectsInvalidSourcePageWithoutDispatchOrCursorAdvance(t *testing.T) {
	for _, ids := range [][]int64{{0}, {2, 1}, {1, 1}, {11}, runtimePage(0, 129)} {
		t.Run("invalid page", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				source := &runtimeSourceStub{maximum: func(context.Context) (int64, error) { return 10, nil },
					list: func(context.Context, int64, int64, int) ([]int64, error) { return ids, nil }}
				registrar := &dispatcherRegistrarStub{}
				config := runtimeTestConfig()
				config.ScanInterval = time.Second
				r := runtimeForTest(t, source, registrar, &dispatcherSenderStub{}, &dispatcherStoreStub{}, config)
				startRuntime(t, r)
				synctest.Wait()
				time.Sleep(1201 * time.Millisecond)
				synctest.Wait()
				calls := source.snapshot()
				if len(calls) != 2 || calls[0].cursor != 0 || calls[1].cursor != 0 || registrar.calls.Load() != 0 {
					t.Fatalf("invalid source page was treated as progress: %+v", calls)
				}
				stopRuntime(t, r)
			})
		})
	}
}

func TestRuntimeMergesScannedIDWithHintWithoutReattributingProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queryGate, registrationGate := make(chan struct{}), make(chan struct{})
		source := &runtimeSourceStub{maximum: func(context.Context) (int64, error) { return 4096, nil },
			list: func(_ context.Context, cursor, _ int64, limit int) ([]int64, error) {
				<-queryGate
				return runtimePage(cursor, limit), nil
			}}
		registrar := &dispatcherRegistrarStub{call: func(_ context.Context, id int64) (*RegisteredAttempt, error) {
			if id != 1 {
				return nil, nil
			}
			<-registrationGate
			attempt := dispatcherAttempt()
			attempt.InstanceID = id
			return &attempt, nil
		}}
		config := runtimeTestConfig()
		config.WaitingCapacity = 1024
		r := runtimeForTest(t, source, registrar, &dispatcherSenderStub{}, &dispatcherStoreStub{}, config)
		startRuntime(t, r)
		synctest.Wait()
		if !r.Hint(1) {
			t.Fatal("hint could not use space outside the page reservation")
		}
		synctest.Wait()
		close(queryGate)
		synctest.Wait()
		if len(source.snapshot()) != 3 {
			t.Fatal("scan did not exhaust its no-progress budget")
		}
		close(registrationGate)
		synctest.Wait()
		if len(source.snapshot()) != 3 {
			t.Fatal("merged scan identity converted prior hint registration into scan progress")
		}
		stopRuntime(t, r)
	})
}

func runtimeTestConfig() RuntimeConfig {
	return RuntimeConfig{Concurrency: 2, WaitingCapacity: 256, ScanInterval: time.Hour,
		QueryTimeout: 2 * time.Second, TotalTimeout: 10 * time.Second, BusyDelay: 5 * time.Second}
}

func runtimeForTest(t *testing.T, source waitingSource, registrar attemptRegistrar, sender Sender, store responseStore, config RuntimeConfig) *Runtime {
	t.Helper()
	tasks, ok := store.(runtimeTasks)
	if !ok {
		tasks = &runtimeTaskStoreStub{responseStore: store}
	}
	r, err := NewRuntime(source, registrar, sender, tasks, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func startRuntime(t *testing.T, r *Runtime) {
	t.Helper()
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
}

func stopRuntime(t *testing.T, r *Runtime) {
	t.Helper()
	r.Stop()
	if err := r.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func runtimeFullPages(upperID int64) *runtimeSourceStub {
	return &runtimeSourceStub{maximum: func(context.Context) (int64, error) { return upperID, nil },
		list: func(_ context.Context, cursor, upper int64, limit int) ([]int64, error) {
			return runtimePage(cursor, min(limit, int(upper-cursor))), nil
		}}
}

func runtimePage(cursor int64, limit int) []int64 {
	ids := make([]int64, limit)
	for index := range ids {
		ids[index] = cursor + int64(index) + 1
	}
	return ids
}

type runtimeQueryCall struct {
	cursor  int64
	upperID int64
	limit   int
}

type runtimeSourceStub struct {
	maximum      func(context.Context) (int64, error)
	list         func(context.Context, int64, int64, int) ([]int64, error)
	databaseTime func(context.Context) (time.Time, error)
	maxCalls     atomic.Int32
	mu           sync.Mutex
	calls        []runtimeQueryCall
}

func (s *runtimeSourceStub) DatabaseTime(ctx context.Context) (time.Time, error) {
	if s.databaseTime != nil {
		return s.databaseTime(ctx)
	}
	return time.Now(), nil
}

func (s *runtimeSourceStub) MaxWaitingID(ctx context.Context) (int64, error) {
	s.maxCalls.Add(1)
	if s.maximum != nil {
		return s.maximum(ctx)
	}
	return 0, nil
}

func (s *runtimeSourceStub) ListWaitingIDs(ctx context.Context, cursor, upperID int64, limit int) ([]int64, error) {
	s.mu.Lock()
	s.calls = append(s.calls, runtimeQueryCall{cursor: cursor, upperID: upperID, limit: limit})
	s.mu.Unlock()
	if s.list != nil {
		return s.list(ctx, cursor, upperID, limit)
	}
	return nil, nil
}

func (s *runtimeSourceStub) snapshot() []runtimeQueryCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]runtimeQueryCall(nil), s.calls...)
}

type runtimeRetryQuery struct {
	scanNow time.Time
	cursor  *repository.DispatchCursor
	limit   int
}

type runtimeTaskStoreStub struct {
	responseStore
	list         func(context.Context, time.Time, *repository.DispatchCursor, int) ([]repository.DispatchCandidate, error)
	listInstance func(context.Context, int64, time.Time, int) ([]repository.DispatchCandidate, error)
	mu           sync.Mutex
	queries      []runtimeRetryQuery
}

func (s *runtimeTaskStoreStub) ListDispatchCandidates(ctx context.Context, now time.Time, cursor *repository.DispatchCursor, limit int) ([]repository.DispatchCandidate, error) {
	var copied *repository.DispatchCursor
	if cursor != nil {
		value := *cursor
		copied = &value
	}
	s.mu.Lock()
	s.queries = append(s.queries, runtimeRetryQuery{scanNow: now, cursor: copied, limit: limit})
	s.mu.Unlock()
	if s.list != nil {
		return s.list(ctx, now, cursor, limit)
	}
	return nil, nil
}

func (s *runtimeTaskStoreStub) snapshot() []runtimeRetryQuery {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]runtimeRetryQuery(nil), s.queries...)
}

func (s *runtimeTaskStoreStub) ListInstanceDispatchCandidates(ctx context.Context, id int64, now time.Time, limit int) ([]repository.DispatchCandidate, error) {
	if s.listInstance != nil {
		return s.listInstance(ctx, id, now, limit)
	}
	return nil, nil
}

func runtimeTotalPages(r *Runtime, source *runtimeSourceStub) int {
	return len(source.snapshot()) + len(r.tasks.(*runtimeTaskStoreStub).snapshot())
}

func TestRuntimeSharesFourPageBudgetAcrossBothCandidateSources(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scanNow := time.Now()
		source := runtimeFullPages(4096)
		source.databaseTime = func(context.Context) (time.Time, error) { return scanNow, nil }
		tasks := &runtimeTaskStoreStub{responseStore: &dispatcherStoreStub{}, list: func(_ context.Context, now time.Time, cursor *repository.DispatchCursor, limit int) ([]repository.DispatchCandidate, error) {
			return runtimeDispatchPage(now.Add(-time.Second), cursor, limit), nil
		}}
		config := runtimeTestConfig()
		config.WaitingCapacity = 1024
		r := runtimeForTest(t, source, &dispatcherRegistrarStub{}, &dispatcherSenderStub{}, tasks, config)
		startRuntime(t, r)
		synctest.Wait()
		waiting, retry := source.snapshot(), tasks.snapshot()
		if len(waiting) != 2 || len(retry) != 2 || waiting[1].cursor != 128 || retry[0].cursor != nil ||
			retry[1].cursor == nil || retry[1].cursor.TaskID != 128 || !retry[0].scanNow.Equal(scanNow) || !retry[1].scanNow.Equal(scanNow) {
			t.Fatalf("sources reset the shared budget or lost separate positions: waiting=%+v retry=%+v", waiting, retry)
		}
		stopRuntime(t, r)
	})
}

func TestRuntimeRetryQueryReservationAndOriginalAttemptArePreserved(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queryReturn, registrationReturn := make(chan struct{}), make(chan struct{})
		var seen []repository.DispatchCandidate
		var mu sync.Mutex
		original := repository.DispatchCandidate{TaskType: model.TaskTypeOrdinary, Status: model.TaskRetryWaiting, TaskID: 81, InstanceID: 19, AttemptNo: 7, NextDispatchAt: time.Now().Add(-time.Second)}
		tasks := &runtimeTaskStoreStub{responseStore: &dispatcherStoreStub{}, list: func(_ context.Context, _ time.Time, _ *repository.DispatchCursor, limit int) ([]repository.DispatchCandidate, error) {
			if limit != 2 {
				t.Errorf("retry query limit=%d, want remaining waiting capacity 2", limit)
			}
			<-queryReturn
			return []repository.DispatchCandidate{original}, nil
		}}
		registrar := &dispatcherRegistrarStub{retryCall: func(_ context.Context, candidate repository.DispatchCandidate) (*RegisteredAttempt, error) {
			mu.Lock()
			seen = append(seen, candidate)
			mu.Unlock()
			<-registrationReturn
			return nil, nil
		}}
		config := runtimeTestConfig()
		config.Concurrency, config.WaitingCapacity = 1, 2
		r := runtimeForTest(t, &runtimeSourceStub{}, registrar, &dispatcherSenderStub{}, tasks, config)
		startRuntime(t, r)
		synctest.Wait()
		if r.Hint(99) {
			t.Fatal("hint bypassed the retry query reservation")
		}
		close(queryReturn)
		synctest.Wait()
		if !r.Hint(90) || !r.Hint(91) || r.Hint(92) {
			t.Fatal("retry processing and waiting hints did not share capacity")
		}
		mu.Lock()
		equal := len(seen) == 1 && reflect.DeepEqual(seen[0], original)
		mu.Unlock()
		if !equal {
			t.Fatal("retry supply replaced the observed attempt number or original cursor time")
		}
		r.Stop()
		close(registrationReturn)
		if err := r.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRuntimeRetryQueryErrorKeepsItsDatabaseTimeAndCompositeCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var clocks, reads atomic.Int32
		initialTime := time.Now()
		source := &runtimeSourceStub{databaseTime: func(context.Context) (time.Time, error) {
			return initialTime.Add(time.Duration(clocks.Add(1)) * time.Hour), nil
		}}
		tasks := &runtimeTaskStoreStub{responseStore: &dispatcherStoreStub{}, list: func(_ context.Context, now time.Time, cursor *repository.DispatchCursor, limit int) ([]repository.DispatchCandidate, error) {
			switch reads.Add(1) {
			case 1:
				return runtimeDispatchPage(now.Add(-time.Second), cursor, limit), nil
			case 2:
				return nil, errors.New("synthetic retry query failure")
			default:
				return nil, nil
			}
		}}
		config := runtimeTestConfig()
		config.ScanInterval = time.Second
		r := runtimeForTest(t, source, &dispatcherRegistrarStub{}, &dispatcherSenderStub{}, tasks, config)
		startRuntime(t, r)
		synctest.Wait()
		for step := 0; len(tasks.snapshot()) < 3 && step < 40; step++ {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
		}
		calls := tasks.snapshot()
		if len(calls) != 3 || clocks.Load() != 1 || calls[0].cursor != nil || calls[1].cursor == nil ||
			!reflect.DeepEqual(calls[1].cursor, calls[2].cursor) || calls[2].cursor.TaskID != 128 ||
			!calls[0].scanNow.Equal(calls[2].scanNow) {
			t.Fatalf("failed retry read moved or reset its traversal: %+v", calls)
		}
		stopRuntime(t, r)
	})
}

func TestRuntimeSameParentAttemptRegistrationEndsBeforeItsRPC(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registrationReturn, sendReturn := make(chan struct{}), make(chan struct{})
		rows := runtimeDispatchPage(time.Now().Add(-time.Second), nil, 2)
		rows[0].InstanceID, rows[1].InstanceID = 22, 22
		var entered atomic.Int32
		registrar := &dispatcherRegistrarStub{retryCall: func(_ context.Context, candidate repository.DispatchCandidate) (*RegisteredAttempt, error) {
			if entered.Add(1) == 1 {
				<-registrationReturn
			}
			attempt := dispatcherAttempt()
			attempt.InstanceID, attempt.TaskID, attempt.AttemptNo = candidate.InstanceID, candidate.TaskID, candidate.AttemptNo+1
			return &attempt, nil
		}}
		sender := &dispatcherSenderStub{call: func(context.Context, RegisteredAttempt) (Response, error) { <-sendReturn; return Response{}, nil }}
		tasks := &runtimeTaskStoreStub{responseStore: &dispatcherStoreStub{}, list: func(context.Context, time.Time, *repository.DispatchCursor, int) ([]repository.DispatchCandidate, error) {
			return rows, nil
		}}
		r := runtimeForTest(t, &runtimeSourceStub{}, registrar, sender, tasks, runtimeTestConfig())
		startRuntime(t, r)
		synctest.Wait()
		if entered.Load() != 1 || sender.calls.Load() != 0 {
			t.Fatal("same-parent registrations overlapped before actual return")
		}
		close(registrationReturn)
		synctest.Wait()
		if entered.Load() != 2 || sender.calls.Load() != 2 {
			t.Fatal("the first RPC retained the parent's registration gate")
		}
		r.Stop()
		close(sendReturn)
		if err := r.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRuntimeParentConflictSkipsFollowingPagesWhileOthersProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		secondPage := make(chan struct{})
		var pages, conflicts, others atomic.Int32
		tasks := &runtimeTaskStoreStub{responseStore: &dispatcherStoreStub{}, list: func(_ context.Context, now time.Time, cursor *repository.DispatchCursor, limit int) ([]repository.DispatchCandidate, error) {
			page := pages.Add(1)
			if page > 2 {
				return nil, nil
			}
			if page == 2 {
				<-secondPage
			}
			rows := runtimeDispatchPage(now.Add(-time.Second), cursor, limit)
			for i := range rows {
				rows[i].InstanceID = 1
			}
			rows[len(rows)-1].InstanceID = int64(page) + 1
			return rows, nil
		}}
		registrar := &dispatcherRegistrarStub{retryCall: func(_ context.Context, candidate repository.DispatchCandidate) (*RegisteredAttempt, error) {
			if candidate.InstanceID == 1 {
				conflicts.Add(1)
				return nil, repository.ErrInstanceLocked
			}
			others.Add(1)
			attempt := dispatcherAttempt()
			attempt.InstanceID, attempt.TaskID, attempt.AttemptNo = candidate.InstanceID, candidate.TaskID, candidate.AttemptNo+1
			return &attempt, nil
		}}
		r := runtimeForTest(t, &runtimeSourceStub{}, registrar, &dispatcherSenderStub{}, tasks, runtimeTestConfig())
		startRuntime(t, r)
		synctest.Wait()
		if conflicts.Load() != 1 {
			t.Fatal("initial same-parent candidates repeatedly competed")
		}
		close(secondPage)
		synctest.Wait()
		if conflicts.Load() != 1 || others.Load() != 2 || len(tasks.snapshot()) != 3 {
			t.Fatal("the next page immediately retried a blocked parent or blocked other parents")
		}
		stopRuntime(t, r)
	})
}

func TestRuntimeLaterParentConflictKeepsItsOwnBackoffDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := runtimeTestConfig()
		config.ScanInterval = time.Second
		r := runtimeForTest(t, &runtimeSourceStub{}, &dispatcherRegistrarStub{}, &dispatcherSenderStub{}, &dispatcherStoreStub{}, config)
		r.mu.Lock()
		r.blockParentLocked(1)
		firstDeadline := r.conflicted[1]
		r.mu.Unlock()
		time.Sleep(time.Until(firstDeadline) - time.Millisecond)
		r.mu.Lock()
		r.blockParentLocked(2)
		laterDeadline := r.conflicted[2]
		r.pagesLeft = 0
		r.mu.Unlock()
		time.Sleep(time.Millisecond)
		r.mu.Lock()
		r.expireConflictsLocked(time.Now())
		_, firstHeld := r.conflicted[1]
		gotLater, laterHeld := r.conflicted[2]
		budget := r.pagesLeft
		r.mu.Unlock()
		if firstHeld || !laterHeld || !gotLater.Equal(laterDeadline) || laterDeadline.Sub(time.Now()) < 799*time.Millisecond || budget != 0 {
			t.Fatal("first-parent expiry erased a recent conflict or restored the scan budget")
		}
		r.Stop()
	})
}

func TestRuntimeSaturatedConflictWindowKeepsInflightIdentitiesAndAuthorizedRPCs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conflictReturns, sendReturn := make(chan struct{}), make(chan struct{})
		var entered atomic.Int32
		registrar := &dispatcherRegistrarStub{call: func(_ context.Context, id int64) (*RegisteredAttempt, error) {
			if id == 999 {
				attempt := dispatcherAttempt()
				attempt.InstanceID = id
				return &attempt, nil
			}
			entered.Add(1)
			<-conflictReturns
			return nil, repository.ErrInstanceLocked
		}}
		sender := &dispatcherSenderStub{call: func(context.Context, RegisteredAttempt) (Response, error) {
			<-sendReturn
			return dispatcherResponse(OutcomeAccepted), nil
		}}
		store := &dispatcherStoreStub{}
		config := runtimeTestConfig()
		config.Concurrency, config.WaitingCapacity, config.ScanInterval = 4, 2, time.Second
		r := runtimeForTest(t, &runtimeSourceStub{}, registrar, sender, store, config)
		startRuntime(t, r)
		synctest.Wait()
		r.Hint(999)
		synctest.Wait()
		r.Hint(1)
		r.Hint(2)
		synctest.Wait()
		r.Hint(3)
		synctest.Wait()
		if entered.Load() != 3 {
			t.Fatal("fixture did not establish in-flight registrations")
		}
		close(conflictReturns)
		synctest.Wait()
		r.mu.Lock()
		count, paused := len(r.conflicted), r.conflictFull
		r.mu.Unlock()
		if count != 3 || !paused || count > config.WaitingCapacity+config.Concurrency {
			t.Fatal("saturation dropped an in-flight conflict identity or exceeded its bound")
		}
		if !r.Hint(4) {
			t.Fatal("contention pause changed normal hint admission capacity")
		}
		close(sendReturn)
		synctest.Wait()
		if entered.Load() != 3 || len(store.snapshot()) != 1 {
			t.Fatal("saturation started new registration or stopped authorized ACK persistence")
		}
		time.Sleep(799 * time.Millisecond)
		synctest.Wait()
		if entered.Load() != 3 {
			t.Fatal("conflict pause ended before an actual parent backoff elapsed")
		}
		for step := 0; entered.Load() == 3 && step < 10; step++ {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
		}
		if entered.Load() != 4 {
			t.Fatal("bounded contention pause did not release waiting work")
		}
		r.mu.Lock()
		count = len(r.conflicted)
		r.mu.Unlock()
		if count > config.WaitingCapacity+config.Concurrency {
			t.Fatal("expired conflict records accumulated without a bound")
		}
		stopRuntime(t, r)
	})
}

func runtimeDispatchPage(at time.Time, cursor *repository.DispatchCursor, limit int) []repository.DispatchCandidate {
	var first int64
	if cursor != nil {
		first = cursor.TaskID
	}
	rows := make([]repository.DispatchCandidate, limit)
	for index := range rows {
		id := first + int64(index) + 1
		rows[index] = repository.DispatchCandidate{TaskType: model.TaskTypeOrdinary, Status: model.TaskRetryWaiting, TaskID: id, InstanceID: id + 10000, AttemptNo: 7, NextDispatchAt: at}
	}
	return rows
}

func TestRuntimeRetryIdentityIncludesTheObservedAttemptNumber(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sendReturn := make(chan struct{})
		at := time.Now().Add(-10 * time.Second)
		var pages atomic.Int32
		tasks := &runtimeTaskStoreStub{responseStore: &dispatcherStoreStub{}, list: func(_ context.Context, _ time.Time, _ *repository.DispatchCursor, limit int) ([]repository.DispatchCandidate, error) {
			page := pages.Add(1)
			if page > 3 {
				return nil, nil
			}
			if limit != 1 {
				t.Errorf("retry query limit=%d, want one shared position", limit)
			}
			attempt := int64(7)
			if page == 3 {
				attempt = 8
			}
			return []repository.DispatchCandidate{{TaskType: model.TaskTypeOrdinary, Status: model.TaskRetryWaiting, TaskID: 11, InstanceID: 22, AttemptNo: attempt, NextDispatchAt: at.Add(time.Duration(page) * time.Second)}}, nil
		}}
		var mu sync.Mutex
		var observed []int64
		registrar := &dispatcherRegistrarStub{retryCall: func(_ context.Context, candidate repository.DispatchCandidate) (*RegisteredAttempt, error) {
			mu.Lock()
			observed = append(observed, candidate.AttemptNo)
			mu.Unlock()
			attempt := dispatcherAttempt()
			attempt.InstanceID, attempt.TaskID, attempt.AttemptNo = candidate.InstanceID, candidate.TaskID, candidate.AttemptNo+1
			return &attempt, nil
		}}
		sender := &dispatcherSenderStub{call: func(context.Context, RegisteredAttempt) (Response, error) { <-sendReturn; return Response{}, nil }}
		config := runtimeTestConfig()
		config.WaitingCapacity = 1
		r := runtimeForTest(t, &runtimeSourceStub{}, registrar, sender, tasks, config)
		startRuntime(t, r)
		synctest.Wait()
		mu.Lock()
		valid := reflect.DeepEqual(observed, []int64{7, 8})
		mu.Unlock()
		if !valid || sender.calls.Load() != 2 {
			t.Fatal("retry deduplication replayed an old attempt or erased a genuinely newer observed attempt")
		}
		r.Stop()
		close(sendReturn)
		if err := r.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRuntimeOffersAnotherDiscoveredParentAfterAtMostEightCandidates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tasks := &runtimeTaskStoreStub{responseStore: &dispatcherStoreStub{}, list: func(_ context.Context, now time.Time, cursor *repository.DispatchCursor, limit int) ([]repository.DispatchCandidate, error) {
			if cursor != nil {
				return nil, nil
			}
			rows := runtimeDispatchPage(now.Add(-time.Second), nil, limit)
			for index := range rows {
				rows[index].InstanceID = 1
				if index >= len(rows)-8 {
					rows[index].InstanceID = 2
				}
			}
			return rows, nil
		}}
		var mu sync.Mutex
		var parents []int64
		registrar := &dispatcherRegistrarStub{retryCall: func(_ context.Context, candidate repository.DispatchCandidate) (*RegisteredAttempt, error) {
			mu.Lock()
			parents = append(parents, candidate.InstanceID)
			mu.Unlock()
			return nil, nil
		}}
		config := runtimeTestConfig()
		config.Concurrency = 1
		r := runtimeForTest(t, &runtimeSourceStub{}, registrar, &dispatcherSenderStub{}, tasks, config)
		startRuntime(t, r)
		synctest.Wait()
		mu.Lock()
		firstOther := -1
		if len(parents) != 0 {
			for index, parent := range parents {
				if parent != parents[0] {
					firstOther = index
					break
				}
			}
		}
		count := len(parents)
		mu.Unlock()
		if count != 128 || firstOther <= 0 || firstOther > 8 {
			t.Fatal("one discovered parent consumed more than eight candidates before offering another parent a turn")
		}
		stopRuntime(t, r)
	})
}

func TestRuntimeTakenRegistrationMayFinishAfterConflictSaturation(t *testing.T) {
	registrar := &dispatcherRegistrarStub{call: func(context.Context, int64) (*RegisteredAttempt, error) {
		return nil, repository.ErrInstanceLocked
	}}
	config := runtimeTestConfig()
	config.Concurrency, config.WaitingCapacity = 1, 1
	r := runtimeForTest(t, &runtimeSourceStub{}, registrar, &dispatcherSenderStub{}, &dispatcherStoreStub{}, config)
	// Control the narrow gap between taking a candidate and entering Registrar,
	// without starting background workers or relying on scheduler timing.
	candidate := waitingCandidate{sequence: 1, instanceID: 22}
	r.pending, r.waiting = []waitingCandidate{candidate}, 1
	r.owned[candidate.key()] = candidate.sequence
	if got, ok := r.take(); !ok || got != candidate {
		t.Fatal("fixture did not take the candidate")
	}
	r.mu.Lock()
	r.blockParentLocked(33)
	r.mu.Unlock()
	wrapper := runtimeRegistrar{runtime: r, registrar: registrar}
	if attempt, err := wrapper.StartInstance(context.Background(), 22); attempt != nil || !errors.Is(err, repository.ErrInstanceLocked) || registrar.calls.Load() != 1 {
		t.Fatal("contention saturation discarded an already-taken registration")
	}
	r.mu.Lock()
	_, retained := r.conflicted[22]
	count := len(r.conflicted)
	r.mu.Unlock()
	if !retained || count != config.WaitingCapacity+config.Concurrency {
		t.Fatal("late in-flight conflict did not use its bounded reserved space")
	}
	r.Stop()
}
