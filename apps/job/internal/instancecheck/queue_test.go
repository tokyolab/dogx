package instancecheck

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

func TestQueueValidatesWithoutOpeningAdmission(t *testing.T) {
	var typedNil *queueCheckerStub
	for _, checker := range []checker{nil, typedNil} {
		if q, err := NewQueue(checker, queueConfig()); err == nil || q != nil {
			t.Fatal("nil checker was accepted")
		}
	}
	for _, config := range []Config{
		{Concurrency: 8, Timeout: 2 * time.Second},
		{Capacity: 512, Concurrency: -1, Timeout: 2 * time.Second},
		{Capacity: 512, Concurrency: 8},
	} {
		if q, err := NewQueue(&queueCheckerStub{}, config); err == nil || q != nil {
			t.Fatal("nonpositive queue settings were accepted")
		}
	}
	checker := &queueCheckerStub{}
	q := newTestQueue(t, checker, queueConfig())
	if q.Hint(1) || len(checker.snapshot()) != 0 {
		t.Fatal("construction admitted or processed a hint")
	}
	q.Stop()
	q.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q.Wait(ctx); err != nil {
		t.Fatal("unused queue did not complete after Stop")
	}
	if err := q.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if q.Start() == nil || q.Hint(1) {
		t.Fatal("stopped queue was restarted")
	}
}

func TestQueueWaitingHintsDoNotPostponeFirstWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		checker := &queueCheckerStub{}
		q := startTestQueue(t, checker, queueConfig())
		started := time.Now()
		if !q.Hint(1) || q.Hint(0) || q.Hint(-1) || q.Start() == nil {
			t.Fatal("invalid admission or duplicate start behavior")
		}
		time.Sleep(50 * time.Millisecond)
		for range 50 {
			if !q.Hint(1) {
				t.Fatal("waiting duplicate was rejected")
			}
		}
		time.Sleep(49 * time.Millisecond)
		synctest.Wait()
		if len(checker.snapshot()) != 0 {
			t.Fatal("check began before the initial merge window")
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		calls := checker.snapshot()
		if len(calls) != 1 || !calls[0].started.Equal(started.Add(mergeWindow)) {
			t.Fatalf("waiting duplicates moved or repeated the check: %+v", calls)
		}
		deadline, ok := calls[0].ctx.Deadline()
		if !ok || deadline.Sub(calls[0].started) != 2*time.Second {
			t.Fatal("queue or merge waiting consumed the processing budget")
		}
		stopTestQueue(t, q)
	})
}

func TestQueueCapacityIncludesProcessingAndFollowupKeepsItsSlotAtTail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn := make(chan struct{})
		checker := &queueCheckerStub{call: func(_ context.Context, _ int64, call int) (Outcome, error) {
			if call == 1 {
				<-firstReturn
			}
			return OutcomeNotReady, nil
		}}
		q := startTestQueue(t, checker, Config{Capacity: 2, Concurrency: 1, Timeout: 2 * time.Second})
		q.Hint(1)
		time.Sleep(mergeWindow)
		synctest.Wait()
		if !q.Hint(1) || !q.Hint(2) || !q.Hint(2) || q.Hint(3) {
			t.Fatal("in-flight and waiting IDs did not share unique capacity")
		}
		time.Sleep(mergeWindow)
		synctest.Wait()
		if len(checker.snapshot()) != 1 {
			t.Fatal("a second check began while the only worker was occupied")
		}
		close(firstReturn)
		synctest.Wait()
		if got := queueCallIDs(checker.snapshot()); !reflect.DeepEqual(got, []int64{1, 2, 1}) {
			t.Fatalf("follow-up did not keep its slot and yield the worker at queue tail: %v", got)
		}
		if !q.Hint(3) {
			t.Fatal("finished IDs retained capacity")
		}
		stopTestQueue(t, q)
	})
}

func TestQueueProcessingHintsKeepTheFirstRemainingWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn := make(chan struct{})
		checker := &queueCheckerStub{call: func(_ context.Context, _ int64, call int) (Outcome, error) {
			if call == 1 {
				<-firstReturn
			}
			return OutcomeNotApplied, nil
		}}
		q := startTestQueue(t, checker, Config{Capacity: 1, Concurrency: 2, Timeout: 2 * time.Second})
		started := time.Now()
		q.Hint(1)
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		time.Sleep(20 * time.Millisecond)
		q.Hint(1)
		time.Sleep(40 * time.Millisecond)
		q.Hint(1)
		time.Sleep(20 * time.Millisecond)
		close(firstReturn)
		synctest.Wait()
		if len(checker.snapshot()) != 1 {
			t.Fatal("follow-up ignored its remaining merge wait")
		}
		time.Sleep(39 * time.Millisecond)
		synctest.Wait()
		if len(checker.snapshot()) != 1 {
			t.Fatal("follow-up began before its first new event's deadline")
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		calls := checker.snapshot()
		if len(calls) != 2 || !calls[1].started.Equal(started.Add(220*time.Millisecond)) {
			t.Fatalf("later hints or completion reset the follow-up window: %+v", calls)
		}
		stopTestQueue(t, q)
	})
}

func TestQueueAllUnconfirmedOutcomesPreserveAnAdmittedFollowup(t *testing.T) {
	uncertain := errors.New("synthetic commit confirmation lost")
	for _, test := range []struct {
		name    string
		outcome Outcome
		err     error
	}{
		{"skipped", OutcomeSkipped, nil},
		{"not ready", OutcomeNotReady, nil},
		{"stopping", OutcomeStopping, nil},
		{"zero row", OutcomeNotApplied, nil},
		{"stage advanced", OutcomeAdvanced, nil},
		{"technical error", OutcomeSkipped, uncertain},
		{"terminal with error", OutcomeTerminal, uncertain},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				firstReturn := make(chan struct{})
				checker := &queueCheckerStub{call: func(_ context.Context, _ int64, call int) (Outcome, error) {
					if call == 1 {
						<-firstReturn
						return test.outcome, test.err
					}
					return OutcomeTerminal, nil
				}}
				q := startTestQueue(t, checker, Config{Capacity: 1, Concurrency: 1, Timeout: 2 * time.Second})
				q.Hint(1)
				time.Sleep(mergeWindow)
				synctest.Wait()
				q.Hint(1)
				close(firstReturn)
				time.Sleep(mergeWindow)
				synctest.Wait()
				if len(checker.snapshot()) != 2 {
					t.Fatal("unconfirmed result erased a hint admitted during processing")
				}
				stopTestQueue(t, q)
			})
		})
	}
}

func TestQueueConfirmedTerminalDiscardsMergedFollowup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn := make(chan struct{})
		checker := &queueCheckerStub{call: func(context.Context, int64, int) (Outcome, error) {
			<-firstReturn
			return OutcomeTerminal, nil
		}}
		q := startTestQueue(t, checker, Config{Capacity: 1, Concurrency: 1, Timeout: 2 * time.Second})
		q.Hint(1)
		time.Sleep(mergeWindow)
		synctest.Wait()
		q.Hint(1)
		close(firstReturn)
		time.Sleep(time.Second)
		synctest.Wait()
		if len(checker.snapshot()) != 1 || !q.Hint(2) {
			t.Fatal("confirmed terminal kept a follow-up or retained its capacity slot")
		}
		stopTestQueue(t, q)
	})
}

func TestQueueNoNewHintDoesNotLoopAfterErrorOrNonterminalReturn(t *testing.T) {
	for _, result := range []struct {
		outcome Outcome
		err     error
	}{
		{OutcomeNotReady, nil},
		{OutcomeNotApplied, nil},
		{OutcomeAdvanced, nil},
		{OutcomeSkipped, errors.New("synthetic database failure")},
	} {
		synctest.Test(t, func(t *testing.T) {
			checker := &queueCheckerStub{call: func(context.Context, int64, int) (Outcome, error) { return result.outcome, result.err }}
			q := startTestQueue(t, checker, Config{Capacity: 1, Concurrency: 1, Timeout: 2 * time.Second})
			q.Hint(1)
			time.Sleep(time.Minute)
			synctest.Wait()
			if len(checker.snapshot()) != 1 || !q.Hint(2) {
				t.Fatal("old hint retried itself or retained the only position")
			}
			stopTestQueue(t, q)
		})
	}
}

func TestQueueLimitsActualConcurrentCallsAndNeverOverlapsOneID(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		returnChecks := make(chan struct{})
		var active, maximum atomic.Int32
		var mu sync.Mutex
		activeIDs := make(map[int64]bool)
		checker := &queueCheckerStub{call: func(_ context.Context, id int64, _ int) (Outcome, error) {
			mu.Lock()
			if activeIDs[id] {
				t.Error("same Instance was checked concurrently")
			}
			activeIDs[id] = true
			mu.Unlock()
			current := active.Add(1)
			for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
			}
			<-returnChecks
			active.Add(-1)
			mu.Lock()
			delete(activeIDs, id)
			mu.Unlock()
			return OutcomeNotReady, nil
		}}
		q := startTestQueue(t, checker, Config{Capacity: 8, Concurrency: 3, Timeout: 2 * time.Second})
		for id := int64(1); id <= 8; id++ {
			if !q.Hint(id) {
				t.Fatal("configured capacity was not available")
			}
		}
		time.Sleep(mergeWindow)
		synctest.Wait()
		if len(checker.snapshot()) != 3 || active.Load() != 3 || q.Hint(9) || !q.Hint(1) {
			t.Fatal("processing count or unique-ID capacity was exceeded")
		}
		close(returnChecks)
		stopTestQueue(t, q)
		if maximum.Load() != 3 || len(checker.snapshot()) != 9 {
			t.Fatal("worker concurrency changed or an admitted follow-up disappeared")
		}
	})
}

func TestQueueProcessingTimeoutDoesNotReleaseAnUnreturnedCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn := make(chan struct{})
		checker := &queueCheckerStub{call: func(ctx context.Context, _ int64, call int) (Outcome, error) {
			if call == 1 {
				<-firstReturn
				return OutcomeSkipped, ctx.Err()
			}
			return OutcomeNotReady, nil
		}}
		q := startTestQueue(t, checker, Config{Capacity: 2, Concurrency: 1, Timeout: 2 * time.Second})
		q.Hint(1)
		q.Hint(2)
		time.Sleep(mergeWindow)
		synctest.Wait()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		calls := checker.snapshot()
		if len(calls) != 1 || !errors.Is(calls[0].ctx.Err(), context.DeadlineExceeded) || q.Hint(3) {
			t.Fatal("timeout freed a worker or capacity before the Check returned")
		}
		q.Hint(1)
		closedAt := time.Now()
		close(firstReturn)
		synctest.Wait()
		calls = checker.snapshot()
		if len(calls) != 2 || calls[1].id != 2 || !calls[1].started.Equal(closedAt) {
			t.Fatal("queued work did not begin when actual processing capacity returned")
		}
		deadline, ok := calls[1].ctx.Deadline()
		if !ok || deadline.Sub(calls[1].started) != 2*time.Second {
			t.Fatal("long queue waiting consumed the next check's own processing budget")
		}
		stopTestQueue(t, q)
		if len(checker.snapshot()) != 3 {
			t.Fatal("timeout erased a processing-time hint")
		}
	})
}

func TestQueueStopDrainsAdmittedHintsWithoutCancelingChecks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn := make(chan struct{})
		checker := &queueCheckerStub{call: func(ctx context.Context, _ int64, call int) (Outcome, error) {
			if call == 1 {
				<-firstReturn
			}
			if ctx.Err() != nil {
				t.Error("Stop canceled an admitted check")
			}
			return OutcomeNotReady, nil
		}}
		q := startTestQueue(t, checker, Config{Capacity: 2, Concurrency: 1, Timeout: 2 * time.Second})
		q.Hint(1)
		time.Sleep(mergeWindow)
		synctest.Wait()
		q.Hint(1)
		q.Hint(2)
		q.Stop()
		q.Stop()
		if q.Hint(1) || q.Hint(3) {
			t.Fatal("Stop still admitted hints")
		}
		close(firstReturn)
		if err := q.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := queueCallIDs(checker.snapshot()); !reflect.DeepEqual(got, []int64{1, 2, 1}) {
			t.Fatalf("Stop lost admitted queue work or its follow-up: %v", got)
		}
	})
}

func TestQueueDrainDeadlineWaitsForActualExitAndStopsFurtherChecks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn := make(chan struct{})
		checker := &queueCheckerStub{call: func(ctx context.Context, _ int64, _ int) (Outcome, error) {
			<-firstReturn
			return OutcomeSkipped, ctx.Err()
		}}
		q := startTestQueue(t, checker, Config{Capacity: 2, Concurrency: 1, Timeout: 2 * time.Second})
		q.Hint(1)
		time.Sleep(mergeWindow)
		synctest.Wait()
		q.Hint(1)
		q.Hint(2)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := q.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("unexpected drain result: %v", err)
		}
		synctest.Wait()
		calls := checker.snapshot()
		if calls[0].ctx.Err() == nil || !errors.Is(q.Wait(ctx), context.DeadlineExceeded) {
			t.Fatal("drain did not cancel work or incorrectly reported that it returned")
		}
		close(firstReturn)
		if err := q.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(checker.snapshot()) != 1 {
			t.Fatal("expired shutdown budget started queued checks or a follow-up")
		}
		if err := q.Drain(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("later Drain replaced the original shutdown outcome")
		}
	})
}

func TestQueueDrainClipsChecksStartedAfterShutdownBegins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		checker := &queueCheckerStub{}
		q := startTestQueue(t, checker, queueConfig())
		q.Hint(1)
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		sharedDeadline, _ := ctx.Deadline()
		if err := q.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		calls := checker.snapshot()
		if len(calls) != 1 {
			t.Fatal("admitted hint did not drain")
		}
		deadline, ok := calls[0].ctx.Deadline()
		if !ok || !deadline.Equal(sharedDeadline) {
			t.Fatal("check received a fresh budget beyond the shared shutdown deadline")
		}
	})
}

func TestQueueCompletionRacingHintCannotLoseAnAcceptedEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 24 {
			firstReturn := make(chan struct{})
			checker := &queueCheckerStub{call: func(_ context.Context, _ int64, call int) (Outcome, error) {
				if call == 1 {
					<-firstReturn
				}
				return OutcomeNotApplied, nil
			}}
			q := startTestQueue(t, checker, Config{Capacity: 1, Concurrency: 2, Timeout: 2 * time.Second})
			q.Hint(1)
			time.Sleep(mergeWindow)
			synctest.Wait()
			race := make(chan struct{})
			admitted := make(chan bool, 1)
			go func() { <-race; admitted <- q.Hint(1) }()
			go func() { <-race; close(firstReturn) }()
			close(race)
			if !<-admitted {
				t.Fatal("single-ID event was unexpectedly refused")
			}
			synctest.Wait()
			time.Sleep(mergeWindow)
			synctest.Wait()
			if len(checker.snapshot()) != 2 {
				t.Fatal("old check cleanup erased an admitted new event")
			}
			stopTestQueue(t, q)
		}
	})
}

func TestQueueFutureEntryDoesNotBlockAnAlreadyDueTailFollowup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn := make(chan struct{})
		checker := &queueCheckerStub{call: func(_ context.Context, _ int64, call int) (Outcome, error) {
			if call == 1 {
				<-firstReturn
			}
			return OutcomeNotReady, nil
		}}
		q := startTestQueue(t, checker, Config{Capacity: 2, Concurrency: 1, Timeout: 2 * time.Second})
		started := time.Now()
		q.Hint(1)
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		q.Hint(1)
		time.Sleep(180 * time.Millisecond)
		q.Hint(2)
		time.Sleep(20 * time.Millisecond)
		close(firstReturn)
		synctest.Wait()
		calls := checker.snapshot()
		if len(calls) != 2 || calls[1].id != 1 || !calls[1].started.Equal(started.Add(300*time.Millisecond)) {
			t.Fatal("future queue head blocked a ready tail follow-up")
		}
		stopTestQueue(t, q)
		calls = checker.snapshot()
		if len(calls) != 3 || calls[2].id != 2 || !calls[2].started.Equal(started.Add(380*time.Millisecond)) {
			t.Fatal("waiting head did not retain its original eligible time")
		}
	})
}

func TestQueuePanicKeepsConcurrentHintWithoutCreatingAnErrorRetryLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstReturn := make(chan struct{})
		checker := &queueCheckerStub{call: func(_ context.Context, _ int64, call int) (Outcome, error) {
			if call == 1 {
				<-firstReturn
			}
			panic("synthetic private diagnostic")
		}}
		q := startTestQueue(t, checker, Config{Capacity: 1, Concurrency: 1, Timeout: 2 * time.Second})
		q.Hint(1)
		time.Sleep(mergeWindow)
		synctest.Wait()
		q.Hint(1)
		close(firstReturn)
		time.Sleep(time.Minute)
		synctest.Wait()
		if len(checker.snapshot()) != 2 || !q.Hint(2) {
			t.Fatal("panic lost a concurrent hint, leaked capacity or retried without a new event")
		}
		stopTestQueue(t, q)
	})
}

func TestQueueFullHintLoggingWaitsForAnActualPositionRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		previous := logx.Reset()
		logx.SetWriter(logx.NewWriter(&logs))
		defer logx.SetWriter(previous)
		firstReturn := make(chan struct{})
		checker := &queueCheckerStub{call: func(_ context.Context, _ int64, call int) (Outcome, error) {
			if call == 1 {
				<-firstReturn
			}
			return OutcomeNotReady, nil
		}}
		q := startTestQueue(t, checker, Config{Capacity: 1, Concurrency: 1, Timeout: 2 * time.Second})
		q.Hint(1)
		time.Sleep(mergeWindow)
		synctest.Wait()
		for id := int64(2); id < 10; id++ {
			if q.Hint(id) {
				t.Fatal("full queue admitted another ID")
			}
		}
		const message = "job instance check hint dropped because the queue is full"
		if strings.Count(logs.String(), message) != 1 {
			t.Fatal("full admission was silent or logged every rejected hint")
		}
		q.Hint(1)
		close(firstReturn)
		synctest.Wait()
		if q.Hint(20) || strings.Count(logs.String(), message) != 1 {
			t.Fatal("follow-up tail requeue incorrectly released the logging latch or capacity")
		}
		time.Sleep(mergeWindow)
		synctest.Wait()
		if !q.Hint(30) || q.Hint(31) || strings.Count(logs.String(), message) != 2 {
			t.Fatal("actual ID release did not make the next saturation observable")
		}
		stopTestQueue(t, q)
	})
}

func TestQueueRecoveryUsesGlobalHalfCapacityWhileNormalHintsUseAllSlots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		checker := &queueCheckerStub{}
		q := startTestQueue(t, checker, Config{Capacity: 6, Concurrency: 1, Timeout: time.Second})
		defer q.Stop()
		ctx := context.Background()
		if !q.Hint(1) || !q.Hint(2) || q.HintRecovery(ctx, 3) != nil {
			t.Fatal("recovery did not fill the remaining space below the global threshold")
		}
		for _, operation := range []func(context.Context) error{
			q.WaitRecoveryCapacity,
			func(ctx context.Context) error { return q.HintRecovery(ctx, 4) },
		} {
			wait, cancel := context.WithTimeout(ctx, time.Millisecond)
			if err := operation(wait); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("recovery exceeded the shared half-capacity threshold: %v", err)
			}
			cancel()
		}
		for id := int64(4); id <= 6; id++ {
			if !q.Hint(id) {
				t.Fatal("recovery threshold reduced normal hint capacity")
			}
		}
		if q.Hint(7) || q.HintRecovery(ctx, 2) != nil {
			t.Fatal("full capacity rejected an existing recovery ID or admitted a new normal ID")
		}
		stopTestQueue(t, q)
		if got := queueCallIDs(checker.snapshot()); !reflect.DeepEqual(got, []int64{1, 2, 3, 4, 5, 6}) {
			t.Fatalf("waiting recovery merge manufactured another check: %v", got)
		}
	})
}

func TestQueueRecoveryMergeAtFullCapacityPreservesActiveFollowup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var once sync.Once
		releaseCheck := func() { once.Do(func() { close(release) }) }
		defer releaseCheck()
		checker := &queueCheckerStub{call: func(_ context.Context, _ int64, call int) (Outcome, error) {
			if call == 1 {
				<-release
			}
			return OutcomeNotReady, nil
		}}
		q := startTestQueue(t, checker, Config{Capacity: 2, Concurrency: 1, Timeout: time.Second})
		defer q.Stop()
		q.Hint(1)
		time.Sleep(mergeWindow)
		synctest.Wait()
		if !q.Hint(2) || q.HintRecovery(context.Background(), 1) != nil {
			t.Fatal("recovery hint could not merge with an active ID at full capacity")
		}
		time.Sleep(mergeWindow)
		if q.HintRecovery(context.Background(), 1) != nil {
			t.Fatal("second recovery hint failed to merge")
		}
		releaseCheck()
		synctest.Wait()
		stopTestQueue(t, q)
		if got := queueCallIDs(checker.snapshot()); !reflect.DeepEqual(got, []int64{1, 2, 1}) {
			t.Fatalf("recovery follow-up was dropped, postponed or duplicated: %v", got)
		}
	})
}

func TestQueueRecoveryWaitsForActualReleaseAndDoesNotRetainDiscoveryContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var once sync.Once
		releaseCheck := func() { once.Do(func() { close(release) }) }
		defer releaseCheck()
		checker := &queueCheckerStub{call: func(_ context.Context, _ int64, call int) (Outcome, error) {
			if call == 1 {
				<-release
			}
			return OutcomeNotReady, nil
		}}
		q := startTestQueue(t, checker, Config{Capacity: 2, Concurrency: 1, Timeout: 2 * time.Second})
		defer q.Stop()
		q.Hint(1)
		time.Sleep(mergeWindow)
		synctest.Wait()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		admitted := make(chan error, 1)
		go func() { admitted <- q.HintRecovery(ctx, 2) }()
		synctest.Wait()
		select {
		case <-admitted:
			t.Fatal("processing ID was excluded from recovery occupancy")
		default:
		}
		releaseCheck()
		if err := <-admitted; err != nil {
			t.Fatalf("released capacity did not wake recovery: %v", err)
		}
		cancel()
		time.Sleep(mergeWindow)
		synctest.Wait()
		stopTestQueue(t, q)
		calls := checker.snapshot()
		if len(calls) != 2 || calls[1].id != 2 {
			t.Fatal("admitted recovery work disappeared after discovery cancellation")
		}
		deadline, ok := calls[1].ctx.Deadline()
		if !ok || deadline.Sub(calls[1].started) != 2*time.Second {
			t.Fatal("discovery context replaced the independent check budget")
		}
	})
}

func TestQueueRecoveryRejectsInvalidOrClosedAdmissionAndAllowsSingleCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newTestQueue(t, &queueCheckerStub{}, Config{Capacity: 1, Concurrency: 1, Timeout: time.Second})
		if q.WaitRecoveryCapacity(context.Background()) == nil || q.HintRecovery(context.Background(), 1) == nil ||
			q.WaitRecoveryCapacity(nil) == nil || q.HintRecovery(nil, 1) == nil || q.HintRecovery(context.Background(), 0) == nil {
			t.Fatal("invalid recovery admission accepted")
		}
		if err := q.Start(); err != nil {
			t.Fatal(err)
		}
		defer q.Stop()
		if err := q.HintRecovery(context.Background(), 1); err != nil {
			t.Fatal("minimum recovery threshold prevented a single-slot queue from progressing")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if !errors.Is(q.HintRecovery(ctx, 1), context.Canceled) {
			t.Fatal("canceled discovery still merged a hint")
		}
		closed := make(chan error, 1)
		go func() { closed <- q.WaitRecoveryCapacity(context.Background()) }()
		synctest.Wait()
		q.Stop()
		if err := <-closed; err == nil || q.HintRecovery(context.Background(), 1) == nil {
			t.Fatal("queue shutdown did not release recovery waiters with an error")
		}
		stopTestQueue(t, q)
	})
}

func queueConfig() Config { return Config{Capacity: 512, Concurrency: 8, Timeout: 2 * time.Second} }

func newTestQueue(t *testing.T, checker checker, config Config) *Queue {
	t.Helper()
	q, err := NewQueue(checker, config)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func startTestQueue(t *testing.T, checker checker, config Config) *Queue {
	t.Helper()
	q := newTestQueue(t, checker, config)
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

func queueCallIDs(calls []queueCheckCall) []int64 {
	ids := make([]int64, len(calls))
	for index, call := range calls {
		ids[index] = call.id
	}
	return ids
}

type queueCheckCall struct {
	id      int64
	started time.Time
	ctx     context.Context
}

type queueCheckerStub struct {
	call  func(context.Context, int64, int) (Outcome, error)
	mu    sync.Mutex
	calls []queueCheckCall
}

func (s *queueCheckerStub) Check(ctx context.Context, id int64) (Outcome, error) {
	s.mu.Lock()
	s.calls = append(s.calls, queueCheckCall{id: id, started: time.Now(), ctx: ctx})
	number := len(s.calls)
	s.mu.Unlock()
	if s.call != nil {
		return s.call(ctx, id, number)
	}
	return OutcomeNotReady, nil
}

func (s *queueCheckerStub) snapshot() []queueCheckCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]queueCheckCall(nil), s.calls...)
}
