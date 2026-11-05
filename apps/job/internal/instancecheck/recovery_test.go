package instancecheck

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func runningRecoveryConfig() RecoveryConfig {
	return RecoveryConfig{PageSize: 2, RunTimeout: 5 * time.Second, SQLTimeout: time.Second, MinAge: 4 * time.Minute}
}

func runningRecoveryTime() time.Time { return time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC) }

func TestNewRecoveryValidatesWithoutReadingOrAdmitting(t *testing.T) {
	source, admission, config := &runningSourceStub{}, &recoveryAdmissionStub{}, runningRecoveryConfig()
	var missingSource *runningSourceStub
	var missingAdmission *recoveryAdmissionStub
	for _, dependency := range []runningSource{nil, missingSource} {
		if _, err := NewRecovery(dependency, admission, config); err == nil {
			t.Fatal("nil running source accepted")
		}
	}
	for _, dependency := range []recoveryAdmission{nil, missingAdmission} {
		if _, err := NewRecovery(source, dependency, config); err == nil {
			t.Fatal("nil recovery admission accepted")
		}
	}
	for _, edit := range []func(*RecoveryConfig){
		func(c *RecoveryConfig) { c.PageSize = 0 }, func(c *RecoveryConfig) { c.PageSize = -1 },
		func(c *RecoveryConfig) { c.RunTimeout = 0 }, func(c *RecoveryConfig) { c.SQLTimeout = -time.Second },
		func(c *RecoveryConfig) { c.MinAge = 0 },
	} {
		invalid := config
		edit(&invalid)
		if _, err := NewRecovery(source, admission, invalid); err == nil {
			t.Fatal("invalid running recovery settings accepted")
		}
	}
	r := newRunningRecovery(t, source, admission, config)
	if r.Run(nil, make(chan struct{})) == nil || r.Run(context.Background(), nil) == nil {
		t.Fatal("missing run context or ticks accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Run(ctx, make(chan struct{})); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled recovery startup did work")
	}
}

func TestRunningScanResumesOnlyAcceptedCursorWithFixedTimeAndRevisitsLateRows(t *testing.T) {
	now := runningRecoveryTime()
	one := repository.RunningCandidate{ID: 1, StartedAt: now.Add(-8 * time.Minute)}
	two := repository.RunningCandidate{ID: 2, StartedAt: now.Add(-8 * time.Minute)}
	three := repository.RunningCandidate{ID: 3, StartedAt: now.Add(-6 * time.Minute)}
	late := repository.RunningCandidate{ID: 4, StartedAt: now.Add(-9 * time.Minute)}
	type query struct {
		bound time.Time
		after *repository.RunningCandidate
	}
	var queries []query
	clockCalls, roomCalls := 0, 0
	source := &runningSourceStub{
		clock: func(context.Context) (time.Time, error) { clockCalls++; return now, nil },
		list: func(_ context.Context, bound time.Time, after *repository.RunningCandidate, limit int) ([]repository.RunningCandidate, error) {
			if limit != 2 {
				t.Fatal("scan ignored bounded page size")
			}
			var saved *repository.RunningCandidate
			if after != nil {
				copy := *after
				saved = &copy
			}
			queries = append(queries, query{bound, saved})
			switch len(queries) {
			case 1:
				return []repository.RunningCandidate{one, two}, nil
			case 2:
				return []repository.RunningCandidate{two, three}, nil
			case 3:
				return nil, nil
			case 4:
				return []repository.RunningCandidate{late, one}, nil
			default:
				return nil, nil
			}
		},
	}
	var admitted []int64
	full := errors.New("synthetic recovery admission timeout")
	blockTwo := true
	r := newRunningRecovery(t, source, &recoveryAdmissionStub{
		room: func(context.Context) error { roomCalls++; return nil },
		hint: func(_ context.Context, id int64) error {
			if blockTwo && id == 2 {
				return full
			}
			admitted = append(admitted, id)
			return nil
		},
	}, runningRecoveryConfig())
	var position runningPosition
	if err := r.scan(context.Background(), &position); !errors.Is(err, full) || position.after == nil || *position.after != one ||
		!position.eligibleBefore.Equal(now.Add(-4*time.Minute)) || !reflect.DeepEqual(admitted, []int64{1}) {
		t.Fatalf("first unadmitted candidate was skipped: position=%+v admitted=%v error=%v", position, admitted, err)
	}
	originalBound := position.eligibleBefore
	now, blockTwo = now.Add(2*time.Minute), false
	if err := r.scan(context.Background(), &position); err != nil || position.after != nil || !position.eligibleBefore.IsZero() || clockCalls != 1 ||
		!reflect.DeepEqual(admitted, []int64{1, 2, 3}) || queries[1].after == nil || *queries[1].after != one || !queries[1].bound.Equal(originalBound) || !queries[2].bound.Equal(originalBound) {
		t.Fatalf("continuation refreshed time or lost cursor: position=%+v admitted=%v queries=%v error=%v", position, admitted, queries, err)
	}
	if err := r.scan(context.Background(), &position); err != nil || clockCalls != 2 || queries[3].after != nil || !queries[3].bound.Equal(now.Add(-4*time.Minute)) ||
		!reflect.DeepEqual(admitted, []int64{1, 2, 3, 4, 1}) || roomCalls != len(queries) {
		t.Fatalf("fresh traversal failed to reset: admitted=%v queries=%v room=%d error=%v", admitted, queries, roomCalls, err)
	}
}

func TestRunningScanRetainsCursorOnQueryFailureAndRejectsMalformedPages(t *testing.T) {
	now := runningRecoveryTime()
	previous := repository.RunningCandidate{ID: 3, StartedAt: now.Add(-8 * time.Minute)}
	valid := repository.RunningCandidate{ID: 4, StartedAt: previous.StartedAt}
	position := runningPosition{eligibleBefore: now.Add(-4 * time.Minute), after: &previous}
	pages := [][]repository.RunningCandidate{
		{valid, {ID: 5, StartedAt: valid.StartedAt}, {ID: 6, StartedAt: valid.StartedAt}},
		{{ID: 0, StartedAt: valid.StartedAt}}, {{ID: 4}}, {previous},
		{valid, valid}, {{ID: 5, StartedAt: valid.StartedAt}, valid},
		{{ID: 4, StartedAt: previous.StartedAt.Add(-time.Second)}},
		{{ID: 4, StartedAt: position.eligibleBefore}},
	}
	for _, page := range pages {
		r := newRunningRecovery(t, &runningSourceStub{list: func(context.Context, time.Time, *repository.RunningCandidate, int) ([]repository.RunningCandidate, error) {
			return page, nil
		}}, &recoveryAdmissionStub{hint: func(context.Context, int64) error { t.Error("malformed page admitted work"); return nil }}, runningRecoveryConfig())
		cursor := position
		if err := r.scan(context.Background(), &cursor); !errors.Is(err, ErrInvalidFacts) || !reflect.DeepEqual(cursor, position) {
			t.Fatalf("malformed page advanced scan: page=%v cursor=%+v error=%v", page, cursor, err)
		}
	}
	cause := errors.New("synthetic partial query failure")
	r := newRunningRecovery(t, &runningSourceStub{list: func(context.Context, time.Time, *repository.RunningCandidate, int) ([]repository.RunningCandidate, error) {
		return []repository.RunningCandidate{valid}, cause
	}}, &recoveryAdmissionStub{hint: func(context.Context, int64) error { t.Error("failed query admitted work"); return nil }}, runningRecoveryConfig())
	cursor := position
	if err := r.scan(context.Background(), &cursor); !errors.Is(err, cause) || !reflect.DeepEqual(cursor, position) {
		t.Fatal("failed query lost its cause or changed the admitted cursor")
	}
	for _, failure := range []error{nil, cause} {
		r = newRunningRecovery(t, &runningSourceStub{clock: func(context.Context) (time.Time, error) { return time.Time{}, failure }}, &recoveryAdmissionStub{}, runningRecoveryConfig())
		var cursor runningPosition
		err := r.scan(context.Background(), &cursor)
		if err == nil || !cursor.eligibleBefore.IsZero() || cursor.after != nil || (failure != nil && !errors.Is(err, failure)) {
			t.Fatal("invalid or failed database clock created a traversal")
		}
	}
}

func TestRunningScanWaitsBeforeQueriesAndNeverSkipsBlockedHandoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := runningRecoveryTime()
		clockCalls, pageCalls, hintCalls := 0, 0, 0
		blockedRoom := true
		source := &runningSourceStub{
			clock: func(context.Context) (time.Time, error) { clockCalls++; return now, nil },
			list: func(context.Context, time.Time, *repository.RunningCandidate, int) ([]repository.RunningCandidate, error) {
				pageCalls++
				return []repository.RunningCandidate{{ID: 1, StartedAt: now.Add(-8 * time.Minute)}, {ID: 2, StartedAt: now.Add(-7 * time.Minute)}}, nil
			},
		}
		r := newRunningRecovery(t, source, &recoveryAdmissionStub{
			room: func(ctx context.Context) error {
				if blockedRoom {
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			},
			hint: func(ctx context.Context, id int64) error {
				hintCalls++
				if id != 1 {
					t.Error("scan skipped a blocked first candidate to merge a later ID")
				}
				<-ctx.Done()
				return ctx.Err()
			},
		}, runningRecoveryConfig())
		for _, room := range []bool{true, false} {
			blockedRoom = room
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			var position runningPosition
			err := r.scan(ctx, &position)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) || position.after != nil {
				t.Fatal("blocked handoff advanced a cursor or escaped its budget")
			}
			if room && (clockCalls != 0 || pageCalls != 0 || hintCalls != 0) {
				t.Fatal("capacity wait held or requested database work")
			}
		}
		if clockCalls != 1 || pageCalls != 1 || hintCalls != 1 {
			t.Fatal("blocked candidate caused another query or handoff")
		}
	})
}

func TestRunningRecoveryWaitsForTicksAfterTraversalAndKeepsPassesBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clocks, pages := 0, 0
		var queryFailure error
		config := runningRecoveryConfig()
		config.RunTimeout = 50 * time.Millisecond
		r := newRunningRecovery(t, &runningSourceStub{
			clock: func(context.Context) (time.Time, error) { clocks++; return runningRecoveryTime(), nil },
			list: func(ctx context.Context, _ time.Time, _ *repository.RunningCandidate, _ int) ([]repository.RunningCandidate, error) {
				pages++
				if pages == 1 {
					<-ctx.Done()
					queryFailure = ctx.Err()
					return []repository.RunningCandidate{{ID: 1, StartedAt: runningRecoveryTime().Add(-8 * time.Minute)}}, nil
				}
				return nil, nil
			},
		}, &recoveryAdmissionStub{hint: func(context.Context, int64) error { t.Error("expired query result admitted"); return nil }}, config)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ticks := make(chan struct{})
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, ticks) }()
		synctest.Wait()
		time.Sleep(config.RunTimeout)
		synctest.Wait()
		if clocks != 1 || pages != 1 || !errors.Is(queryFailure, context.DeadlineExceeded) {
			t.Fatal("initial pass did not bound query cleanup or immediately retried")
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if pages != 1 {
			t.Fatal("recovery invented a tick")
		}
		ticks <- struct{}{}
		synctest.Wait()
		if clocks != 1 || pages != 2 {
			t.Fatal("continuation refreshed its fixed clock")
		}
		ticks <- struct{}{}
		synctest.Wait()
		if clocks != 2 || pages != 3 {
			t.Fatal("completed traversal did not start fresh on the next tick")
		}
		close(ticks)
		if err := <-done; err == nil || errors.Is(err, context.Canceled) {
			t.Fatal("closed tick source did not terminate recovery")
		}
	})
}

func TestRunningRecoverySQLBudgetCoversClockAndCandidateReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, blockClock := range []bool{true, false} {
			now := runningRecoveryTime()
			clockCalls, pages := 0, 0
			r := newRunningRecovery(t, &runningSourceStub{
				clock: func(ctx context.Context) (time.Time, error) {
					clockCalls++
					if blockClock {
						<-ctx.Done()
					}
					return now, nil
				},
				list: func(ctx context.Context, _ time.Time, _ *repository.RunningCandidate, _ int) ([]repository.RunningCandidate, error) {
					pages++
					<-ctx.Done()
					return []repository.RunningCandidate{{ID: 1, StartedAt: now.Add(-8 * time.Minute)}}, nil
				},
			}, &recoveryAdmissionStub{hint: func(context.Context, int64) error { t.Error("late SQL success admitted work"); return nil }}, runningRecoveryConfig())
			started := time.Now()
			var position runningPosition
			err := r.scan(context.Background(), &position)
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) != time.Second || clockCalls != 1 || position.after != nil ||
				(blockClock && (pages != 0 || !position.eligibleBefore.IsZero())) || (!blockClock && pages != 1) {
				t.Fatalf("SQL query escaped its budget or advanced state: clock=%v clocks=%d pages=%d position=%+v error=%v", blockClock, clockCalls, pages, position, err)
			}
		}
	})
}

func TestRunningRecoveryRetainsActualQueryOwnershipAndDiscardsLostTermCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := runningRecoveryTime()
		clocks, pages := 0, 0
		release := make(chan struct{})
		var admitted []int64
		r := newRunningRecovery(t, &runningSourceStub{
			clock: func(context.Context) (time.Time, error) { clocks++; return now, nil },
			list: func(ctx context.Context, _ time.Time, after *repository.RunningCandidate, _ int) ([]repository.RunningCandidate, error) {
				pages++
				if pages == 1 {
					return []repository.RunningCandidate{{ID: 1, StartedAt: now.Add(-8 * time.Minute)}}, nil
				}
				if pages == 2 {
					if after == nil || after.ID != 1 {
						t.Error("first traversal lost its admitted cursor")
					}
					<-ctx.Done()
					<-release
					return []repository.RunningCandidate{{ID: 2, StartedAt: now.Add(-7 * time.Minute)}}, nil
				}
				if after != nil {
					t.Error("new leader term retained the lost term cursor")
				}
				return nil, nil
			},
		}, &recoveryAdmissionStub{hint: func(_ context.Context, id int64) error { admitted = append(admitted, id); return nil }}, runningRecoveryConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer func() {
			cancel()
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		ticks := make(chan struct{})
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, ticks) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("discovery released ownership before canceled query returned")
		default:
		}
		competing, cancelCompeting := context.WithCancel(context.Background())
		cancelCompeting()
		if err := r.Run(competing, ticks); err == nil || errors.Is(err, context.Canceled) {
			t.Fatal("another Run overlapped the old query")
		}
		close(release)
		if err := <-done; !errors.Is(err, context.Canceled) || !reflect.DeepEqual(admitted, []int64{1}) {
			t.Fatal("lost term handed off a late query result")
		}
		next, cancelNext := context.WithCancel(context.Background())
		defer cancelNext()
		go func() { done <- r.Run(next, ticks) }()
		synctest.Wait()
		if clocks != 2 || pages != 3 {
			t.Fatal("fresh term did not rebuild discovery")
		}
		cancelNext()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}

func TestRunningRecoveryContinuesBeyondHalfCapacityWithinOnePass(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		checker := &queueCheckerStub{}
		q := startTestQueue(t, checker, Config{Capacity: 4, Concurrency: 1, Timeout: time.Second})
		defer q.Stop()
		now := runningRecoveryTime()
		pages := 0
		r := newRunningRecovery(t, &runningSourceStub{list: func(_ context.Context, _ time.Time, after *repository.RunningCandidate, limit int) ([]repository.RunningCandidate, error) {
			pages++
			first := int64(1)
			if after != nil {
				first = after.ID + 1
			}
			var rows []repository.RunningCandidate
			for id := first; id <= 6 && len(rows) < limit; id++ {
				rows = append(rows, repository.RunningCandidate{ID: id, StartedAt: now.Add(-8 * time.Minute)})
			}
			return rows, nil
		}}, q, runningRecoveryConfig())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var position runningPosition
		if err := r.scan(ctx, &position); err != nil || pages != 4 || position.after != nil || !position.eligibleBefore.IsZero() {
			t.Fatalf("half-capacity threshold became a per-pass total: pages=%d position=%+v error=%v", pages, position, err)
		}
		stopTestQueue(t, q)
		if got := queueCallIDs(checker.snapshot()); !reflect.DeepEqual(got, []int64{1, 2, 3, 4, 5, 6}) {
			t.Fatalf("one pass did not refill released capacity: %v", got)
		}
	})
}

func newRunningRecovery(t *testing.T, source runningSource, admission recoveryAdmission, config RecoveryConfig) *Recovery {
	t.Helper()
	r, err := NewRecovery(source, admission, config)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

type runningSourceStub struct {
	clock func(context.Context) (time.Time, error)
	list  func(context.Context, time.Time, *repository.RunningCandidate, int) ([]repository.RunningCandidate, error)
}

func (s *runningSourceStub) DatabaseTime(ctx context.Context) (time.Time, error) {
	if s.clock != nil {
		return s.clock(ctx)
	}
	return runningRecoveryTime(), nil
}

func (s *runningSourceStub) ListRunningCandidates(ctx context.Context, bound time.Time, after *repository.RunningCandidate, limit int) ([]repository.RunningCandidate, error) {
	if s.list != nil {
		return s.list(ctx, bound, after, limit)
	}
	return nil, nil
}

type recoveryAdmissionStub struct {
	room func(context.Context) error
	hint func(context.Context, int64) error
}

func (s *recoveryAdmissionStub) WaitRecoveryCapacity(ctx context.Context) error {
	if s.room != nil {
		return s.room(ctx)
	}
	return ctx.Err()
}

func (s *recoveryAdmissionStub) HintRecovery(ctx context.Context, id int64) error {
	if s.hint != nil {
		return s.hint(ctx, id)
	}
	return ctx.Err()
}
