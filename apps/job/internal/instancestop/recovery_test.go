package instancestop

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"
)

func stopRecoveryConfig() RecoveryConfig {
	return RecoveryConfig{PageSize: 2, RunTimeout: 5 * time.Second, SQLTimeout: time.Second}
}

func TestNewRecoveryRejectsInvalidDependenciesAndSettings(t *testing.T) {
	valid := stopRecoveryConfig()
	hint := func(int64) bool { return true }
	var typedNil *stoppingSourceStub
	for _, source := range []stoppingSource{nil, typedNil} {
		if _, err := NewRecovery(source, hint, valid); err == nil {
			t.Fatal("nil stopping source accepted")
		}
	}
	if _, err := NewRecovery(&stoppingSourceStub{}, nil, valid); err == nil {
		t.Fatal("nil admission callback accepted")
	}
	for _, change := range []func(*RecoveryConfig){
		func(c *RecoveryConfig) { c.PageSize = 0 },
		func(c *RecoveryConfig) { c.RunTimeout = 0 }, func(c *RecoveryConfig) { c.SQLTimeout = 0 },
		func(c *RecoveryConfig) { c.PageSize = -1 },
		func(c *RecoveryConfig) { c.RunTimeout = -time.Second }, func(c *RecoveryConfig) { c.SQLTimeout = -time.Second },
	} {
		config := valid
		change(&config)
		if _, err := NewRecovery(&stoppingSourceStub{}, hint, config); err == nil {
			t.Fatal("invalid stopping discovery settings accepted")
		}
	}
}

func TestStoppingScanKeepsFixedUpperBoundAndStopsBeforeUnadmittedID(t *testing.T) {
	ids := []int64{1, 3, 5, 7}
	var bounds [][3]int64
	upperCalls := 0
	source := &stoppingSourceStub{
		upper: func(context.Context) (int64, error) { upperCalls++; return ids[len(ids)-1], nil },
		list: func(_ context.Context, after, upper int64, limit int) ([]int64, error) {
			bounds = append(bounds, [3]int64{after, upper, int64(limit)})
			page := make([]int64, 0, limit)
			for _, id := range ids {
				if id > after && id <= upper && len(page) < limit {
					page = append(page, id)
				}
			}
			return page, nil
		},
	}
	full := true
	var admitted []int64
	recovery, err := NewRecovery(source, func(id int64) bool {
		if full && id == 5 {
			return false
		}
		admitted = append(admitted, id)
		return true
	}, stopRecoveryConfig())
	if err != nil {
		t.Fatal(err)
	}
	var position stoppingPosition
	if err := recovery.scan(context.Background(), &position); err != nil || position != (stoppingPosition{upper: 7, after: 3}) ||
		!reflect.DeepEqual(admitted, []int64{1, 3}) || upperCalls != 1 || len(bounds) != 2 {
		t.Fatalf("full admission advanced past rejected ID: position=%+v admitted=%v bounds=%v error=%v", position, admitted, bounds, err)
	}
	// A lower late commit and a newly allocated upper ID belong to a fresh
	// traversal. The continuation retains the old bound and last admitted ID.
	ids, full = []int64{1, 2, 3, 5, 7, 9}, false
	if err := recovery.scan(context.Background(), &position); err != nil || position != (stoppingPosition{}) || upperCalls != 1 ||
		!reflect.DeepEqual(admitted, []int64{1, 3, 5, 7}) || bounds[2] != ([3]int64{3, 7, 2}) {
		t.Fatalf("continuation changed its bounds: position=%+v admitted=%v bounds=%v error=%v", position, admitted, bounds, err)
	}
	admitted = nil
	if err := recovery.scan(context.Background(), &position); err != nil || position != (stoppingPosition{}) || upperCalls != 2 ||
		!reflect.DeepEqual(admitted, ids) || bounds[3] != ([3]int64{0, 9, 2}) {
		t.Fatalf("new traversal failed to revisit lower IDs: position=%+v admitted=%v bounds=%v error=%v", position, admitted, bounds, err)
	}
}

func TestStoppingScanRetainsCursorOnErrorsAndRejectsMalformedPages(t *testing.T) {
	cause := errors.New("synthetic stopping query failure")
	for _, page := range [][]int64{{4, 5, 6}, {0}, {3}, {4, 4}, {5, 4}, {10}} {
		calls := 0
		source := &stoppingSourceStub{list: func(context.Context, int64, int64, int) ([]int64, error) { return page, nil }}
		recovery, err := NewRecovery(source, func(int64) bool { calls++; return true }, stopRecoveryConfig())
		if err != nil {
			t.Fatal(err)
		}
		position := stoppingPosition{upper: 9, after: 3}
		if err := recovery.scan(context.Background(), &position); !errors.Is(err, ErrInvalidState) || calls != 0 || position != (stoppingPosition{upper: 9, after: 3}) {
			t.Fatalf("malformed page advanced scan: page=%v position=%+v calls=%d error=%v", page, position, calls, err)
		}
	}
	for _, failUpper := range []bool{false, true} {
		source := &stoppingSourceStub{
			upper: func(context.Context) (int64, error) { return 9, cause },
			list:  func(context.Context, int64, int64, int) ([]int64, error) { return []int64{4}, cause },
		}
		recovery, err := NewRecovery(source, func(int64) bool { t.Error("failed query admitted work"); return true }, stopRecoveryConfig())
		if err != nil {
			t.Fatal(err)
		}
		position := stoppingPosition{upper: 9, after: 3}
		if failUpper {
			position = stoppingPosition{}
		}
		before := position
		if err := recovery.scan(context.Background(), &position); !errors.Is(err, cause) || position != before {
			t.Fatalf("failed query lost its cause or cursor: position=%+v error=%v", position, err)
		}
	}
}

func TestStoppingScanEmptyTraversalAndMissingUpperRemainNormal(t *testing.T) {
	for _, upper := range []int64{0, 9, -1} {
		pages := 0
		recovery, err := NewRecovery(&stoppingSourceStub{
			upper: func(context.Context) (int64, error) { return upper, nil },
			list:  func(context.Context, int64, int64, int) ([]int64, error) { pages++; return nil, nil },
		}, func(int64) bool { t.Error("empty traversal admitted work"); return true }, stopRecoveryConfig())
		if err != nil {
			t.Fatal(err)
		}
		var position stoppingPosition
		err = recovery.scan(context.Background(), &position)
		if upper < 0 {
			if !errors.Is(err, ErrInvalidState) || pages != 0 {
				t.Fatal("negative upper bound accepted")
			}
		} else if err != nil || position != (stoppingPosition{}) || (upper == 0 && pages != 0) || (upper > 0 && pages != 1) {
			t.Fatalf("empty traversal failed: upper=%d position=%+v pages=%d error=%v", upper, position, pages, err)
		}
	}
}

func TestStoppingScanBudgetRejectsLateSuccessWithoutAdvancing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, budget := range []time.Duration{100 * time.Millisecond, 2 * time.Second} {
			var elapsed time.Duration
			recovery, err := NewRecovery(&stoppingSourceStub{list: func(ctx context.Context, _, _ int64, _ int) ([]int64, error) {
				started := time.Now()
				<-ctx.Done()
				elapsed = time.Since(started)
				return []int64{4}, nil
			}}, func(int64) bool { t.Error("late query success admitted work"); return true }, stopRecoveryConfig())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			position := stoppingPosition{upper: 9, after: 3}
			err = recovery.scan(ctx, &position)
			cancel()
			want := min(budget, time.Second)
			if !errors.Is(err, context.DeadlineExceeded) || elapsed != want || position != (stoppingPosition{upper: 9, after: 3}) {
				t.Fatalf("query escaped effective budget or advanced cursor: elapsed=%s want=%s position=%+v error=%v", elapsed, want, position, err)
			}
		}
	})
}

func TestStoppingRecoveryRunsImmediatelyAndResumesOnlyOnNextInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		upperCalls, pageCalls, offered := 0, 0, 0
		recovery, err := NewRecovery(&stoppingSourceStub{
			upper: func(context.Context) (int64, error) { upperCalls++; return 9, nil },
			list: func(_ context.Context, after, upper int64, limit int) ([]int64, error) {
				pageCalls++
				if after != 0 || upper != 9 || limit != 2 {
					t.Errorf("rejected ID was skipped: after=%d upper=%d limit=%d", after, upper, limit)
				}
				return []int64{9}, nil
			},
		}, func(int64) bool { offered++; return false }, stopRecoveryConfig())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		ticks := make(chan struct{})
		go func() { done <- recovery.Run(ctx, ticks) }()
		synctest.Wait()
		if upperCalls != 1 || pageCalls != 1 || offered != 1 {
			t.Fatal("discovery did not perform exactly one immediate pass")
		}
		time.Sleep(time.Minute - time.Nanosecond)
		synctest.Wait()
		if pageCalls != 1 {
			t.Fatal("full admission caused an immediate retry loop")
		}
		ticks <- struct{}{}
		synctest.Wait()
		if upperCalls != 1 || pageCalls != 2 || offered != 2 {
			t.Fatal("next interval failed to resume the rejected candidate")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("discovery shutdown=%v", err)
		}
	})
}

func TestStoppingRecoveryLostTermWaitsForQueryAndNextTermStartsFromZero(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		upperCalls, pageCalls := 0, 0
		release := make(chan struct{})
		var admitted []int64
		source := &stoppingSourceStub{
			upper: func(context.Context) (int64, error) { upperCalls++; return 9, nil },
			list: func(ctx context.Context, after, _ int64, _ int) ([]int64, error) {
				pageCalls++
				if pageCalls == 1 {
					return []int64{1, 3}, nil
				}
				if pageCalls == 2 {
					if after != 3 {
						t.Error("old traversal lost its admitted position")
					}
					<-ctx.Done()
					<-release
					return []int64{5, 9}, nil
				}
				if after != 0 {
					t.Error("new term inherited the old term cursor")
				}
				return nil, nil
			},
		}
		recovery, err := NewRecovery(source, func(id int64) bool { admitted = append(admitted, id); return true }, stopRecoveryConfig())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer func() {
			cancel()
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		done := make(chan error, 1)
		go func() { done <- recovery.Run(ctx, make(chan struct{})) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("lost term returned before its query actually exited")
		default:
		}
		competing, cancelCompeting := context.WithCancel(context.Background())
		cancelCompeting()
		if err := recovery.Run(competing, make(chan struct{})); err == nil || errors.Is(err, context.Canceled) {
			t.Fatal("overlapping recovery Run was accepted")
		}
		close(release)
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("old recovery shutdown=%v", err)
		}
		if !reflect.DeepEqual(admitted, []int64{1, 3}) {
			t.Fatalf("lost term admitted late query results: %v", admitted)
		}
		next, cancelNext := context.WithCancel(context.Background())
		defer cancelNext()
		go func() { done <- recovery.Run(next, make(chan struct{})) }()
		synctest.Wait()
		if upperCalls != 2 || pageCalls != 3 {
			t.Fatal("fresh term did not rebuild its traversal")
		}
		cancelNext()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("new recovery shutdown=%v", err)
		}
	})
}

func TestStoppingRecoveryRunBudgetBoundsPassWithoutImmediateRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := stopRecoveryConfig()
		config.RunTimeout = 50 * time.Millisecond
		upperCalls, pageCalls := 0, 0
		var queryFailure error
		recovery, err := NewRecovery(&stoppingSourceStub{
			upper: func(context.Context) (int64, error) { upperCalls++; return 9, nil },
			list: func(ctx context.Context, _, _ int64, _ int) ([]int64, error) {
				pageCalls++
				<-ctx.Done()
				queryFailure = ctx.Err()
				return []int64{9}, nil
			},
		}, func(int64) bool { t.Error("expired pass admitted work"); return true }, config)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- recovery.Run(ctx, make(chan struct{})) }()
		synctest.Wait()
		time.Sleep(config.RunTimeout)
		synctest.Wait()
		if !errors.Is(queryFailure, context.DeadlineExceeded) || upperCalls != 1 || pageCalls != 1 {
			t.Fatalf("run budget did not bound its query: upper=%d pages=%d query error=%v", upperCalls, pageCalls, queryFailure)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("bounded pass shutdown=%v", err)
		}
	})
}

type stoppingSourceStub struct {
	upper func(context.Context) (int64, error)
	list  func(context.Context, int64, int64, int) ([]int64, error)
}

func (s *stoppingSourceStub) StoppingUpperID(ctx context.Context) (int64, error) {
	if s.upper == nil {
		return 0, nil
	}
	return s.upper(ctx)
}

func (s *stoppingSourceStub) ListStoppingCandidates(ctx context.Context, after, upper int64, limit int) ([]int64, error) {
	if s.list == nil {
		return nil, nil
	}
	return s.list(ctx, after, upper, limit)
}
