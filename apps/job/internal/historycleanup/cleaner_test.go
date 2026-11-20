package historycleanup

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

type instanceStub struct {
	repository.JobInstanceRepository
	clock  func(context.Context) (time.Time, error)
	list   func(context.Context, time.Time, *repository.HistoryCandidate, int) ([]repository.HistoryCandidate, error)
	lock   func(context.Context, int64) (*repository.HistoryInstance, error)
	delete func(context.Context, int64, time.Time) (bool, error)
}

func (s *instanceStub) DatabaseTime(ctx context.Context) (time.Time, error) { return s.clock(ctx) }
func (s *instanceStub) ListExpiredHistory(ctx context.Context, before time.Time, after *repository.HistoryCandidate, limit int) ([]repository.HistoryCandidate, error) {
	return s.list(ctx, before, after, limit)
}
func (s *instanceStub) LockForHistoryCleanup(ctx context.Context, id int64) (*repository.HistoryInstance, error) {
	return s.lock(ctx, id)
}
func (s *instanceStub) DeleteHistory(ctx context.Context, id int64, before time.Time) (bool, error) {
	return s.delete(ctx, id, before)
}

type taskStub struct {
	repository.JobTaskRepository
	delete func(context.Context, int64) (int64, error)
}

func (s *taskStub) DeleteHistoryForInstance(ctx context.Context, id int64) (int64, error) {
	return s.delete(ctx, id)
}

func fixture(t *testing.T) (*Cleaner, *instanceStub, *taskStub, time.Time) {
	t.Helper()
	now := time.Date(2026, time.November, 20, 0, 0, 0, 0, time.UTC)
	old := now.Add(-8 * 24 * time.Hour)
	instances := &instanceStub{
		clock: func(context.Context) (time.Time, error) { return now, nil },
		list: func(context.Context, time.Time, *repository.HistoryCandidate, int) ([]repository.HistoryCandidate, error) {
			return nil, nil
		},
		lock: func(_ context.Context, id int64) (*repository.HistoryInstance, error) {
			return &repository.HistoryInstance{ID: id, Status: model.InstanceSucceeded, FinishedAt: &old}, nil
		},
		delete: func(context.Context, int64, time.Time) (bool, error) { return true, nil },
	}
	tasks := &taskStub{delete: func(context.Context, int64) (int64, error) { return 1, nil }}
	transact := func(ctx context.Context, fn func(repository.Repositories) error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(repository.Repositories{Instances: instances, Tasks: tasks})
	}
	cleaner, err := New(instances, transact, Config{Retention: 7 * 24 * time.Hour,
		Interval: 4 * time.Hour, PageSize: 2, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return cleaner, instances, tasks, now
}

func TestCleanupTraversesPagesAndRechecksParents(t *testing.T) {
	c, instances, tasks, now := fixture(t)
	old := now.Add(-8 * 24 * time.Hour)
	cutoff := now.Add(-c.config.Retention)
	clockCalls, pageCalls := 0, 0
	instances.clock = func(context.Context) (time.Time, error) { clockCalls++; return now, nil }
	instances.list = func(_ context.Context, before time.Time, after *repository.HistoryCandidate, limit int) ([]repository.HistoryCandidate, error) {
		pageCalls++
		if !before.Equal(cutoff) || limit != 2 {
			t.Fatalf("discovery cutoff or limit changed: %v, %d", before, limit)
		}
		start := int64(1)
		if after != nil {
			start = after.ID + 1
		}
		var page []repository.HistoryCandidate
		for id := start; id <= 6 && len(page) < limit; id++ {
			page = append(page, repository.HistoryCandidate{ID: id, FinishedAt: old})
		}
		return page, nil
	}
	instances.lock = func(_ context.Context, id int64) (*repository.HistoryInstance, error) {
		parent := &repository.HistoryInstance{ID: id, Status: model.InstanceSucceeded, FinishedAt: &old}
		switch id {
		case 2:
			return nil, repository.ErrInstanceLocked
		case 3:
			return nil, nil
		case 4:
			parent.Status = model.InstanceRunning
		case 5:
			parent.FinishedAt = &cutoff
		case 6:
			parent.Status = model.InstanceFailed
		}
		return parent, nil
	}
	var childDeletes, parentDeletes []int64
	tasks.delete = func(_ context.Context, id int64) (int64, error) {
		childDeletes = append(childDeletes, id)
		return id, nil
	}
	instances.delete = func(_ context.Context, id int64, before time.Time) (bool, error) {
		if !before.Equal(cutoff) || len(childDeletes) != len(parentDeletes)+1 {
			t.Fatal("parent deletion did not follow child deletion with the fixed cutoff")
		}
		parentDeletes = append(parentDeletes, id)
		return true, nil
	}
	result, err := c.scan(context.Background())
	if err != nil || result != (passResult{deleted: 2, tasks: 7, skipped: 4}) {
		t.Fatalf("result=%+v, err=%v", result, err)
	}
	if clockCalls != 1 || pageCalls != 4 || !reflect.DeepEqual(parentDeletes, []int64{1, 6}) || !reflect.DeepEqual(childDeletes, parentDeletes) {
		t.Fatalf("clock=%d, pages=%d, parents=%v, children=%v", clockCalls, pageCalls, parentDeletes, childDeletes)
	}
}

func TestCleanupNeverCountsAnUnconfirmedCommit(t *testing.T) {
	c, instances, tasks, now := fixture(t)
	commitErr := errors.New("commit confirmation lost")
	callbackDone := false
	c.transact = func(_ context.Context, fn func(repository.Repositories) error) error {
		if err := fn(repository.Repositories{Instances: instances, Tasks: tasks}); err != nil {
			return err
		}
		callbackDone = true
		return commitErr
	}
	deleted, count, err := c.deleteInstance(context.Background(), 1, now.Add(-c.config.Retention))
	if !callbackDone || deleted || count != 0 || !errors.Is(err, commitErr) {
		t.Fatalf("callback=%v, deleted=%v, count=%d, err=%v", callbackDone, deleted, count, err)
	}
}

func TestCleanupAbortsTheTransactionOnDeleteFailure(t *testing.T) {
	for _, stage := range []string{"children", "parent", "remaining_children"} {
		t.Run(stage, func(t *testing.T) {
			c, instances, tasks, now := fixture(t)
			storageErr := errors.New("storage failed")
			parentCalls := 0
			tasks.delete = func(context.Context, int64) (int64, error) {
				if stage == "children" {
					return 0, storageErr
				}
				return 502, nil
			}
			instances.delete = func(context.Context, int64, time.Time) (bool, error) {
				parentCalls++
				if stage == "parent" {
					return false, storageErr
				}
				return false, nil
			}
			deleted, count, err := c.deleteInstance(context.Background(), 1, now.Add(-c.config.Retention))
			if deleted || count != 0 || err == nil {
				t.Fatalf("failed deletion was reported as success: %v, %d, %v", deleted, count, err)
			}
			if stage == "children" && parentCalls != 0 {
				t.Fatal("parent deletion continued after child failure")
			}
			if stage == "remaining_children" && !errors.Is(err, errInvalidHistory) {
				t.Fatalf("unexpected guard failure: %v", err)
			}
		})
	}
}

func TestCleanupProtectsEveryActiveStateAndMissingFinishTime(t *testing.T) {
	for _, status := range []model.InstanceStatus{model.InstanceWaiting, model.InstanceRunning, model.InstanceStopping, 99} {
		c, instances, tasks, now := fixture(t)
		old := now.Add(-30 * 24 * time.Hour)
		instances.lock = func(context.Context, int64) (*repository.HistoryInstance, error) {
			return &repository.HistoryInstance{ID: 1, Status: status, FinishedAt: &old}, nil
		}
		tasks.delete = func(context.Context, int64) (int64, error) { t.Fatal("deleted active history"); return 0, nil }
		if deleted, _, err := c.deleteInstance(context.Background(), 1, now.Add(-c.config.Retention)); deleted || err != nil {
			t.Fatalf("state %d: deleted=%v, err=%v", status, deleted, err)
		}
	}
	for _, finished := range []*time.Time{nil, new(time.Time)} {
		c, instances, tasks, now := fixture(t)
		instances.lock = func(context.Context, int64) (*repository.HistoryInstance, error) {
			return &repository.HistoryInstance{ID: 1, Status: model.InstanceCanceled, FinishedAt: finished}, nil
		}
		tasks.delete = func(context.Context, int64) (int64, error) {
			t.Fatal("deleted history without a finish time")
			return 0, nil
		}
		if deleted, _, err := c.deleteInstance(context.Background(), 1, now.Add(-c.config.Retention)); deleted || err != nil {
			t.Fatalf("deleted=%v, err=%v", deleted, err)
		}
	}
}

func TestCleanupRejectsMalformedDiscoveryBeforeAnyWrite(t *testing.T) {
	c, instances, _, now := fixture(t)
	old := now.Add(-8 * 24 * time.Hour)
	cutoff := now.Add(-c.config.Retention)
	pages := [][]repository.HistoryCandidate{
		{{ID: 0, FinishedAt: old}},
		{{ID: 1}},
		{{ID: 1, FinishedAt: cutoff}},
		{{ID: 2, FinishedAt: old}, {ID: 1, FinishedAt: old}},
		{{ID: 1, FinishedAt: old}, {ID: 1, FinishedAt: old}},
		{{ID: 1, FinishedAt: old}, {ID: 2, FinishedAt: old}, {ID: 3, FinishedAt: old}},
	}
	c.transact = func(context.Context, func(repository.Repositories) error) error {
		t.Fatal("invalid page reached deletion")
		return nil
	}
	for _, page := range pages {
		instances.list = func(context.Context, time.Time, *repository.HistoryCandidate, int) ([]repository.HistoryCandidate, error) {
			return page, nil
		}
		if _, err := c.scan(context.Background()); !errors.Is(err, errInvalidHistory) {
			t.Fatalf("page %+v: %v", page, err)
		}
	}
	storageErr := errors.New("partial query failure")
	instances.list = func(context.Context, time.Time, *repository.HistoryCandidate, int) ([]repository.HistoryCandidate, error) {
		return []repository.HistoryCandidate{{ID: 1, FinishedAt: old}}, storageErr
	}
	if _, err := c.scan(context.Background()); !errors.Is(err, storageErr) {
		t.Fatalf("partial query result used: %v", err)
	}
}

func TestCleanupStartsImmediatelyAndWaitsAfterActualReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, instances, _, now := fixture(t)
		c.config.Interval, c.config.OperationTimeout = time.Minute, time.Hour
		release := make(chan struct{})
		calls := 0
		instances.clock = func(context.Context) (time.Time, error) {
			calls++
			if calls == 1 {
				<-release
			}
			return now, nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()
		synctest.Wait()
		if calls != 1 {
			t.Fatalf("initial pass not started: %d", calls)
		}
		time.Sleep(3 * time.Minute)
		synctest.Wait()
		if calls != 1 {
			t.Fatal("overlapping pass started")
		}
		close(release)
		synctest.Wait()
		time.Sleep(time.Minute - time.Second)
		synctest.Wait()
		if calls != 1 {
			t.Fatal("missed ticks were replayed")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if calls != 2 {
			t.Fatalf("next pass did not start after completion-relative interval: %d", calls)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run: %v", err)
		}
	})
}

func TestCleanupCancellationWaitsForStorageAndRejectsOverlappingRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, instances, _, now := fixture(t)
		release := make(chan struct{})
		instances.clock = func(context.Context) (time.Time, error) { <-release; return now, nil }
		instances.list = func(context.Context, time.Time, *repository.HistoryCandidate, int) ([]repository.HistoryCandidate, error) {
			t.Fatal("discovery continued after term cancellation")
			return nil, nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("released maintenance before storage returned")
		default:
		}
		if err := c.Run(context.Background()); err == nil {
			t.Fatal("accepted overlapping Run")
		}
		close(release)
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run: %v", err)
		}
		if err := c.Run(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled new Run: %v", err)
		}
	})
}

func TestCleanupPassFailureWaitsAndRestartsWithFreshDatabaseTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, instances, _, now := fixture(t)
		c.config.Interval = time.Minute
		calls := 0
		instances.clock = func(context.Context) (time.Time, error) {
			calls++
			if calls == 1 {
				return time.Time{}, errors.New("database unavailable")
			}
			return now.Add(time.Hour), nil
		}
		instances.list = func(_ context.Context, before time.Time, after *repository.HistoryCandidate, _ int) ([]repository.HistoryCandidate, error) {
			if !before.Equal(now.Add(time.Hour-c.config.Retention)) || after != nil {
				t.Fatal("retried pass reused an old cutoff or cursor")
			}
			return nil, nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()
		synctest.Wait()
		if calls != 1 {
			t.Fatalf("initial clock calls: %d", calls)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if calls != 2 {
			t.Fatalf("failed pass did not wait before retry: %d", calls)
		}
		cancel()
		<-done
	})
}

func TestCleanupOperationDeadlinePreventsFurtherWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, instances, tasks, now := fixture(t)
		instances.lock = func(ctx context.Context, _ int64) (*repository.HistoryInstance, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		tasks.delete = func(context.Context, int64) (int64, error) { t.Fatal("deleted after lock timeout"); return 0, nil }
		deleted, _, err := c.deleteInstance(context.Background(), 1, now.Add(-c.config.Retention))
		if deleted || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deleted=%v, err=%v", deleted, err)
		}
	})
}

func TestCleanupRestartsAfterFailureWithoutRepeatingConfirmedDeletes(t *testing.T) {
	c, instances, tasks, now := fixture(t)
	old := now.Add(-8 * 24 * time.Hour)
	removed := map[int64]bool{}
	fail := true
	instances.list = func(_ context.Context, _ time.Time, after *repository.HistoryCandidate, limit int) ([]repository.HistoryCandidate, error) {
		var page []repository.HistoryCandidate
		for id := int64(1); id <= 3 && len(page) < limit; id++ {
			if !removed[id] && (after == nil || id > after.ID) {
				page = append(page, repository.HistoryCandidate{ID: id, FinishedAt: old})
			}
		}
		return page, nil
	}
	tasks.delete = func(_ context.Context, id int64) (int64, error) {
		if id == 2 && fail {
			return 0, context.DeadlineExceeded
		}
		return 1, nil
	}
	instances.delete = func(_ context.Context, id int64, _ time.Time) (bool, error) {
		if removed[id] {
			t.Fatal("already committed deletion was repeated")
		}
		removed[id] = true
		return true, nil
	}
	first, err := c.scan(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || first.deleted != 2 || first.failed != 1 || first.failedInstance != 2 || first.failureStage != "instance_transaction" || failureReason(err) != "deadline_exceeded" {
		t.Fatalf("first pass=%+v error=%v", first, err)
	}
	fail = false
	second, err := c.scan(context.Background())
	if err != nil || second.deleted != 1 || len(removed) != 3 {
		t.Fatalf("resumed pass=%+v error=%v remaining=%v", second, err, removed)
	}
}

func TestNewRejectsInvalidConfigurationAndDependencies(t *testing.T) {
	c, instances, _, _ := fixture(t)
	for _, change := range []func(*Config){
		func(c *Config) { c.Retention = 0 },
		func(c *Config) { c.Interval = -1 },
		func(c *Config) { c.PageSize = 0 },
		func(c *Config) { c.PageSize = 101 },
		func(c *Config) { c.OperationTimeout = 0 },
	} {
		cfg := c.config
		change(&cfg)
		if _, err := New(instances, c.transact, cfg); err == nil {
			t.Fatalf("accepted config %+v", cfg)
		}
	}
	var absent *instanceStub
	for _, source := range []historySource{nil, absent} {
		if _, err := New(source, c.transact, c.config); err == nil {
			t.Fatal("accepted missing source")
		}
	}
	if _, err := New(instances, nil, c.config); err == nil {
		t.Fatal("accepted missing transaction runner")
	}
	if err := c.Run(nil); err == nil {
		t.Fatal("accepted nil context")
	}
}
