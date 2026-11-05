package execution

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/zeromicro/go-zero/core/logx"
)

type notificationSourceStub struct {
	query func(context.Context, int64, string, int) ([]string, error)
}

func (s *notificationSourceStub) ListCancelTargets(ctx context.Context, id int64, after string, limit int) ([]string, error) {
	return s.query(ctx, id, after, limit)
}

type notificationSenderStub struct {
	call func(context.Context, CancelTarget) error
}

func (s *notificationSenderStub) Cancel(ctx context.Context, target CancelTarget) error {
	return s.call(ctx, target)
}

func notificationConfigFixture() NotificationConfig {
	return NotificationConfig{InstanceCapacity: 4, PageSize: 2, TargetCapacity: 4,
		Concurrency: 2, QueryTimeout: time.Second, InstanceTimeout: time.Minute}
}

func notificationFixture(id int64) Notification {
	return Notification{InstanceID: id, ExecutorGroupID: 10, Reason: model.EndReasonManualStop}
}

func startTestNotifier(t *testing.T, source *notificationSourceStub, sender *notificationSenderStub, config NotificationConfig) *Notifier {
	t.Helper()
	n, err := NewNotifier(source, sender, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	return n
}

func finishTestNotifier(t *testing.T, n *Notifier) {
	t.Helper()
	n.Stop()
	if err := n.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNotifierValidatesConstructionAndSingleUseLifecycle(t *testing.T) {
	source := &notificationSourceStub{query: func(context.Context, int64, string, int) ([]string, error) { return nil, nil }}
	sender := &notificationSenderStub{call: func(context.Context, CancelTarget) error { return nil }}
	config := notificationConfigFixture()
	var absentSource *notificationSourceStub
	var absentSender *notificationSenderStub
	for _, dependency := range []notificationSource{nil, absentSource} {
		if n, err := NewNotifier(dependency, sender, config); err == nil || n != nil {
			t.Fatal("nil address source was accepted")
		}
	}
	for _, dependency := range []notificationSender{nil, absentSender} {
		if n, err := NewNotifier(source, dependency, config); err == nil || n != nil {
			t.Fatal("nil sender was accepted")
		}
	}
	for _, edit := range []func(*NotificationConfig){
		func(c *NotificationConfig) { c.InstanceCapacity = 0 },
		func(c *NotificationConfig) { c.PageSize = 0 },
		func(c *NotificationConfig) { c.PageSize = 33 },
		func(c *NotificationConfig) { c.TargetCapacity = 0 },
		func(c *NotificationConfig) { c.Concurrency = 0 },
		func(c *NotificationConfig) { c.QueryTimeout = 0 },
		func(c *NotificationConfig) { c.InstanceTimeout = 0 },
	} {
		invalid := config
		edit(&invalid)
		if n, err := NewNotifier(source, sender, invalid); err == nil || n != nil {
			t.Fatal("invalid notification configuration was accepted")
		}
	}
	n, err := NewNotifier(source, sender, config)
	if err != nil {
		t.Fatal(err)
	}
	if n.Hint(notificationFixture(1)) || n.Drain(nil) == nil {
		t.Fatal("unused notifier admitted work or accepted a nil drain context")
	}
	n.Stop()
	n.Stop()
	if n.Start() == nil || n.Hint(notificationFixture(1)) || n.Wait(context.Background()) != nil || n.Drain(context.Background()) != nil {
		t.Fatal("Stop-before-Start did not finish the unused notifier")
	}
}

func TestNotifierRotatesPagesAndPreservesDatabaseOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstQuery := make(chan struct{})
		type query struct {
			id    int64
			after string
		}
		var queries []query
		source := &notificationSourceStub{query: func(_ context.Context, id int64, after string, limit int) ([]string, error) {
			queries = append(queries, query{id, after})
			if limit != 2 {
				t.Errorf("page limit = %d", limit)
			}
			if id == 1 && after == "" {
				<-firstQuery
				// A database collation need not use Go's byte ordering. The
				// notifier must preserve the repository's returned sequence.
				return []string{"z:9", "a:9"}, nil
			}
			if id == 2 && after == "" {
				return []string{"b:9"}, nil
			}
			return []string{}, nil
		}}
		var mu sync.Mutex
		delivered := make(map[string]int)
		sender := &notificationSenderStub{call: func(_ context.Context, target CancelTarget) error {
			mu.Lock()
			defer mu.Unlock()
			delivered[target.Address]++
			if target.ExecutorGroupID != 10 || target.Reason != model.EndReasonManualStop {
				t.Error("notification lost its committed group or reason")
			}
			return nil
		}}
		n := startTestNotifier(t, source, sender, notificationConfigFixture())
		if n.Start() == nil || !n.Hint(notificationFixture(1)) {
			t.Fatal("single-use start or initial admission failed")
		}
		synctest.Wait()
		if !n.Hint(notificationFixture(1)) || !n.Hint(notificationFixture(2)) {
			t.Fatal("duplicate/new notification refused")
		}
		conflict := notificationFixture(1)
		conflict.Reason = model.EndReasonInstanceTimeout
		if n.Hint(conflict) || n.Hint(Notification{}) {
			t.Fatal("conflicting decision or invalid identity was merged")
		}
		close(firstQuery)
		synctest.Wait()
		finishTestNotifier(t, n)
		want := []query{{1, ""}, {2, ""}, {1, "a:9"}, {2, "b:9"}}
		if !reflect.DeepEqual(queries, want) || !reflect.DeepEqual(delivered, map[string]int{"z:9": 1, "a:9": 1, "b:9": 1}) {
			t.Fatalf("page rotation, cursor or duplicate coalescing changed: queries=%v delivered=%v", queries, delivered)
		}
	})
}

func TestNotifierTargetCapacityIncludesActiveCallsAndPausesCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		queries, sends := 0, 0
		source := &notificationSourceStub{query: func(_ context.Context, _ int64, after string, _ int) ([]string, error) {
			queries++
			if after == "" {
				return []string{"first:9", "second:9"}, nil
			}
			return []string{}, nil
		}}
		sender := &notificationSenderStub{call: func(context.Context, CancelTarget) error {
			sends++
			if sends == 1 {
				<-release
			}
			return nil
		}}
		config := notificationConfigFixture()
		config.InstanceCapacity, config.TargetCapacity, config.Concurrency = 1, 1, 1
		n := startTestNotifier(t, source, sender, config)
		n.Hint(notificationFixture(1))
		synctest.Wait()
		n.mu.Lock()
		entry := n.entries[1]
		cursor, retained, targets := entry.cursor, len(n.entries), n.targetCount
		n.mu.Unlock()
		if queries != 1 || sends != 1 || cursor != "first:9" || retained != 1 || targets != 1 ||
			!n.Hint(notificationFixture(1)) || n.Hint(notificationFixture(2)) {
			t.Error("active send released capacity, skipped its unaccepted address or allowed read-ahead")
		}
		close(release)
		synctest.Wait()
		finishTestNotifier(t, n)
		if queries != 2 || sends != 2 {
			t.Fatalf("paused page was lost or repeated: queries=%d sends=%d", queries, sends)
		}
	})
}

func TestNotifierExpiryDiscardsWaitingTargetsButRetainsActualCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var activeCtx context.Context
		sends := 0
		source := &notificationSourceStub{query: func(_ context.Context, _ int64, after string, _ int) ([]string, error) {
			if after == "" {
				return []string{"first:9", "second:9"}, nil
			}
			return []string{}, nil
		}}
		sender := &notificationSenderStub{call: func(ctx context.Context, _ CancelTarget) error {
			sends++
			activeCtx = ctx
			<-release
			return ctx.Err()
		}}
		config := notificationConfigFixture()
		config.InstanceCapacity, config.Concurrency, config.InstanceTimeout = 1, 1, 10*time.Second
		n := startTestNotifier(t, source, sender, config)
		n.Hint(notificationFixture(1))
		synctest.Wait()
		time.Sleep(8 * time.Second)
		if !n.Hint(notificationFixture(1)) {
			t.Fatal("duplicate did not merge")
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		n.mu.Lock()
		entries, targets, queued := len(n.entries), n.targetCount, n.targets.Len()
		n.mu.Unlock()
		if !errors.Is(activeCtx.Err(), context.DeadlineExceeded) || entries != 1 || targets != 1 || queued != 0 ||
			sends != 1 || n.Hint(notificationFixture(2)) {
			t.Errorf("natural expiry must preserve DeadlineExceeded and active ownership: cause=%v entries=%d targets=%d queued=%d sends=%d",
				activeCtx.Err(), entries, targets, queued, sends)
		}
		close(release)
		synctest.Wait()
		finishTestNotifier(t, n)
		if sends != 1 {
			t.Fatal("expired queued target was sent after the active call returned")
		}
	})
}

func TestNotifierQueryFailureRetainsAlreadyAcceptedTargets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		queries, sends := 0, 0
		source := &notificationSourceStub{query: func(_ context.Context, _ int64, after string, _ int) ([]string, error) {
			queries++
			if after == "" {
				return []string{"target:9"}, nil
			}
			return nil, errors.New("synthetic-private-query-token")
		}}
		sender := &notificationSenderStub{call: func(ctx context.Context, _ CancelTarget) error {
			sends++
			<-release
			if ctx.Err() != nil {
				t.Error("query failure canceled an accepted target")
			}
			return nil
		}}
		config := notificationConfigFixture()
		config.InstanceCapacity, config.PageSize = 1, 1
		n := startTestNotifier(t, source, sender, config)
		n.Hint(notificationFixture(1))
		synctest.Wait()
		if queries != 2 || sends != 1 || n.Hint(notificationFixture(2)) {
			t.Error("failed enumeration was retried or discarded active target ownership")
		}
		close(release)
		synctest.Wait()
		finishTestNotifier(t, n)
	})
}

func TestNotifierBoundsSenderConcurrencyAndStopsReadingWhenTargetsAreFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		queries := 0
		source := &notificationSourceStub{query: func(_ context.Context, _ int64, after string, _ int) ([]string, error) {
			queries++
			switch after {
			case "":
				return []string{"a:9", "b:9"}, nil
			case "b:9":
				return []string{"c:9", "d:9"}, nil
			case "d:9":
				return []string{"e:9", "f:9"}, nil
			default:
				return nil, nil
			}
		}}
		var mu sync.Mutex
		active, maximum, calls := 0, 0, 0
		sender := &notificationSenderStub{call: func(context.Context, CancelTarget) error {
			mu.Lock()
			active++
			calls++
			maximum = max(maximum, active)
			mu.Unlock()
			<-release
			mu.Lock()
			active--
			mu.Unlock()
			return nil
		}}
		n := startTestNotifier(t, source, sender, notificationConfigFixture())
		n.Hint(notificationFixture(1))
		synctest.Wait()
		n.mu.Lock()
		targets, waiting := n.targetCount, n.targets.Len()
		n.mu.Unlock()
		if queries != 2 || targets != 4 || waiting != 2 || active != 2 || maximum != 2 {
			t.Errorf("unbounded sends or address read-ahead: queries=%d targets=%d waiting=%d active=%d max=%d", queries, targets, waiting, active, maximum)
		}
		close(release)
		synctest.Wait()
		finishTestNotifier(t, n)
		if calls != 6 || maximum != 2 || queries != 4 {
			t.Fatalf("bounded sends lost work: calls=%d max=%d queries=%d", calls, maximum, queries)
		}
	})
}

func TestNotifierInstanceBudgetCoversWaitingAndClipsQueryBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var queryCtx context.Context
		queries := 0
		source := &notificationSourceStub{query: func(ctx context.Context, _ int64, _ string, _ int) ([]string, error) {
			queries++
			queryCtx = ctx
			<-release
			return nil, nil
		}}
		sender := &notificationSenderStub{call: func(context.Context, CancelTarget) error {
			t.Error("expired query started a notification")
			return nil
		}}
		config := notificationConfigFixture()
		config.InstanceTimeout, config.QueryTimeout = time.Second, time.Hour
		n := startTestNotifier(t, source, sender, config)
		n.Hint(notificationFixture(1))
		synctest.Wait()
		n.Hint(notificationFixture(2))
		time.Sleep(time.Second)
		synctest.Wait()
		n.mu.Lock()
		entries, queued := len(n.entries), n.enumerating.Len()
		n.mu.Unlock()
		if queries != 1 || entries != 1 || queued != 0 || !errors.Is(queryCtx.Err(), context.DeadlineExceeded) {
			t.Error("waiting time escaped the original budget or query ownership was released before return")
		}
		close(release)
		synctest.Wait()
		finishTestNotifier(t, n)
	})
}

func TestNotifierQueryBudgetWaitsForActualReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var queryCtx context.Context
		sends := 0
		source := &notificationSourceStub{query: func(ctx context.Context, _ int64, _ string, _ int) ([]string, error) {
			queryCtx = ctx
			<-release
			return []string{"late:9"}, nil
		}}
		sender := &notificationSenderStub{call: func(context.Context, CancelTarget) error { sends++; return nil }}
		config := notificationConfigFixture()
		config.InstanceCapacity = 1
		n := startTestNotifier(t, source, sender, config)
		n.Hint(notificationFixture(1))
		synctest.Wait()
		time.Sleep(config.QueryTimeout)
		synctest.Wait()
		if !errors.Is(queryCtx.Err(), context.DeadlineExceeded) || n.Hint(notificationFixture(2)) {
			t.Error("query timeout was missing or released a still-running query")
		}
		close(release)
		synctest.Wait()
		finishTestNotifier(t, n)
		if sends != 0 {
			t.Fatal("query response after its deadline started a notification")
		}
	})
}

func TestNotifierDrainUsesSharedDeadlineAndWaitsForActualExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var activeCtx context.Context
		sends := 0
		source := &notificationSourceStub{query: func(_ context.Context, _ int64, after string, _ int) ([]string, error) {
			if after == "" {
				return []string{"first:9", "second:9"}, nil
			}
			return []string{}, nil
		}}
		sender := &notificationSenderStub{call: func(ctx context.Context, _ CancelTarget) error {
			sends++
			activeCtx = ctx
			<-release
			return ctx.Err()
		}}
		config := notificationConfigFixture()
		config.Concurrency = 1
		n := startTestNotifier(t, source, sender, config)
		n.Hint(notificationFixture(1))
		synctest.Wait()
		drainCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := n.Drain(drainCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Drain = %v", err)
		}
		synctest.Wait()
		if activeCtx.Err() == nil || n.Hint(notificationFixture(2)) {
			t.Error("shutdown did not cancel active work or close admission")
		}
		select {
		case <-n.done:
			t.Error("shutdown declared completion while a dependency still ran")
		default:
		}
		later, stopLater := context.WithTimeout(context.Background(), time.Second)
		defer stopLater()
		if err := n.Drain(later); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("later Drain = %v", err)
		}
		close(release)
		finishTestNotifier(t, n)
		if sends != 1 || !errors.Is(n.Drain(context.Background()), context.DeadlineExceeded) {
			t.Fatal("shutdown granted a fresh budget or started waiting work")
		}
	})
}

func TestNotifierFailureLogsExcludeDependencyContent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs executorClientLogBuffer
		previous := logx.Reset()
		logx.SetWriter(logx.NewWriter(&logs))
		defer logx.SetWriter(previous)
		source := &notificationSourceStub{query: func(_ context.Context, id int64, after string, _ int) ([]string, error) {
			if id == 1 {
				return nil, errors.New("synthetic-private-query-token")
			}
			if after == "" {
				return []string{"synthetic-private-address:9"}, nil
			}
			return nil, nil
		}}
		sender := &notificationSenderStub{call: func(context.Context, CancelTarget) error {
			panic("synthetic-private-panic-token")
		}}
		n := startTestNotifier(t, source, sender, notificationConfigFixture())
		n.Hint(notificationFixture(1))
		n.Hint(notificationFixture(2))
		synctest.Wait()
		finishTestNotifier(t, n)
		content := logs.String()
		if strings.Contains(content, "synthetic-private") || !strings.Contains(content, "address query failed") ||
			!strings.Contains(content, "target notification unconfirmed") {
			t.Fatalf("unsafe or missing notification diagnostics: %s", content)
		}
	})
}
