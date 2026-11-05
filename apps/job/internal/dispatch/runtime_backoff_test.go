package dispatch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestRuntimeSlowFourthPageStartsBackoffAfterActualReturn(t *testing.T) {
	for _, failure := range []bool{false, true} {
		name := "success"
		if failure {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r, source, tasks := runtimeWithSlowFourthPage(t, &dispatcherRegistrarStub{}, failure)
				startRuntime(t, r)
				defer stopRuntime(t, r)
				synctest.Wait()
				if len(source.snapshot()) != 2 || len(tasks.snapshot()) != 2 {
					t.Fatal("expected alternating waiting/retry pages with the fourth query still running")
				}
				// The last query exceeds the whole idle interval. That elapsed time
				// must not grant another four pages when it eventually returns.
				time.Sleep(2 * time.Second)
				synctest.Wait()
				if runtimeTotalPages(r, source) != 4 {
					t.Fatal("slow query consumed the backoff interval before returning")
				}
				time.Sleep(799 * time.Millisecond)
				synctest.Wait()
				if runtimeTotalPages(r, source) != 4 {
					t.Fatal("switching candidate sources bypassed the shared post-query backoff")
				}
				time.Sleep(402 * time.Millisecond)
				synctest.Wait()
				waiting := source.snapshot()
				if runtimeTotalPages(r, source) != 8 || len(waiting) < 3 || waiting[2].cursor != 256 || source.maxCalls.Load() != 1 {
					t.Fatal("backoff did not resume the existing traversals with one shared four-page budget")
				}
			})
		})
	}
}

func TestRuntimeRegistrationDuringSlowFourthPageRestoresSharedBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registrationGate := make(chan struct{})
		release := sync.OnceFunc(func() { close(registrationGate) })
		var first atomic.Bool
		registrar := &dispatcherRegistrarStub{call: func(_ context.Context, id int64) (*RegisteredAttempt, error) {
			if first.Swap(true) {
				return nil, nil
			}
			<-registrationGate
			attempt := dispatcherAttempt()
			attempt.InstanceID = id
			return &attempt, nil
		}}
		r, source, _ := runtimeWithSlowFourthPage(t, registrar, false)
		startRuntime(t, r)
		defer func() {
			release()
			stopRuntime(t, r)
		}()
		synctest.Wait()
		if runtimeTotalPages(r, source) != 4 {
			t.Fatal("scanner did not reach its slow fourth page")
		}
		time.Sleep(time.Second)
		release()
		synctest.Wait()
		if runtimeTotalPages(r, source) != 4 {
			t.Fatal("registration progress started a concurrent candidate query")
		}
		// A current scan candidate committed while the page was in flight. Its
		// progress credit survives the return and avoids an unnecessary idle wait.
		time.Sleep(time.Second)
		synctest.Wait()
		if runtimeTotalPages(r, source) != 8 {
			t.Fatal("slow query return discarded confirmed registration progress or accumulated extra budget")
		}
	})
}

func runtimeWithSlowFourthPage(t *testing.T, registrar ordinaryRegistrar, failure bool) (*Runtime, *runtimeSourceStub, *runtimeTaskStoreStub) {
	t.Helper()
	source := runtimeFullPages(4096)
	retryCalls := 0
	tasks := &runtimeTaskStoreStub{responseStore: &dispatcherStoreStub{},
		list: func(_ context.Context, now time.Time, cursor *repository.RetryCursor, limit int) ([]repository.RetryCandidate, error) {
			retryCalls++
			if retryCalls == 2 {
				time.Sleep(2 * time.Second)
				if failure {
					return nil, errors.New("synthetic slow retry query failure")
				}
			}
			var lastID int64
			if cursor != nil {
				lastID = cursor.TaskID
			}
			rows := make([]repository.RetryCandidate, limit)
			for index := range rows {
				id := lastID + int64(index) + 1
				rows[index] = repository.RetryCandidate{TaskID: id, InstanceID: 10000 + id, AttemptNo: 1,
					NextDispatchAt: now.Add(-time.Second)}
			}
			return rows, nil
		}}
	config := runtimeTestConfig()
	config.WaitingCapacity, config.ScanInterval, config.QueryTimeout = 1024, time.Second, 5*time.Second
	r := runtimeForTest(t, source, registrar, &dispatcherSenderStub{}, tasks, config)
	return r, source, tasks
}
