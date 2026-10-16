package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestRealCronInitializesEveryRuleInLargeBatches(t *testing.T) {
	for _, count := range []int{500, 1100} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			r := unitRuntime(triggerServiceStub{})
			r.config.QueueCapacity = count
			term := unitTerm(context.Background(), r, r.newCron())
			t.Cleanup(func() { <-term.cron.Stop().Done(); term.queue.close() })
			rules := annualRules(count)
			term.apply(rules)
			entries := term.cron.Entries()
			if len(entries) != count || term.queue.size != count {
				t.Fatalf("rules or due checks truncated: entries=%d checks=%d", len(entries), term.queue.size)
			}
			for _, entry := range entries {
				if entry.Next.IsZero() {
					t.Fatal("real Cron returned an uninitialized entry after batch acknowledgement")
				}
			}
		})
	}
}

func TestHundredSimultaneousJobsAndRepeatedHintsStayBounded(t *testing.T) {
	q := newJobQueue(100)
	defer q.close()
	start := make(chan struct{})
	var producers sync.WaitGroup
	for id := int64(1); id <= 100; id++ {
		producers.Go(func() {
			<-start
			for range 10 {
				if !q.offer(id) {
					t.Errorf("existing Job %d was rejected within the 100-Job capacity", id)
				}
			}
		})
	}
	close(start)
	producers.Wait()
	if len(q.jobs) != 100 || q.size != 100 || q.offer(101) {
		t.Fatal("same-time burst exceeded the distinct Job capacity")
	}
	seen := make(map[int64]bool, 100)
	for range 100 {
		id, ok := q.take()
		if !ok || seen[id] {
			t.Fatalf("burst lost or duplicated job %d", id)
		}
		seen[id] = true
		q.complete(id)
	}
	if len(q.jobs) != 0 || q.size != 0 {
		t.Fatal("completed burst leaked queue slots")
	}
}

func annualRules(count int) []repository.Rule {
	rules := make([]repository.Rule, count)
	for i := range rules {
		rules[i] = repository.Rule{ID: int64(i + 1), CronExpr: "0 0 0 1 JAN *"}
	}
	return rules
}

func BenchmarkRuleApply500(b *testing.B) {
	rules := annualRules(500)
	r := unitRuntime(triggerServiceStub{})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		term := unitTerm(context.Background(), r, r.newCron())
		term.apply(rules)
		<-term.cron.Stop().Done()
		term.queue.close()
	}
}

func BenchmarkQueueBurst100(b *testing.B) {
	q := newJobQueue(100)
	defer q.close()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for id := int64(1); id <= 100; id++ {
			q.offer(id)
			q.offer(id)
		}
		for range 100 {
			id, _ := q.take()
			q.complete(id)
		}
	}
}
