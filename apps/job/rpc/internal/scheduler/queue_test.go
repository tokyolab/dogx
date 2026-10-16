package scheduler

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestQueueCapacityIncludesProcessingAndDirtyRotates(t *testing.T) {
	q := newJobQueue(2)
	if !q.offer(1) || !q.offer(2) || !q.offer(1) || q.offer(3) {
		t.Fatal("queue admission or deduplication failed")
	}
	id, ok := q.take()
	if !ok || id != 1 {
		t.Fatalf("take=%d,%v", id, ok)
	}
	if q.offer(3) {
		t.Fatal("processing Job must still consume capacity")
	}
	q.offer(1)
	q.offer(1)
	q.complete(1)
	id, _ = q.take()
	if id != 2 {
		t.Fatal("dirty Job must rejoin the tail")
	}
	q.complete(2)
	if !q.offer(3) {
		t.Fatal("completed clean Job did not release capacity")
	}
	id, _ = q.take()
	if id != 1 {
		t.Fatal("processing hint was lost")
	}
	q.complete(1)
	id, _ = q.take()
	if id != 3 {
		t.Fatal("queue order changed")
	}
	q.complete(3)
	q.close()
	if _, ok := q.take(); ok || q.offer(1) {
		t.Fatal("closed queue accepted or returned work")
	}
}

func TestQueueCloseDropsPendingAndWakesConsumers(t *testing.T) {
	q := newJobQueue(2)
	q.offer(1)
	q.offer(2)
	id, _ := q.take()
	q.close()
	q.complete(id)
	if _, ok := q.take(); ok {
		t.Fatal("closed queue drained old work")
	}
	empty := newJobQueue(1)
	done := make(chan struct{})
	go func() { empty.take(); close(done) }()
	empty.close()
	<-done
}

func TestQueueConcurrentHintsNeverProcessSameJobTwice(t *testing.T) {
	q := newJobQueue(16)
	var active [16]atomic.Int32
	var overlap atomic.Bool
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for {
				id, ok := q.take()
				if !ok {
					return
				}
				if active[id].Add(1) != 1 {
					overlap.Store(true)
				}
				active[id].Add(-1)
				q.complete(id)
			}
		})
	}
	var producers sync.WaitGroup
	for range 8 {
		producers.Go(func() {
			for i := range 2000 {
				q.offer(int64(i % 16))
			}
		})
	}
	producers.Wait()
	q.close()
	workers.Wait()
	if overlap.Load() {
		t.Fatal("same Job ran concurrently")
	}
}
