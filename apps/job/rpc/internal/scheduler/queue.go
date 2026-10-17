package scheduler

import "sync"

type pendingJob struct {
	processing bool
	dirty      bool
}

// jobQueue is a concurrency-safe bounded queue that coalesces trigger hints by
// Job ID. Construct it with newJobQueue. Capacity includes both queued and
// in-flight distinct IDs; workers retain their slots so duplicates cannot bypass
// the limit. The queue is not durable: database progress recovers rejected hints
// and hints discarded on shutdown.
type jobQueue struct {
	mu     sync.Mutex
	ready  *sync.Cond
	jobs   map[int64]*pendingJob
	buffer []int64
	head   int
	size   int
	closed bool
}

// newJobQueue creates a fixed-capacity queue. The caller must validate that
// capacity is positive.
func newJobQueue(capacity int) *jobQueue {
	q := &jobQueue{jobs: make(map[int64]*pendingJob), buffer: make([]int64, capacity)}
	q.ready = sync.NewCond(&q.mu)
	return q
}

// offer returns true when a new ID is queued or an existing ID is coalesced,
// without waiting for capacity. A full queue rejects new IDs but still accepts
// duplicates; a closed queue rejects all hints. Hints received during processing
// mark the ID dirty for requeue on complete. True does not acknowledge processing.
func (q *jobQueue) offer(id int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	if existing, ok := q.jobs[id]; ok {
		if existing.processing {
			existing.dirty = true
		}
		return true
	}
	if len(q.jobs) == len(q.buffer) {
		return false
	}
	q.jobs[id] = &pendingJob{}
	q.push(id)
	return true
}

func (q *jobQueue) push(id int64) {
	q.buffer[(q.head+q.size)%len(q.buffer)] = id
	q.size++
	q.ready.Signal()
}

// take waits for a hint, or returns false once closed without draining the queue.
// Each accepted ID belongs to only one worker at a time and requires exactly one
// complete call after normal processing. On term cancellation, close may instead
// discard all unfinished hints.
func (q *jobQueue) take() (int64, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.size == 0 && !q.closed {
		q.ready.Wait()
	}
	if q.closed {
		return 0, false
	}
	id := q.buffer[q.head]
	q.head = (q.head + 1) % len(q.buffer)
	q.size--
	q.jobs[id].processing = true
	return id, true
}

// complete releases an ID returned by take, or reuses its slot to queue a hint
// received during processing. It must not be called for an untaken or completed
// ID. After closure, completion from an in-flight worker is ignored.
func (q *jobQueue) complete(id int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	job := q.jobs[id]
	if job.dirty {
		job.processing, job.dirty = false, false
		q.push(id)
	} else {
		delete(q.jobs, id)
	}
}

// close is idempotent: it discards pending hints and wakes all takers. It does not
// cancel or wait for in-flight workers. The term owner must separately cancel
// their context and wait for them to exit.
func (q *jobQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.jobs = nil
	q.size = 0
	q.ready.Broadcast()
}
