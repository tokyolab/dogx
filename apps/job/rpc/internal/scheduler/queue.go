package scheduler

import "sync"

type pendingJob struct {
	processing bool
	dirty      bool
}

// A slot remains owned while a worker runs. This bounds queued AND in-flight
// distinct IDs, and lets a new hint reuse the same slot without losing a wakeup.
type jobQueue struct {
	mu     sync.Mutex
	ready  *sync.Cond
	jobs   map[int64]*pendingJob
	buffer []int64
	head   int
	size   int
	closed bool
}

func newJobQueue(capacity int) *jobQueue {
	q := &jobQueue{jobs: make(map[int64]*pendingJob), buffer: make([]int64, capacity)}
	q.ready = sync.NewCond(&q.mu)
	return q
}

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

func (q *jobQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.jobs = nil
	q.size = 0
	q.ready.Broadcast()
}
