package sim

import (
	"container/heap"
	"time"
	"raft-kv/raft"
)

// event is a single scheduled callback, ordered by virtual time
type event struct {
	at int64 // virtual time in nanoseconds
	seq int64 // sequence number to break ties
	fn func()
}

// eventHeap is a min-heap of events, ordered by virtual time and sequence number
type eventHeap []*event

func (q eventHeap) Len() int { return len(q) }
func (q eventHeap) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}

func (q eventHeap) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *eventHeap) Push(x interface{}) {
	*q = append(*q, x.(*event))
}

func (q *eventHeap) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	*q = old[0 : n-1]
	return item
}

type Scheduler struct {
	now int64 //current virtual time
	queue eventHeap
	seq int64 //sequence number for tiebreaking
}

// NewScheduler creates a Scheduler starting at virtual time 0
func NewScheduler() * Scheduler{
	s := &Scheduler{
		queue: make(eventHeap, 0),
	}
	heap.Init(&s.queue)
	return s
}

//Now return the current virtual time as time.Time (epoch+virtual ns)
func (s *Scheduler) Now() time.Time {
	return time.Unix(0, s.now)
}

// Scedule queues fn to run after the 'after' virtual duration fas elapsed
func (s *Scheduler) Schedule(after time.Duration, fn func()) {
	s.seq++
    heap.Push(&s.queue, &event{
		at: s.now + int64(after),
		seq: s.seq,
		fn: fn,
	})
}

func (s *Scheduler) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	s.Schedule(d, func() {
		ch <- s.Now()
	})
	return ch
}

var _ raft.Clock = (*Scheduler)(nil)

// Run drains the event queue: pop earliest event,jump virtual clock
//to its time, execute it. Stops when the queue is empty
func (s *Scheduler) Run() {
	for s.queue.Len() > 0 {
		e := heap.Pop(&s.queue).(*event)
		s.now = e.at //jump
		e.fn()
	}
}
