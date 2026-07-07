package sim

import (
	"container/heap"
	"time"

	"raft-kv/raft"
)

type event struct {
	at  int64
	seq int64
	fn  func()
}

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
	now   int64
	queue eventHeap
	seq   int64
}

func NewScheduler() *Scheduler {
	s := &Scheduler{
		queue: make(eventHeap, 0),
	}
	heap.Init(&s.queue)
	return s
}

func (s *Scheduler) Now() time.Time {
	return time.Unix(0, s.now)
}

func (s *Scheduler) Schedule(after time.Duration, fn func()) {
	s.seq++
	heap.Push(&s.queue, &event{
		at:  s.now + int64(after),
		seq: s.seq,
		fn:  fn,
	})
}

func (s *Scheduler) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	s.Schedule(d, func() {
		ch <- s.Now()
	})
	return ch
}

func (s *Scheduler) AfterFunc(d time.Duration, fn func()) {
	s.Schedule(d, fn)
}

var _ raft.Clock = (*Scheduler)(nil)

func (s *Scheduler) Run() {
	for s.queue.Len() > 0 {
		e := heap.Pop(&s.queue).(*event)
		s.now = e.at
		e.fn()
	}
}

func (s *Scheduler) RunFor(d time.Duration) {
	deadline := s.now + int64(d)
	for s.queue.Len() > 0 && s.queue[0].at <= deadline {
		e := heap.Pop(&s.queue).(*event)
		s.now = e.at
		e.fn()
	}
	if s.now < deadline {
		s.now = deadline
	}
}