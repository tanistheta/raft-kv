package prod

import (
	"sync"
	"time"
)

// RealClock implements raft.Clock over real wall-clock time (the
// counterpart to sim's virtual Clock). It takes Mu before running any
// scheduled callback, since raft.Node has no locking of its own and
// assumes a single-threaded driver. Callers must guard every other path
// into the same Node with the same mutex.
type RealClock struct {
	Mu *sync.Mutex
}

// NewRealClock returns a RealClock guarded by mu.
func NewRealClock(mu *sync.Mutex) *RealClock {
	return &RealClock{Mu: mu}
}

func (c *RealClock) Now() time.Time {
	return time.Now()
}

func (c *RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// AfterFunc runs fn after d, holding Mu for the duration.
func (c *RealClock) AfterFunc(d time.Duration, fn func()) {
	time.AfterFunc(d, func() {
		c.Mu.Lock()
		defer c.Mu.Unlock()
		fn()
	})
}