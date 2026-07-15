package maelstrom

import (
	"sync"

	"raft-kv/kv"
)

// resultTracker wraps a *kv.StateMachine so it can report what Apply
// returned for a specific log index. raft.StateMachine.Apply(command) takes
// no index, but Node.applyCommitted calls it exactly once per index in
// strictly increasing order (raft/apply.go), so a local counter
// reconstructs the index->result mapping without touching raft/ at all.
type resultTracker struct {
	store *kv.StateMachine

	mu      sync.Mutex
	next    int
	results map[int]error
}

func newResultTracker(store *kv.StateMachine) *resultTracker {
	return &resultTracker{store: store, next: 1, results: make(map[int]error)}
}

// Apply implements raft.StateMachine.
func (r *resultTracker) Apply(command []byte) error {
	err := r.store.Apply(command)

	r.mu.Lock()
	r.results[r.next] = err
	r.next++
	r.mu.Unlock()

	return err
}

// resultAt returns the error Apply produced when index was applied, and
// whether that index has been applied yet at all.
func (r *resultTracker) resultAt(index int) (error, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	err, ok := r.results[index]
	return err, ok
}