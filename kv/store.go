package kv

import (
	"sync"

	"raft-kv/raft"
)

// IndexOutcome is what happened to a log entry a caller proposed and is
// waiting on, checked against whichever node is currently leader.
type IndexOutcome int

const (
	// StillPending: not yet decided one way or the other. Check again
	// later - commit progress only ever advances in reaction to a
	// message, so there's no point polling faster than that.
	StillPending IndexOutcome = iota
	// Committed: this exact entry (matching term) made it to CommitIndex.
	// Safe to look up its apply result and reply to whoever proposed it.
	Committed
	// Superseded: something else now occupies that log index. The entry
	// this caller proposed lost out and is never coming back; whoever's
	// waiting on it should be told to retry, not left hanging.
	Superseded
)

// ResolveIndex answers "what happened to the entry I appended at index,
// term" against node, which may or may not still be leader and may not be
// the same node that was leader when the entry was proposed.
//
// This exists because sim/workload.go and maelstrom/client.go both need to
// answer exactly this question and used to each carry their own copy of the
// logic. One of those copies had a real bug (see docs/bugs.md, "Client
// workload falsely 'abandoned' writes that later committed for real"):
// anchoring the check to the *original* proposing leader instead of
// whoever's leader *now*, and treating "the current leader's log hasn't
// reached this index yet" as proof of loss when Raft only actually
// overwrites a slot when something else is appended there. Both mistakes
// are avoided here: callers pass whichever node they currently consider
// leader (which may have changed since the entry was proposed), and only an
// actual conflicting entry at that index counts as Superseded.
func ResolveIndex(node *raft.Node, index, term int) IndexOutcome {
	if node == nil || node.LastLogIndex() < index {
		return StillPending
	}
	entry, err := node.GetLogEntry(index)
	if err != nil {
		return StillPending
	}
	if entry.Term != term {
		return Superseded
	}
	if node.CommitIndex < index {
		return StillPending
	}
	return Committed
}

// ResultTracker wraps a *StateMachine so a caller can look up what Apply
// returned for a specific log index once ResolveIndex reports Committed.
// raft.StateMachine.Apply(command) takes no index, but Node.applyCommitted
// calls it exactly once per index in strictly increasing order
// (raft/apply.go), so a local counter reconstructs the index->result
// mapping without touching raft/ itself.
type ResultTracker struct {
	store *StateMachine

	mu      sync.Mutex
	next    int
	results map[int]error
}

func NewResultTracker(store *StateMachine) *ResultTracker {
	return &ResultTracker{store: store, next: 1, results: make(map[int]error)}
}

// Apply implements raft.StateMachine.
func (r *ResultTracker) Apply(command []byte) error {
	err := r.store.Apply(command)

	r.mu.Lock()
	r.results[r.next] = err
	r.next++
	r.mu.Unlock()

	return err
}

// ResultAt returns the error Apply produced when index was applied, and
// whether that index has been applied yet at all.
func (r *ResultTracker) ResultAt(index int) (error, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	err, ok := r.results[index]
	return err, ok
}