package kv

import (
	"errors"
	"testing"

	"raft-kv/raft"
)

func TestResolveIndexStillPendingWhenLogHasNotReachedIndex(t *testing.T) {
	node := &raft.Node{}
	if got := ResolveIndex(node, 1, 1); got != StillPending {
		t.Fatalf("ResolveIndex on empty log = %v, want StillPending", got)
	}
}

func TestResolveIndexStillPendingWhenNotYetCommitted(t *testing.T) {
	node := &raft.Node{}
	node.AppendLogEntry(raft.LogEntry{Term: 1, Command: []byte("SET x=1")})
	// CommitIndex defaults to 0, so index 1 isn't committed yet.
	if got := ResolveIndex(node, 1, 1); got != StillPending {
		t.Fatalf("ResolveIndex before commit = %v, want StillPending", got)
	}
}

func TestResolveIndexCommittedWhenTermMatchesAndCommitted(t *testing.T) {
	node := &raft.Node{}
	node.AppendLogEntry(raft.LogEntry{Term: 3, Command: []byte("SET x=1")})
	node.CommitIndex = 1
	if got := ResolveIndex(node, 1, 3); got != Committed {
		t.Fatalf("ResolveIndex = %v, want Committed", got)
	}
}

// TestResolveIndexSupersededOnConflictingTerm covers the scenario
// docs/bugs.md's "falsely abandoned writes" bug got wrong: a different
// leader's entry now occupies this index. That's the one case that should
// report Superseded rather than StillPending, regardless of CommitIndex.
func TestResolveIndexSupersededOnConflictingTerm(t *testing.T) {
	node := &raft.Node{}
	node.AppendLogEntry(raft.LogEntry{Term: 5, Command: []byte("SET x=2")})
	node.CommitIndex = 1
	// Proposed under term 3, but the entry actually sitting at index 1 is
	// term 5 - a different leader's entry won out.
	if got := ResolveIndex(node, 1, 3); got != Superseded {
		t.Fatalf("ResolveIndex on term mismatch = %v, want Superseded", got)
	}
}

// TestResolveIndexStillPendingAcrossLeaderChange is the specific regression
// covered in docs/bugs.md: a later leader whose log simply hasn't caught up
// to this index yet must not be treated as having lost the entry. Only an
// actual conflicting entry (covered above) counts as Superseded.
func TestResolveIndexStillPendingAcrossLeaderChange(t *testing.T) {
	shortLog := &raft.Node{} // stands in for a newer leader still catching up
	if got := ResolveIndex(shortLog, 5, 2); got != StillPending {
		t.Fatalf("ResolveIndex on a shorter log = %v, want StillPending, not Superseded", got)
	}
}

func TestResultTrackerMapsSequentialApplyCallsToIndices(t *testing.T) {
	rt := NewResultTracker(NewStateMachine())

	if err := rt.Apply([]byte("SET x=1")); err != nil {
		t.Fatalf("Apply #1 returned error: %v", err)
	}
	if err := rt.Apply([]byte("CAS x 1 2")); err != nil {
		t.Fatalf("Apply #2 returned error: %v", err)
	}
	if err := rt.Apply([]byte("CAS x 99 3")); !errors.Is(err, ErrCASMismatch) {
		t.Fatalf("Apply #3 error = %v, want ErrCASMismatch", err)
	}

	if err, ok := rt.ResultAt(1); !ok || err != nil {
		t.Errorf("ResultAt(1) = %v, %v, want nil, true", err, ok)
	}
	if err, ok := rt.ResultAt(2); !ok || err != nil {
		t.Errorf("ResultAt(2) = %v, %v, want nil, true", err, ok)
	}
	if err, ok := rt.ResultAt(3); !ok || !errors.Is(err, ErrCASMismatch) {
		t.Errorf("ResultAt(3) = %v, %v, want ErrCASMismatch, true", err, ok)
	}
}

func TestResultTrackerNotYetAppliedIndex(t *testing.T) {
	rt := NewResultTracker(NewStateMachine())
	rt.Apply([]byte("SET x=1"))

	if _, ok := rt.ResultAt(5); ok {
		t.Errorf("ResultAt(5) reported ok=true for an index never applied")
	}
}