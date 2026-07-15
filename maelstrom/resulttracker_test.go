package maelstrom

import (
	"errors"
	"testing"

	"raft-kv/kv"
)

func TestResultTrackerMapsSequentialApplyCallsToIndices(t *testing.T) {
	rt := newResultTracker(kv.NewStateMachine())

	if err := rt.Apply([]byte("SET x=1")); err != nil {
		t.Fatalf("Apply #1 returned error: %v", err)
	}
	if err := rt.Apply([]byte("CAS x 1 2")); err != nil {
		t.Fatalf("Apply #2 returned error: %v", err)
	}
	if err := rt.Apply([]byte("CAS x 99 3")); !errors.Is(err, kv.ErrCASMismatch) {
		t.Fatalf("Apply #3 error = %v, want ErrCASMismatch", err)
	}

	if err, ok := rt.resultAt(1); !ok || err != nil {
		t.Errorf("resultAt(1) = %v, %v, want nil, true", err, ok)
	}
	if err, ok := rt.resultAt(2); !ok || err != nil {
		t.Errorf("resultAt(2) = %v, %v, want nil, true", err, ok)
	}
	if err, ok := rt.resultAt(3); !ok || !errors.Is(err, kv.ErrCASMismatch) {
		t.Errorf("resultAt(3) = %v, %v, want ErrCASMismatch, true", err, ok)
	}
}

func TestResultTrackerNotYetAppliedIndex(t *testing.T) {
	rt := newResultTracker(kv.NewStateMachine())
	rt.Apply([]byte("SET x=1"))

	if _, ok := rt.resultAt(5); ok {
		t.Errorf("resultAt(5) reported ok=true for an index never applied")
	}
}