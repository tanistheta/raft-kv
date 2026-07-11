package checker

import (
	"fmt"

	"raft-kv/raft"
)

type Kind int

const (
	Write Kind = iota
	Read
)

type Op struct {
	Kind     Kind
	Key      string
	Value    string
	Start    int64
	End      int64
	ServedBy raft.NodeID
}

func CheckLinearizable(history []Op) (bool, string) {
	byKey := make(map[string][]Op)
	for _, op := range history {
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	for key, ops := range byKey {
		if ok, _ := linearize("", ops); !ok {
			return false, fmt.Sprintf("key %q: no valid linearization for %d ops", key, len(ops))
		}
	}
	return true, ""
}

func linearize(state string, remaining []Op) (bool, []Op) {
	if len(remaining) == 0 {
		return true, nil
	}
	for i := range remaining {
		if !isMinimal(i, remaining) {
			continue
		}
		op := remaining[i]
		newState := state
		if op.Kind == Write {
			newState = op.Value
		} else if op.Value != state {
			continue
		}

		rest := make([]Op, 0, len(remaining)-1)
		rest = append(rest, remaining[:i]...)
		rest = append(rest, remaining[i+1:]...)

		if ok, path := linearize(newState, rest); ok {
			return true, append([]Op{op}, path...)
		}
	}
	return false, nil
}

func isMinimal(i int, remaining []Op) bool {
	for j, other := range remaining {
		if j == i {
			continue
		}
		if other.End <= remaining[i].Start {
			return false
		}
	}
	return true
}