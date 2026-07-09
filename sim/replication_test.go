package sim

import (
	"testing"
	"time"

	"raft-kv/raft"
)

func TestLogReplication(t *testing.T) {
	scheduler := NewScheduler()
	injector := NewFaultInjector(42)
	network := NewInMemoryNetwork(scheduler, injector)

	ids := []raft.NodeID{"A", "B", "C"}
	nodes := setupNodes(scheduler, network, ids)

	nodes["A"].StartElection()
	scheduler.RunFor(500 * time.Millisecond)

	var leaderID raft.NodeID
	leaderCount := 0
	for id, node := range nodes {
		if node.Role == raft.Leader {
			leaderCount++
			leaderID = id
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader before replication, found %d", leaderCount)
	}
	leader := nodes[leaderID]

	leader.AppendLogEntry(raft.LogEntry{
		Term:    leader.CurrentTerm,
		Command: []byte("SET x=1"),
	})

	scheduler.RunFor(500 * time.Millisecond)

	for id, node := range nodes {
		if node.LastLogIndex() != 1 {
			t.Errorf("node %v: expected LastLogIndex 1, got %d", id, node.LastLogIndex())
		}
		entry, err := node.GetLogEntry(1)
		if err != nil {
			t.Fatalf("node %v: entry at index 1 missing: %v", id, err)
		}
		if entry.Term != leader.CurrentTerm {
			t.Errorf("node %v: entry term mismatch, expected %d got %d", id, leader.CurrentTerm, entry.Term)
		}
		if string(entry.Command) != "SET x=1" {
			t.Errorf("node %v: command mismatch, got %q", id, entry.Command)
		}
	}

	if leader.CommitIndex != 1 {
		t.Errorf("leader CommitIndex: expected 1, got %d", leader.CommitIndex)
	}

	for id, node := range nodes {
		if id == leaderID {
			continue
		}
		if node.CommitIndex != 1 {
			t.Errorf("follower %v CommitIndex: expected 1, got %d", id, node.CommitIndex)
		}
	}
}
