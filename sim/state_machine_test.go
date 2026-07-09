package sim

import (
	"testing"
	"time"

	"raft-kv/kv"
	"raft-kv/raft"
)

func TestStateMachineConvergesAfterPartitionHeal(t *testing.T) {
	scheduler := NewScheduler()
	injector := NewFaultInjector(42)
	network := NewInMemoryNetwork(scheduler, injector)

	ids := []raft.NodeID{"A", "B", "C"}
	nodes := setupNodes(scheduler, network, ids)

	sms := make(map[raft.NodeID]*kv.StateMachine)
	for id, node := range nodes {
		sm := kv.NewStateMachine()
		sms[id] = sm
		node.StateMachine = sm
	}

	nodes["A"].StartElection()
	scheduler.RunFor(500 * time.Millisecond)

	var leaderID raft.NodeID
	for id, node := range nodes {
		if node.Role == raft.Leader {
			leaderID = id
		}
	}
	if leaderID == "" {
		t.Fatal("no leader elected before partition")
	}
	oldLeader := nodes[leaderID]

	var majoritySide []raft.NodeID
	for _, id := range ids {
		if id != leaderID {
			majoritySide = append(majoritySide, id)
		}
	}

	injector.Partition([]raft.NodeID{leaderID}, majoritySide)

	oldLeader.AppendLogEntry(raft.LogEntry{
		Term:    oldLeader.CurrentTerm,
		Command: []byte("SET x=stale"),
	})
	scheduler.RunFor(500 * time.Millisecond)

	var newLeaderID raft.NodeID
	for _, id := range majoritySide {
		if nodes[id].Role == raft.Leader {
			newLeaderID = id
		}
	}
	if newLeaderID == "" {
		t.Fatal("majority side failed to elect a new leader")
	}
	newLeader := nodes[newLeaderID]

	newLeader.AppendLogEntry(raft.LogEntry{
		Term:    newLeader.CurrentTerm,
		Command: []byte("SET x=committed"),
	})
	scheduler.RunFor(500 * time.Millisecond)

	if newLeader.CommitIndex != 1 {
		t.Fatalf("majority-side leader failed to commit, CommitIndex=%d", newLeader.CommitIndex)
	}

	injector.HealPartition()
	scheduler.RunFor(1 * time.Second)

	for id, sm := range sms {
		val, ok := sm.Get("x")
		if !ok {
			t.Errorf("node %v: key x was never applied to the state machine", id)
			continue
		}
		if val != "committed" {
			t.Errorf("node %v: expected 'committed', got %q — stale/uncommitted write leaked into state", id, val)
		}
	}
}
