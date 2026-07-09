package sim

import(
	"testing"
	"time"
	"raft-kv/raft"
)
func TestDivergentEntryOverwrittenAfterPartitionHeal(t *testing.T) {
	scheduler := NewScheduler()
	injector := NewFaultInjector(42)
	network := NewInMemoryNetwork(scheduler, injector)

	ids := []raft.NodeID{"A", "B", "C"}
	nodes := setupNodes(scheduler, network, ids)

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
	t.Logf("original leader: %v", leaderID)

	var majoritySide []raft.NodeID
	for _, id := range ids {
		if id != leaderID {
			majoritySide = append(majoritySide, id)
		}
	}

	injector.Partition([]raft.NodeID{leaderID}, majoritySide)

	oldLeader.AppendLogEntry(raft.LogEntry{
		Term:    oldLeader.CurrentTerm,
		Command: []byte("stale-write-from-deposed-leader"),
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
	t.Logf("new leader on majority side: %v (term %d)", newLeaderID, newLeader.CurrentTerm)

	if newLeader.CurrentTerm <= oldLeader.CurrentTerm {
		t.Fatalf("new leader term %d not higher than old leader term %d",
			newLeader.CurrentTerm, oldLeader.CurrentTerm)
	}

	newLeader.AppendLogEntry(raft.LogEntry{
		Term:    newLeader.CurrentTerm,
		Command: []byte("committed-write-from-majority-leader"),
	})
	scheduler.RunFor(500 * time.Millisecond)

	if newLeader.CommitIndex != 1 {
		t.Fatalf("majority-side leader failed to commit its entry, CommitIndex=%d", newLeader.CommitIndex)
	}

	injector.HealPartition()
	scheduler.RunFor(1 * time.Second)

	if oldLeader.Role == raft.Leader {
		t.Fatal("old leader never stepped down after partition healed")
	}

	for id, node := range nodes {
		entry, err := node.GetLogEntry(1)
		if err != nil {
			t.Fatalf("node %v: missing entry at index 1: %v", id, err)
		}
		if string(entry.Command) != "committed-write-from-majority-leader" {
			t.Errorf("node %v: expected converged log to hold majority-committed entry, got %q",
				id, entry.Command)
		}
		if entry.Term != newLeader.CurrentTerm {
			t.Errorf("node %v: expected entry term %d, got %d", id, newLeader.CurrentTerm, entry.Term)
		}
	}

	for id, node := range nodes {
		if node.CommitIndex != 1 {
			t.Errorf("node %v: expected CommitIndex 1 after convergence, got %d", id, node.CommitIndex)
		}
	}
}