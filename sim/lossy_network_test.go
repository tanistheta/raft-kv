package sim

import (
	"testing"
	"time"

	"raft-kv/raft"
)

func TestElectionSucceedsUnderLossyNetwork(t *testing.T) {
	scheduler := NewScheduler()
	injector := NewFaultInjector(42)
	injector.dropRate = 0.3
	network := NewInMemoryNetwork(scheduler, injector)

	ids := []raft.NodeID{"A", "B", "C"}
	nodes := setupNodes(scheduler, network, ids)

	nodes["A"].StartElection()
	scheduler.RunFor(10 * time.Second)

	leadersByTerm := map[int][]raft.NodeID{}
	anyLeader := false
	for id, n := range nodes {
		if n.Role == raft.Leader {
			anyLeader = true
			leadersByTerm[n.CurrentTerm] = append(leadersByTerm[n.CurrentTerm], id)
		}
	}
	if !anyLeader {
		t.Fatal("no node ever became leader despite 10s of virtual time under 30% loss")
	}
	for term, leaders := range leadersByTerm {
		if len(leaders) > 1 {
			t.Fatalf("safety violation: term %d has %d leaders: %v", term, len(leaders), leaders)
		}
	}
}