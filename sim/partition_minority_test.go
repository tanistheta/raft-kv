package sim

import (
	"testing"
	"time"

	"raft-kv/raft"
)

func TestPartitionMinorityCannotElect(t *testing.T) {
	scheduler := NewScheduler()
	injector := NewFaultInjector(42)
	network := NewInMemoryNetwork(scheduler, injector)

	ids := []raft.NodeID{"A", "B", "C", "D", "E"}
	nodes := setupNodes(scheduler, network, ids)

	nodes["A"].StartElection()
	scheduler.RunFor(500 * time.Millisecond)

	var leaderID raft.NodeID
	for id, node := range nodes {
		if node.Role == raft.Leader {
			leaderID = id
			break
		}
	}

	var secondMinorityID raft.NodeID
	for _, id := range ids {
		if id != leaderID {
			secondMinorityID = id
			break
		}
	}
	minority := []raft.NodeID{leaderID, secondMinorityID}
	var majority []raft.NodeID
	for _, id := range ids {
		if id != leaderID && id != secondMinorityID {
			majority = append(majority, id)
		}
	}

	injector.Partition(majority, minority)
	scheduler.RunFor(500 * time.Millisecond)

	majorityLeaderCount := 0
	for _, id := range majority {
		if nodes[id].Role == raft.Leader {
			majorityLeaderCount++
		}
	}
	if majorityLeaderCount != 1 {
		t.Fatalf("expected 1 leader in majority partition, found %d", majorityLeaderCount)
	}
	oldLeaderTerm := nodes[leaderID].CurrentTerm
	var newMajorityLeaderTerm int
	for _, id := range majority {
		if nodes[id].Role == raft.Leader {
			newMajorityLeaderTerm = nodes[id].CurrentTerm
		}
	}
	if newMajorityLeaderTerm <= oldLeaderTerm {
		t.Fatalf("new majority leader term (%d) should be strictly greater than old leader term (%d)", newMajorityLeaderTerm, oldLeaderTerm)
	}
}