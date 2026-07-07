package sim

import (
	"testing"
	"time"

	"raft-kv/raft"
)

func setupNodes(scheduler *Scheduler, network *InMemoryNetwork, ids []raft.NodeID) map[raft.NodeID]*raft.Node {
	nodes := make(map[raft.NodeID]*raft.Node)
	for i, id := range ids {
		peers := []raft.NodeID{}
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		node := &raft.Node{
			NodeID:  id,
			Peers:   peers,
			Role:    raft.Follower,
			Clock:   scheduler,
			Network: network,
			Storage: StubStorage{},
			RNG:     NewSeededRNG(int64(1000 + i)),
		}
		node.Start()
		nodes[id] = node
	}
	return nodes
}

func TestThreeNodeElection(t *testing.T) {
	scheduler := NewScheduler()
	injector := NewFaultInjector(42)
	network := NewInMemoryNetwork(scheduler, injector)

	ids := []raft.NodeID{"A", "B", "C"}
	nodes := setupNodes(scheduler, network, ids)

	nodes["A"].StartElection()
	scheduler.RunFor(500 * time.Millisecond)

	leaderCount := 0
	for _, node := range nodes {
		if node.Role == raft.Leader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Errorf("Expected 1 leader, but found %d", leaderCount)
	}
}

func TestLeaderDisconnectAndReelect(t *testing.T) {
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
		t.Fatalf("expected 1 leader before disconnect, found %d", leaderCount)
	}
	t.Logf("original leader: %v", leaderID)

	network.Unregister(leaderID)
	scheduler.RunFor(500 * time.Millisecond)

	newLeaderCount := 0
	var newLeaderID raft.NodeID
	for id, node := range nodes {
		if id == leaderID {
			continue
		}
		if node.Role == raft.Leader {
			newLeaderCount++
			newLeaderID = id
		}
	}

	if newLeaderCount == 0 {
		t.Fatal("no new leader elected after original leader disconnected")
	}
	if newLeaderCount > 1 {
		t.Fatalf("split-brain: %d leaders among survivors", newLeaderCount)
	}
	if newLeaderID == leaderID {
		t.Fatalf("disconnected node is still recorded as leader")
	}
	t.Logf("new leader after disconnect: %v", newLeaderID)
}

func TestPartitionMajorityReelects(t *testing.T) {
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
			break
		}
	}

	// Partition the network: isolate the leader from the other two nodes
	var otherTwo []raft.NodeID
	for _, id := range ids {
		if id != leaderID {
			otherTwo = append(otherTwo, id)
		}
	}
	injector.Partition([]raft.NodeID{leaderID}, otherTwo)
	scheduler.RunFor(500 * time.Millisecond)

	//assert that a new leader is elected among the remaining nodes
	newLeaderCount := 0
	var newLeaderID raft.NodeID

	for id, node := range nodes {
		if id == leaderID {
			continue
		}	
	if node.Role == raft.Leader {
			newLeaderCount++
			newLeaderID = id
		}	
	}
	if newLeaderCount == 0 {
		t.Fatal("no new leader elected after partition")
	}
	if newLeaderCount > 1 {
		t.Fatalf("split-brain: %d leaders among majority side", newLeaderCount)
	}
	if newLeaderID == leaderID {
		t.Fatalf("partitioned leader still counted as leader")
	}
	t.Logf("old leader: %v, new leader after partition: %v", leaderID, newLeaderID)
}