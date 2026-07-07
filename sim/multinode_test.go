package sim
import (
	"testing"
	"time"
	"raft-kv/raft"
)

func TestThreeNodeElection(t *testing.T) {
	network := NewInMemoryNetwork()

	ids := []raft.NodeID{"A", "B", "C"}
	nodes := make(map[raft.NodeID]*raft.Node)

	for _, id := range ids {
		peers := []raft.NodeID{}
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		node := &raft.Node{
			NodeID: id,
			Peers:  peers,
			Clock:  RealClock{},
			Role:   raft.Follower,
			Network: network,
			Storage: StubStorage{},
			RNG: StubRNG{},
			Inbox: network.Register(id),
		}
		nodes[id] = node
		go node.Run()
	}
	
	nodes["A"].StartElection()
	time.Sleep(500 * time.Millisecond)
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
	network := NewInMemoryNetwork()

	ids := []raft.NodeID{"A", "B", "C"}
	nodes := make(map[raft.NodeID]*raft.Node)

	for _, id := range ids {
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
			Clock:   RealClock{},
			Network: network,
			Storage: StubStorage{},
			RNG:     StubRNG{},
			Inbox:   network.Register(id),
		}
		nodes[id] = node
		go node.Run()
	}

	nodes["A"].StartElection()
	time.Sleep(500 * time.Millisecond)

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

	time.Sleep(500 * time.Millisecond)

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