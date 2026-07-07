package sim
import (
	"testing"
	"time"
	"raft-kv/raft"
)

func TestThreeNodeElection(t *testing.T) {
	scheduler := NewScheduler()
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
			Clock:  scheduler,
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