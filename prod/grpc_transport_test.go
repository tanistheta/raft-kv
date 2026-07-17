package prod

import (
	"sync"
	"testing"
	"time"

	"raft-kv/kv"
	"raft-kv/raft"
	"raft-kv/sim"
)

type grpcClusterNode struct {
	id        raft.NodeID
	mu        *sync.Mutex
	raftNode  *raft.Node
	transport *GRPCTransport
	store     *kv.StateMachine
}

// TestClusterElectsLeaderAndReplicatesOverRealGRPC is the gRPC-transport
// equivalent of maelstrom/integration_test.go's 3-node test: real leader
// election and real log replication, but over actual TCP sockets and real
// gRPC servers on localhost rather than an in-memory pipe or a
// stdin/stdout process boundary. This is the actual proof this transport
// works, not just that it compiles against raft.Network's interface.
func TestClusterElectsLeaderAndReplicatesOverRealGRPC(t *testing.T) {
	ids := []raft.NodeID{"n1", "n2", "n3"}
	addrs := map[raft.NodeID]string{
		"n1": "127.0.0.1:17001",
		"n2": "127.0.0.1:17002",
		"n3": "127.0.0.1:17003",
	}

	nodes := make(map[raft.NodeID]*grpcClusterNode)
	for _, id := range ids {
		peerAddrs := make(map[raft.NodeID]string)
		var peers []raft.NodeID
		for _, other := range ids {
			if other != id {
				peerAddrs[other] = addrs[other]
				peers = append(peers, other)
			}
		}

		mu := &sync.Mutex{}
		transport, err := NewGRPCTransport(id, addrs[id], peerAddrs, mu)
		if err != nil {
			t.Fatalf("NewGRPCTransport(%s): %v", id, err)
		}
		t.Cleanup(func() { transport.Close() })

		store := kv.NewStateMachine()
		raftNode := &raft.Node{
			NodeID:       id,
			Peers:        peers,
			Clock:        NewRealClock(mu),
			Network:      transport,
			Storage:      sim.NewMemStorage(),
			StateMachine: store,
			RNG:          RealRNG{},
		}
		nodes[id] = &grpcClusterNode{id: id, mu: mu, raftNode: raftNode, transport: transport, store: store}
	}

	for _, n := range nodes {
		n.mu.Lock()
		n.raftNode.Start()
		n.mu.Unlock()
	}

	leader := awaitGRPCLeader(t, nodes, 5*time.Second)

	leader.mu.Lock()
	leader.raftNode.AppendLogEntry(raft.LogEntry{
		Term:    leader.raftNode.CurrentTerm,
		Command: []byte("SET x=1"),
	})
	leader.mu.Unlock()

	deadline := time.Now().Add(3 * time.Second)
	for {
		allMatch := true
		for _, n := range nodes {
			n.mu.Lock()
			val, ok := n.store.Get("x")
			n.mu.Unlock()
			if !ok || val != "1" {
				allMatch = false
			}
		}
		if allMatch {
			return
		}
		if time.Now().After(deadline) {
			for _, n := range nodes {
				n.mu.Lock()
				val, ok := n.store.Get("x")
				n.mu.Unlock()
				t.Errorf("node %s store[x] = %q, %v", n.id, val, ok)
			}
			t.Fatal("replicas never converged on x=1 over real gRPC")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func awaitGRPCLeader(t *testing.T, nodes map[raft.NodeID]*grpcClusterNode, timeout time.Duration) *grpcClusterNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			n.mu.Lock()
			role := n.raftNode.Role
			n.mu.Unlock()
			if role == raft.Leader {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout over real gRPC")
	return nil
}