package maelstrom

import (
	"sync"

	"raft-kv/kv"
	"raft-kv/prod"
	"raft-kv/raft"
	"raft-kv/sim"
)

// RunProcess turns the current OS process into one raft-kv cluster member
// speaking Maelstrom's protocol over stdin/stdout. It blocks until stdin
// closes or a fatal protocol error occurs (mirroring Node.Run), and is the
// entire body of cmd/maelstrom/main.go by design - all the real wiring
// lives here, in the library package, so it can be reasoned about (and
// eventually tested) alongside the rest of maelstrom/, not hidden in an
// untestable main().
//
// The stack it builds is exactly the one integration_test.go proves works
// (raft.Node + NetworkAdapter + ClientHandler + Router, one per process),
// with two differences forced by running as a real process instead of a
// test: NodeID/Peers come from Maelstrom's init message instead of a
// hardcoded id list (see Node.OnInit in protocol.go), and Storage is
// sim.MemStorage because no durable storage has been built yet - a crashed
// real process currently loses its log, same limitation the DST harness's
// crash/restart faults don't yet model for this transport. That gap is
// tracked, not hidden: Phase 3 external validation with `maelstrom test`
// (no nemesis) doesn't crash nodes, so it doesn't exercise it either way.
func RunProcess() error {
	transport := NewNode()
	var mu sync.Mutex

	transport.OnInit = func(nodeID string, nodeIDs []string) error {
		mu.Lock()
		defer mu.Unlock()

		var peers []raft.NodeID
		for _, id := range nodeIDs {
			if id != nodeID {
				peers = append(peers, raft.NodeID(id))
			}
		}

		store := kv.NewStateMachine()
		tracker := kv.NewResultTracker(store)
		network := NewNetworkAdapter(transport)

		raftNode := &raft.Node{
			NodeID:       raft.NodeID(nodeID),
			Peers:        peers,
			Clock:        prod.NewRealClock(&mu),
			Network:      network,
			Storage:      sim.NewMemStorage(),
			StateMachine: tracker,
			RNG:          prod.RealRNG{},
		}
		client := NewClientHandler(transport, raftNode, store, tracker)
		NewRouter(transport, network, client, &mu)

		raftNode.Start()
		return nil
	}

	return transport.Run()
}