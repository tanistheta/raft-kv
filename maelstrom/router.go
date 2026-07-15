package maelstrom

import (
	"encoding/json"
	"sync"
)

// raftRPCTypes are the message types NetworkAdapter owns; everything else
// goes to ClientHandler.
var raftRPCTypes = map[string]bool{
	"RequestVote":        true,
	"RequestVoteReply":   true,
	"AppendEntries":      true,
	"AppendEntriesReply": true,
}

// Router combines a NetworkAdapter and a ClientHandler into the single
// Handler a maelstrom.Node can have. Constructing it takes over the
// transport's Handler (overriding whatever NewNetworkAdapter set), so
// build order doesn't matter as long as Router is constructed last.
//
// mu must be the exact *sync.Mutex passed to this raft.Node's
// prod.RealClock - Dispatch holds it for the same reason AfterFunc does:
// raft.Node has no locking of its own, so every path that touches it
// (timers, RPC dispatch, client ops) has to share one lock.
type Router struct {
	network *NetworkAdapter
	client  *ClientHandler
	mu      *sync.Mutex
}

func NewRouter(transport *Node, network *NetworkAdapter, client *ClientHandler, mu *sync.Mutex) *Router {
	r := &Router{network: network, client: client, mu: mu}
	transport.Handler = r.Dispatch
	return r
}

// Dispatch routes msg by type, then always gives ClientHandler a chance to
// resolve pending writes - commit progress can only have changed in
// response to a message, so checking here is sufficient without a
// separate polling loop.
func (r *Router) Dispatch(msg Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var body baseBody
	if err := json.Unmarshal(msg.Body, &body); err == nil && raftRPCTypes[body.Type] {
		r.network.dispatch(msg)
	} else {
		r.client.Handle(msg)
	}
	r.client.ResolvePending()
}