package sim

import "raft-kv/raft"

type InMemoryNetwork struct {
	inboxes map[raft.NodeID]chan raft.RPCMessage
}

func NewInMemoryNetwork() *InMemoryNetwork {
	return &InMemoryNetwork{
		inboxes: make(map[raft.NodeID]chan raft.RPCMessage),
	}
}

func (net *InMemoryNetwork) Register(id raft.NodeID) chan raft.RPCMessage {
	ch := make(chan raft.RPCMessage, 100) 
	net.inboxes[id] = ch
	return ch
}

func (net *InMemoryNetwork) Send(to raft.NodeID, msg raft.RPCMessage) error {
	ch, ok := net.inboxes[to]
	if !ok {
		return nil 
	}
	ch <- msg
	return nil
}

func (net *InMemoryNetwork) Recv() <-chan raft.RPCMessage {
	panic("Recv() is per-node, not per-network — use the channel from Register()")
}