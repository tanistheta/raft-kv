package sim

import "raft-kv/raft"

type InMemoryNetwork struct {
	handlers  map[raft.NodeID]func(raft.RPCMessage)
	scheduler *Scheduler
	injector  *FaultInjector
}

func NewInMemoryNetwork(s *Scheduler, injector *FaultInjector) *InMemoryNetwork {
	return &InMemoryNetwork{
		handlers:  make(map[raft.NodeID]func(raft.RPCMessage)),
		scheduler: s,
		injector:  injector,
	}
}

func (net *InMemoryNetwork) Register(id raft.NodeID, handler func(raft.RPCMessage)) {
	net.handlers[id] = handler
}

func (net *InMemoryNetwork) Unregister(id raft.NodeID) {
	delete(net.handlers, id)
}

func (net *InMemoryNetwork) Send(to raft.NodeID, msg raft.RPCMessage) error {
	if _, ok := net.handlers[msg.From]; !ok {
		return nil
	}
	handler, ok := net.handlers[to]
	if !ok {
		return nil
	}

	delay := net.injector.NetworkDelay()
	net.scheduler.Schedule(delay, func() {
		if net.injector.ShouldDrop(msg.From, to, msg) {
			return
		}
		handler(msg)
	})
	return nil
}