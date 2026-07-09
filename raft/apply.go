package raft

func (n *Node) applyCommitted() {
	for n.LastApplied < n.CommitIndex {
		n.LastApplied++
		entry, err := n.GetLogEntry(n.LastApplied)
		if err != nil {
			n.LastApplied--
			return
		}
		if n.StateMachine != nil {
			n.StateMachine.Apply(entry.Command)
		}
	}
}
