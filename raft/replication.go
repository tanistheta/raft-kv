package raft

type AppendEntriesArgs struct {
	Term     int
	LeaderID NodeID
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (n *Node) handleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	reply := AppendEntriesReply{
		Success: false,
	}
	if args.Term < n.CurrentTerm {
		reply.Term = n.CurrentTerm
		return reply
	}

	if args.Term > n.CurrentTerm {
		n.CurrentTerm = args.Term
		n.Role = Follower
	}

	n.ElectionTimer = n.Clock.After(n.electionTimeout())

	reply.Success = true
	reply.Term = n.CurrentTerm
	return reply
}

func (n *Node) sendHeartbeat() {
	for _, peerID := range n.Peers {
		args := AppendEntriesArgs{
			Term:     n.CurrentTerm,
			LeaderID: n.NodeID,
		}
		msg := RPCMessage{
			Type:    "AppendEntries",
			From:    n.NodeID,
			To:      peerID,
			Term:    n.CurrentTerm,
			Payload: args,
		}
		n.Network.Send(peerID, msg)
	}
}

func (n *Node) handleAppendEntriesReply(reply AppendEntriesReply) {
	if reply.Term > n.CurrentTerm {
		n.CurrentTerm = reply.Term
		n.Role = Follower
	}
}
