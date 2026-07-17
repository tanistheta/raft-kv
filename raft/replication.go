package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderID     NodeID
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	From          NodeID
	ConflictIndex int
	MatchIndex    int
}

func (n *Node) handleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	reply := AppendEntriesReply{Success: false, From: n.NodeID}

	if args.Term < n.CurrentTerm {
		reply.Term = n.CurrentTerm
		return reply
	}

	if args.Term > n.CurrentTerm {
		n.CurrentTerm = args.Term
		n.VotedFor = ""
		n.Storage.SaveState(PersistentState{
			CurrentTerm: n.CurrentTerm,
			VotedFor:    n.VotedFor,
		})
	}
	n.Role = Follower
	n.resetElectionTimer()
	// Recorded even if the consistency check below rejects this
	// particular AppendEntries: args.Term >= n.CurrentTerm already
	// established args.LeaderID as the legitimate leader for this term,
	// regardless of whether this specific batch of entries happens to
	// line up with our log yet.
	n.LeaderID = args.LeaderID

	if args.PrevLogIndex > 0 {
		entry, err := n.GetLogEntry(args.PrevLogIndex)
		if err != nil {
			reply.ConflictIndex = n.LastLogIndex() + 1
			reply.Term = n.CurrentTerm
			return reply
		}
		if entry.Term != args.PrevLogTerm {
			reply.ConflictIndex = args.PrevLogIndex
			reply.Term = n.CurrentTerm
			return reply
		}
	}

	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= len(n.Log) {
			existing := n.Log[idx-1]
			if existing.Term != e.Term {
				n.Log = n.Log[:idx-1]
				if n.Storage != nil {
					n.Storage.TruncateLog(idx)
				}
				n.Log = append(n.Log, e)
				if n.Storage != nil {
					n.Storage.AppendLog([]LogEntry{e})
				}
			}
		} else {
			n.Log = append(n.Log, e)
			if n.Storage != nil {
				n.Storage.AppendLog([]LogEntry{e})
			}
		}
	}

	if args.LeaderCommit > n.CommitIndex {
		n.CommitIndex = min(args.LeaderCommit, n.LastLogIndex())
		n.applyCommitted()
	}

	reply.Success = true
	reply.Term = n.CurrentTerm
	reply.MatchIndex = args.PrevLogIndex + len(args.Entries)
	return reply
}

func (n *Node) sendAppendEntries(peerID NodeID) {
	nextIdx := n.NextIndex[peerID]
	prevLogIndex := nextIdx - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		if e, err := n.GetLogEntry(prevLogIndex); err == nil {
			prevLogTerm = e.Term
		}
	}

	var entries []LogEntry
	if nextIdx <= n.LastLogIndex() {
		src := n.Log[nextIdx-1:]
		entries = make([]LogEntry, len(src))
		copy(entries, src)
	}

	args := AppendEntriesArgs{
		Term:         n.CurrentTerm,
		LeaderID:     n.NodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.CommitIndex,
	}
	msg := RPCMessage{
		Type: "AppendEntries", From: n.NodeID, To: peerID,
		Term: n.CurrentTerm, Payload: args,
	}
	n.Network.Send(peerID, msg)
}

func (n *Node) sendHeartbeat() {
	for _, peerID := range n.Peers {
		n.sendAppendEntries(peerID)
	}
}

func (n *Node) handleAppendEntriesReply(reply AppendEntriesReply) {
	if reply.Term > n.CurrentTerm {
		n.CurrentTerm = reply.Term
		n.Role = Follower
		n.VotedFor = ""
		// This node just learned it's no longer (or never was) leader
		// for the current term. It doesn't yet know who is - that
		// comes later via an AppendEntries from whoever actually won.
		n.LeaderID = ""
		n.Storage.SaveState(PersistentState{
			CurrentTerm: n.CurrentTerm,
			VotedFor:    n.VotedFor,
		})
		return
	}
	if n.Role != Leader {
		return
	}

	if reply.Success {
		if reply.MatchIndex > n.MatchIndex[reply.From] {
			n.MatchIndex[reply.From] = reply.MatchIndex
			n.NextIndex[reply.From] = reply.MatchIndex + 1
		}
		n.advanceCommitIndex()
	} else {
		if reply.ConflictIndex > 0 {
			n.NextIndex[reply.From] = reply.ConflictIndex
		} else if n.NextIndex[reply.From] > 1 {
			n.NextIndex[reply.From]--
		}
	}
}

func (n *Node) advanceCommitIndex() {
	for idx := n.LastLogIndex(); idx > n.CommitIndex; idx-- {
		entry, err := n.GetLogEntry(idx)
		if err != nil || entry.Term != n.CurrentTerm {
			continue
		}
		count := 1
		for _, p := range n.Peers {
			if n.MatchIndex[p] >= idx {
				count++
			}
		}
		if count >= (len(n.Peers)+1)/2+1 {
			n.CommitIndex = idx
			n.applyCommitted()
			break
		}
	}
}