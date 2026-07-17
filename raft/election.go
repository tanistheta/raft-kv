package raft

import "time"

type RequestVoteArgs struct {
	Term         int
	CandidateID  NodeID
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (n *Node) StartElection() {
	n.CurrentTerm++
	n.Role = Candidate
	n.VotedFor = n.NodeID
	// Starting a campaign means this node timed out waiting on whatever
	// leader it knew about (or never knew one). Either way that
	// knowledge is now stale - clear it rather than let a forwarding
	// caller keep sending requests at a leader nobody's heard from.
	n.LeaderID = ""
	n.Storage.SaveState(PersistentState{
		CurrentTerm: n.CurrentTerm,
		VotedFor:    n.VotedFor,
	})
	n.resetElectionTimer()
	n.VotesReceived = 1 // vote for self

	for _, peerID := range n.Peers {
		args := RequestVoteArgs{
			Term:         n.CurrentTerm,
			CandidateID:  n.NodeID,
			LastLogIndex: n.LastLogIndex(),
			LastLogTerm:  n.LastLogTerm(),
		}
		msg := RPCMessage{
			Type:    "RequestVote",
			From:    n.NodeID,
			To:      peerID,
			Term:    n.CurrentTerm,
			Payload: args,
		}
		n.Network.Send(peerID, msg)
	}
}

func (n *Node) handleRequestVote(args RequestVoteArgs) RequestVoteReply {
	reply := RequestVoteReply{
		VoteGranted: false,
	}
	if args.Term > n.CurrentTerm {
		n.CurrentTerm = args.Term
		n.Role = Follower
		n.VotedFor = ""
		n.Storage.SaveState(PersistentState{
			CurrentTerm: n.CurrentTerm,
			VotedFor:    n.VotedFor,
		})
	}
	logOK := args.LastLogTerm > n.LastLogTerm() ||
		(args.LastLogTerm == n.LastLogTerm() && args.LastLogIndex >= n.LastLogIndex())

	if args.Term == n.CurrentTerm && (n.VotedFor == "" || n.VotedFor == args.CandidateID) && logOK {
		reply.VoteGranted = true
		n.VotedFor = args.CandidateID
		n.Storage.SaveState(PersistentState{
			CurrentTerm: n.CurrentTerm,
			VotedFor:    n.VotedFor,
		})
		n.resetElectionTimer()
	}
	reply.Term = n.CurrentTerm
	return reply
}

func (n *Node) handleRequestVoteReply(reply RequestVoteReply) {
	if reply.Term > n.CurrentTerm {
		n.CurrentTerm = reply.Term
		n.Role = Follower
		n.VotedFor = ""
		n.LeaderID = ""
		n.Storage.SaveState(PersistentState{
			CurrentTerm: n.CurrentTerm,
			VotedFor:    n.VotedFor,
		})
		n.resetElectionTimer()
		return
	}
	if reply.VoteGranted && n.Role == Candidate {
		n.VotesReceived++
		if n.VotesReceived >= (len(n.Peers)+1)/2+1 {
			n.Role = Leader
			// A caller forwarding a client request only needs
			// LeaderID to point somewhere reachable - pointing it
			// at ourselves the instant we win is simpler than
			// leaving it at whatever the old leader's ID happened
			// to be (or empty) until the first heartbeat round-trips.
			n.LeaderID = n.NodeID
			n.NextIndex = make(map[NodeID]int)
			n.MatchIndex = make(map[NodeID]int)
			for _, p := range n.Peers {
				n.NextIndex[p] = n.LastLogIndex() + 1
				n.MatchIndex[p] = 0
			}
			n.runAsLeader()
		}
	}
}

func (n *Node) electionTimeout() time.Duration {
	base := 150 * time.Millisecond
	jitter := time.Duration(n.RNG.Intn(150)) * time.Millisecond
	return base + jitter
}

func (n *Node) runAsLeader() {
	if n.Role != Leader || n.Stopped {
		return
	}
	n.sendHeartbeat()
	n.Clock.AfterFunc(50*time.Millisecond, func() {
		n.runAsLeader()
	})
}