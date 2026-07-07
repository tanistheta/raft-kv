package raft
import "time"
type RequestVoteArgs struct {
	Term        int
	CandidateID NodeID
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (n *Node) StartElection() {
	n.CurrentTerm++
	n.Role = Candidate
	n.VotedFor = n.NodeID
	n.Storage.SaveState(PersistentState{
		CurrentTerm: n.CurrentTerm,
		VotedFor:    n.VotedFor,
	})
	n.resetElectionTimer()
	n.VotesReceived = 1 // vote for self

	for _, peerID := range n.Peers {
		args := RequestVoteArgs{
			Term:        n.CurrentTerm,
			CandidateID: n.NodeID,
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
	if args.Term == n.CurrentTerm && (n.VotedFor == "" || n.VotedFor == args.CandidateID) {
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
	if n.Role != Leader {
		return
	}
	n.sendHeartbeat()
	n.Clock.AfterFunc(50*time.Millisecond, func() {
		n.runAsLeader()
	})
}