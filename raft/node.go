package raft
import "time"

type Role string

const (
	Follower  Role = "FOLLOWER"
	Candidate Role = "CANDIDATE"
	Leader    Role = "LEADER"
)

type Node struct {
	Role      Role
	CurrentTerm int
	VotedFor    NodeID
	Log         []LogEntry
	NodeID      NodeID
	ElectionTimer <- chan time.Time
	VotesReceived int
	Peers 	  []NodeID
	Inbox   chan RPCMessage

	Clock   Clock
	Network Network
	Storage Storage
	RNG     RNG
}

func (n *Node) Run() {
	for {
		select {
		case msg := <-n.Inbox:
			switch msg.Type {
			case "RequestVote":
				args := msg.Payload.(RequestVoteArgs)
				reply := n.handleRequestVote(args)
				n.Network.Send(msg.From, RPCMessage{
					Type: "RequestVoteReply", From: n.NodeID, To: msg.From,
					Term: n.CurrentTerm, Payload: reply,
				})
			case "RequestVoteReply":
				n.handleRequestVoteReply(msg.Payload.(RequestVoteReply))
			case "AppendEntries":
				args := msg.Payload.(AppendEntriesArgs)
				reply := n.handleAppendEntries(args)
				n.Network.Send(msg.From, RPCMessage{
					Type: "AppendEntriesReply", From: n.NodeID, To: msg.From,
					Term: n.CurrentTerm, Payload: reply,
				})
			case "AppendEntriesReply":
				n.handleAppendEntriesReply(msg.Payload.(AppendEntriesReply))
			}
		case <-n.ElectionTimer:
			if n.Role != Leader {
				n.StartElection()
			}
		}
	}
}