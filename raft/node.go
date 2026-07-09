package raft

type Role string

const (
	Follower  Role = "FOLLOWER"
	Candidate Role = "CANDIDATE"
	Leader    Role = "LEADER"
)

type Node struct {
	Role          Role
	CurrentTerm   int
	VotedFor      NodeID
	Log           []LogEntry
	NodeID        NodeID
	VotesReceived int
	Peers         []NodeID

	CommitIndex int
	NextIndex map[NodeID]int
	MatchIndex map[NodeID]int

	timerGen int

	Clock   Clock
	Network Network
	Storage Storage
	RNG     RNG
}

func (n *Node) Start() {
	n.Network.Register(n.NodeID, n.HandleMessage)
	n.resetElectionTimer()
}

func (n *Node) resetElectionTimer() {
	n.timerGen++
	myGen := n.timerGen
	n.Clock.AfterFunc(n.electionTimeout(), func() {
		if myGen == n.timerGen && n.Role != Leader {
			n.StartElection()
		}
	})
}

func (n *Node) HandleMessage(msg RPCMessage) {
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
}