package raft

import "time"

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

	// LeaderID is this node's best current knowledge of who the leader
	// is, for callers (prod.ClientAPI) that need to forward a client
	// request to the leader instead of just rejecting it. Empty means
	// "unknown" (e.g. mid-election, or a follower that hasn't heard from
	// a leader yet). This is advisory, not authoritative: it can lag
	// briefly behind reality (an old leader can still appear here for a
	// moment after a new election starts elsewhere), same as every other
	// piece of state a follower holds about the wider cluster. Forwarding
	// logic must treat a stale LeaderID as just another case of "that
	// wasn't actually the leader" and handle it the same way a direct
	// client call would.
	LeaderID NodeID

	// ElectionTimeoutBase, ElectionTimeoutJitter, and HeartbeatInterval
	// tune this node's timers. Zero values fall back to the original
	// constants (150ms base, 150ms jitter, 50ms heartbeat), so every
	// existing construction of Node - all of sim/, maelstrom/, and their
	// tests - keeps byte-identical timing without touching a single call
	// site. cmd/node overrides them for real deployments: the paper's
	// aggressive defaults assume scheduling latency far below what a
	// small shared VM actually delivers, and on such a VM they cause
	// election churn (and the fsync load that comes with it) whenever the
	// machine gets slow. The election timeout drawn is uniform in
	// [Base, Base+Jitter).
	ElectionTimeoutBase   time.Duration
	ElectionTimeoutJitter time.Duration
	HeartbeatInterval     time.Duration

	CommitIndex  int
	LastApplied  int
	NextIndex    map[NodeID]int
	MatchIndex   map[NodeID]int
	StateMachine StateMachine

	timerGen int
	Stopped  bool

	Clock   Clock
	Network Network
	Storage Storage
	RNG     RNG
}

func (n *Node) Stop() {
	n.Stopped = true
}

func (n *Node) Start() {
	n.loadFromStorage()
	n.Network.Register(n.NodeID, n.HandleMessage)
	n.resetElectionTimer()
}

func (n *Node) resetElectionTimer() {
	n.timerGen++
	myGen := n.timerGen
	n.Clock.AfterFunc(n.electionTimeout(), func() {
		if myGen == n.timerGen && n.Role != Leader && !n.Stopped {
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
