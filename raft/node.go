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

	Clock   Clock
	Network Network
	Storage Storage
	RNG     RNG
}
