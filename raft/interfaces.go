package raft

import "time"

type NodeID string

type Clock interface{
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type RPCMessage struct{
	From NodeID
	To NodeID
	Term int
	Payload interface{}
}

type Network interface{
	Send(to NodeID, msg RPCMessage) error
	Recv() <-chan RPCMessage
}

type LogEntry struct{
	Term int
	Index int
	Command []byte
}

type PersistentState struct{
	CurrentTerm int
	VotedFor NodeID
}

type Storage interface{
	AppendLog(entries []LogEntry) error
	ReadLog(fromIndex int) ([]LogEntry, error)
	LastLogIndex() (int, error)
	TruncateLog(fromIndex int) error

	SaveState(state PersistentState) error
	LoadState() (PersistentState, error)
}

type RNG interface{
	Intn(n int) int
}
