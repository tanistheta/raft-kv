package sim

import (
	"time"
	"raft-kv/raft"
)

type StubClock struct{}

func (StubClock) Now() time.Time {
	return time.Now()
}  
func (StubClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	return ch
}

type StubNetwork struct {
	recvCh chan raft.RPCMessage
}	

func NewStubNetwork() *StubNetwork {
	return &StubNetwork{
		recvCh: make(chan raft.RPCMessage),
	}
}

func (n *StubNetwork) Send(to raft.NodeID, msg raft.RPCMessage) error {
	return nil
}

func (n *StubNetwork) Recv() <-chan raft.RPCMessage {
	return n.recvCh
}

type StubStorage struct{}

func (StubStorage) AppendLog(entries []raft.LogEntry) error { return nil }
func (StubStorage) ReadLog(fromIndex int) ([]raft.LogEntry, error) { return nil, nil }
func (StubStorage) LastLogIndex() (int, error) { return 0, nil }
func (StubStorage) TruncateLog(fromIndex int) error { return nil }
func (StubStorage) SaveState(state raft.PersistentState) error { return nil }
func (StubStorage) LoadState() (raft.PersistentState, error) { return raft.PersistentState{}, nil
}

type StubRNG struct{}

func (StubRNG) Intn(n int) int {return 0}