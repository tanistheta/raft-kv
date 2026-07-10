package sim

import (
	"fmt"
	"sync"

	"raft-kv/raft"
)

type MemStorage struct {
	mu    sync.Mutex
	log   []raft.LogEntry
	state raft.PersistentState
}

func NewMemStorage() *MemStorage {
	return &MemStorage{}
}

func (m *MemStorage) AppendLog(entries []raft.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = append(m.log, entries...)
	return nil
}

func (m *MemStorage) ReadLog(fromIndex int) ([]raft.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fromIndex <= 0 {
		fromIndex = 1
	}
	if fromIndex > len(m.log) {
		return nil, nil
	}
	out := make([]raft.LogEntry, len(m.log)-(fromIndex-1))
	copy(out, m.log[fromIndex-1:])
	return out, nil
}

func (m *MemStorage) LastLogIndex() (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.log), nil
}

func (m *MemStorage) TruncateLog(fromIndex int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fromIndex <= 0 {
		return fmt.Errorf("invalid truncate index %d", fromIndex)
	}
	if fromIndex-1 < len(m.log) {
		m.log = m.log[:fromIndex-1]
	}
	return nil
}

func (m *MemStorage) SaveState(state raft.PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
	return nil
}

func (m *MemStorage) LoadState() (raft.PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state, nil
}
