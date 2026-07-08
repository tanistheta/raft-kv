package raft

import "fmt"

func (n *Node) LastLogIndex() int {
	if len(n.Log) == 0 {
		return 0
	}
	return n.Log[len(n.Log)-1].Index
}

func (n *Node) LastLogTerm() int {
	if len(n.Log) == 0 {
		return 0
	}
	return n.Log[len(n.Log)-1].Term
}

func (n *Node) GetLogEntry(index int) (LogEntry, error) {
	if index <= 0 {
		return LogEntry{}, fmt.Errorf("invalid index %d: log is 1-indexed", index)
	}
	sliceIdx := index - 1
	if sliceIdx >= len(n.Log) {
		return LogEntry{}, fmt.Errorf("index %d out of bounds (log length %d)", index, len(n.Log))
	}
	return n.Log[sliceIdx], nil
}

func (n *Node) AppendLogEntry(entry LogEntry) {
	entry.Index = len(n.Log) + 1
	n.Log = append(n.Log, entry)
}