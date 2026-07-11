package sim

import (
	"fmt"
	"time"

	"raft-kv/checker"
	"raft-kv/kv"
	"raft-kv/raft"
)

type Workload struct {
	Scheduler *Scheduler
	Nodes     map[raft.NodeID]*raft.Node
	Stores    map[raft.NodeID]*kv.StateMachine
	NodeOrder []raft.NodeID
	RNG       raft.RNG
	Keys      []string
	TickEvery time.Duration

	History []checker.Op

	valueSeq int
	pending  []*pendingWrite
	stopped  bool
}

type pendingWrite struct {
	op    checker.Op
	index int
	term  int
}

func (w *Workload) Start() { w.tick() }

func (w *Workload) Stop() { w.stopped = true }

func (w *Workload) tick() {
	if w.stopped {
		return
	}
	w.resolvePending()
	w.issueOp()
	w.Scheduler.AfterFunc(w.TickEvery, w.tick)
}

func (w *Workload) leader() (raft.NodeID, *raft.Node) {
	for _, id := range w.NodeOrder {
		if n, ok := w.Nodes[id]; ok && !n.Stopped && n.Role == raft.Leader {
			return id, n
		}
	}
	return "", nil
}

func (w *Workload) issueOp() {
	leaderID, leader := w.leader()
	if leader == nil {
		return
	}
	key := w.Keys[w.RNG.Intn(len(w.Keys))]
	now := int64(w.Scheduler.Now().UnixNano())

	if w.RNG.Intn(2) == 0 {
		w.valueSeq++
		value := fmt.Sprintf("v%d", w.valueSeq)
		leader.AppendLogEntry(raft.LogEntry{
			Term:    leader.CurrentTerm,
			Command: []byte(fmt.Sprintf("SET %s=%s", key, value)),
		})
		w.pending = append(w.pending, &pendingWrite{
			op:    checker.Op{Kind: checker.Write, Key: key, Value: value, Start: now, ServedBy: leaderID},
			index: leader.LastLogIndex(),
			term:  leader.CurrentTerm,
		})
		return
	}

	store := w.Stores[leaderID]
	if store == nil {
		return
	}
	if leader.CommitIndex < leader.LastLogIndex() {

		return
	}
	val, _ := store.Get(key)
	w.History = append(w.History, checker.Op{
		Kind: checker.Read, Key: key, Value: val, Start: now, End: now, ServedBy: leaderID,
	})
}

func (w *Workload) resolvePending() {
	if len(w.pending) == 0 {
		return
	}
	_, leader := w.leader()
	var still []*pendingWrite
	for _, p := range w.pending {
		if leader == nil || leader.LastLogIndex() < p.index {
			still = append(still, p)
			continue
		}
		entry, err := leader.GetLogEntry(p.index)
		if err != nil {
			still = append(still, p)
			continue
		}
		if entry.Term != p.term {
			continue
		}
		if leader.CommitIndex >= p.index {
			p.op.End = int64(w.Scheduler.Now().UnixNano())
			w.History = append(w.History, p.op)
			continue
		}
		still = append(still, p)
	}
	w.pending = still
}