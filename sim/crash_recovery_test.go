package sim

import (
	"testing"
	"time"

	"raft-kv/raft"
)

func setupNodesWithStorage(scheduler *Scheduler, network *InMemoryNetwork, ids []raft.NodeID) (map[raft.NodeID]*raft.Node, map[raft.NodeID]*MemStorage) {
	nodes := make(map[raft.NodeID]*raft.Node)
	storages := make(map[raft.NodeID]*MemStorage)
	for i, id := range ids {
		peers := []raft.NodeID{}
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		storage := NewMemStorage()
		storages[id] = storage
		node := &raft.Node{
			NodeID:  id,
			Peers:   peers,
			Role:    raft.Follower,
			Clock:   scheduler,
			Network: network,
			Storage: storage,
			RNG:     NewSeededRNG(int64(1000 + i)),
		}
		node.Start()
		nodes[id] = node
	}
	return nodes, storages
}

func findLeader(nodes map[raft.NodeID]*raft.Node) (raft.NodeID, int) {
	var leaderID raft.NodeID
	count := 0
	for id, node := range nodes {
		if node.Role == raft.Leader {
			leaderID = id
			count++
		}
	}
	return leaderID, count
}

func TestLeaderCrashAndRestartRecoversState(t *testing.T) {
	scheduler := NewScheduler()
	injector := NewFaultInjector(42)
	network := NewInMemoryNetwork(scheduler, injector)

	ids := []raft.NodeID{"A", "B", "C"}
	nodes, storages := setupNodesWithStorage(scheduler, network, ids)

	nodes["A"].StartElection()
	scheduler.RunFor(500 * time.Millisecond)

	leaderID, leaderCount := findLeader(nodes)
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader before crash, found %d", leaderCount)
	}
	leader := nodes[leaderID]
	leader.AppendLogEntry(raft.LogEntry{Term: leader.CurrentTerm, Command: []byte("SET x=1")})
	scheduler.RunFor(500 * time.Millisecond)

	if leader.CommitIndex != 1 {
		t.Fatalf("expected entry 1 committed before crash, CommitIndex=%d", leader.CommitIndex)
	}
	termAtCrash := leader.CurrentTerm

	leader.Stop()
	network.Unregister(leaderID)
	delete(nodes, leaderID)

	scheduler.RunFor(500 * time.Millisecond)
	newLeaderID, newLeaderCount := findLeader(nodes)
	if newLeaderCount != 1 {
		t.Fatalf("expected 1 new leader among survivors, found %d", newLeaderCount)
	}
	if newLeaderID == leaderID {
		t.Fatalf("crashed node still counted as leader")
	}
	newLeader := nodes[newLeaderID]
	newLeader.AppendLogEntry(raft.LogEntry{Term: newLeader.CurrentTerm, Command: []byte("SET y=2")})
	scheduler.RunFor(500 * time.Millisecond)
	if newLeader.CommitIndex != 2 {
		t.Fatalf("expected entry 2 committed while old leader was down, CommitIndex=%d", newLeader.CommitIndex)
	}

	restartPeers := []raft.NodeID{}
	for _, id := range ids {
		if id != leaderID {
			restartPeers = append(restartPeers, id)
		}
	}
	restarted := &raft.Node{
		NodeID:  leaderID,
		Peers:   restartPeers,
		Role:    raft.Follower,
		Clock:   scheduler,
		Network: network,
		Storage: storages[leaderID],
		RNG:     NewSeededRNG(9999),
	}

	restarted.Start()
	if restarted.CurrentTerm < termAtCrash {
		t.Fatalf("restarted node lost term: had %d before crash, reloaded %d", termAtCrash, restarted.CurrentTerm)
	}
	if restarted.LastLogIndex() != 1 {
		t.Fatalf("restarted node lost its pre-crash log: expected LastLogIndex 1, got %d", restarted.LastLogIndex())
	}
	entry, err := restarted.GetLogEntry(1)
	if err != nil || string(entry.Command) != "SET x=1" {
		t.Fatalf("restarted node's reloaded entry 1 wrong: err=%v entry=%+v", err, entry)
	}

	nodes[leaderID] = restarted

	scheduler.RunFor(1 * time.Second)

	finalLeaderID, finalLeaderCount := findLeader(nodes)
	if finalLeaderCount != 1 {
		t.Fatalf("expected exactly 1 leader after restart, found %d", finalLeaderCount)
	}
	if finalLeaderID == leaderID {
		t.Fatalf("restarted node became leader without winning an election it should have lost (stale term should not let it lead)")
	}

	if restarted.LastLogIndex() != 2 {
		t.Fatalf("restarted node did not catch up: expected LastLogIndex 2, got %d", restarted.LastLogIndex())
	}
	caughtUpEntry, err := restarted.GetLogEntry(2)
	if err != nil || string(caughtUpEntry.Command) != "SET y=2" {
		t.Fatalf("restarted node missing entry it should have replayed: err=%v entry=%+v", err, caughtUpEntry)
	}
	if restarted.CommitIndex != 2 {
		t.Fatalf("restarted node CommitIndex not advanced: got %d", restarted.CommitIndex)
	}

	for id, node := range nodes {
		if node.LastLogIndex() < 2 {
			t.Errorf("node %v did not converge: LastLogIndex=%d", id, node.LastLogIndex())
			continue
		}
		e1, _ := node.GetLogEntry(1)
		e2, _ := node.GetLogEntry(2)
		if string(e1.Command) != "SET x=1" || string(e2.Command) != "SET y=2" {
			t.Errorf("node %v diverged: entry1=%q entry2=%q", id, e1.Command, e2.Command)
		}
	}
}

func TestRestartedNodeDoesNotDoubleVote(t *testing.T) {
	scheduler := NewScheduler()
	injector := NewFaultInjector(7)
	network := NewInMemoryNetwork(scheduler, injector)

	ids := []raft.NodeID{"A", "B", "C"}
	nodes, storages := setupNodesWithStorage(scheduler, network, ids)

	nodes["A"].StartElection()
	scheduler.RunFor(200 * time.Millisecond)

	voter := nodes["C"]
	votedTerm := voter.CurrentTerm
	votedFor := voter.VotedFor
	if votedFor == "" {
		t.Fatalf("expected node C to have voted in term %d, VotedFor is empty", votedTerm)
	}

	voter.Stop()
	network.Unregister("C")
	restarted := &raft.Node{
		NodeID:  "C",
		Peers:   []raft.NodeID{"A", "B"},
		Role:    raft.Follower,
		Clock:   scheduler,
		Network: network,
		Storage: storages["C"],
		RNG:     NewSeededRNG(4242),
	}
	restarted.Start()

	if restarted.CurrentTerm != votedTerm {
		t.Fatalf("restarted node lost term: expected %d, got %d", votedTerm, restarted.CurrentTerm)
	}
	if restarted.VotedFor != votedFor {
		t.Fatalf("restarted node lost its vote record: expected VotedFor=%v, got %v", votedFor, restarted.VotedFor)
	}

	var gotReply raft.RequestVoteReply
	network.Register("__test_rival__", func(msg raft.RPCMessage) {
		if msg.Type == "RequestVoteReply" {
			gotReply = msg.Payload.(raft.RequestVoteReply)
		}
	})
	restarted.HandleMessage(raft.RPCMessage{
		Type: "RequestVote",
		From: "__test_rival__",
		To:   "C",
		Term: votedTerm,
		Payload: raft.RequestVoteArgs{
			Term:         votedTerm,
			CandidateID:  "B",
			LastLogIndex: restarted.LastLogIndex(),
			LastLogTerm:  restarted.LastLogTerm(),
		},
	})

	if votedFor != "B" && gotReply.VoteGranted {
		t.Fatalf("restarted node granted a second vote in term %d after crash; persisted VotedFor was not honored", votedTerm)
	}
}

func TestMinorityCrashDuringPartitionHealReconciles(t *testing.T) {
	scheduler := NewScheduler()
	injector := NewFaultInjector(17)
	network := NewInMemoryNetwork(scheduler, injector)

	ids := []raft.NodeID{"A", "B", "C", "D", "E"}
	nodes, storages := setupNodesWithStorage(scheduler, network, ids)

	nodes["A"].StartElection()
	scheduler.RunFor(500 * time.Millisecond)

	leaderID, leaderCount := findLeader(nodes)
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader before partition, found %d", leaderCount)
	}
	leader := nodes[leaderID]
	leader.AppendLogEntry(raft.LogEntry{Term: leader.CurrentTerm, Command: []byte("SET x=1")})
	scheduler.RunFor(500 * time.Millisecond)
	if leader.CommitIndex != 1 {
		t.Fatalf("expected entry 1 committed before partition, CommitIndex=%d", leader.CommitIndex)
	}

	var minority, majority []raft.NodeID
	minority = append(minority, leaderID)
	for _, id := range ids {
		if id == leaderID {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}
	injector.Partition(minority, majority)

	leader.AppendLogEntry(raft.LogEntry{Term: leader.CurrentTerm, Command: []byte("SET conflict=1")})
	scheduler.RunFor(300 * time.Millisecond)
	if leader.CommitIndex != 1 {
		t.Fatalf("stranded leader should not be able to commit past index 1, CommitIndex=%d", leader.CommitIndex)
	}

	scheduler.RunFor(500 * time.Millisecond)
	var majorityLeaderID raft.NodeID
	majorityLeaderCount := 0
	for _, id := range majority {
		if nodes[id].Role == raft.Leader {
			majorityLeaderCount++
			majorityLeaderID = id
		}
	}
	if majorityLeaderCount != 1 {
		t.Fatalf("expected exactly 1 leader on majority side, found %d", majorityLeaderCount)
	}
	majorityLeader := nodes[majorityLeaderID]
	majorityLeader.AppendLogEntry(raft.LogEntry{Term: majorityLeader.CurrentTerm, Command: []byte("SET y=2")})
	scheduler.RunFor(500 * time.Millisecond)
	if majorityLeader.CommitIndex != 2 {
		t.Fatalf("expected majority to commit entry 2, CommitIndex=%d", majorityLeader.CommitIndex)
	}

	for _, id := range minority {
		nodes[id].Stop()
		network.Unregister(id)
		delete(nodes, id)
	}
	injector.HealPartition()

	for i, id := range minority {
		var peers []raft.NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		restarted := &raft.Node{
			NodeID:  id,
			Peers:   peers,
			Role:    raft.Follower,
			Clock:   scheduler,
			Network: network,
			Storage: storages[id],
			RNG:     NewSeededRNG(int64(5000 + i)),
		}
		restarted.Start()
		nodes[id] = restarted
	}

	scheduler.RunFor(1000 * time.Millisecond)

	finalLeaderID, finalLeaderCount := findLeader(nodes)
	if finalLeaderCount != 1 {
		t.Fatalf("expected exactly 1 leader after reconciliation, found %d", finalLeaderCount)
	}
	for _, id := range minority {
		if finalLeaderID == id {
			t.Fatalf("a node that just restarted from a stale minority partition became leader: %v", id)
		}
	}

	for id, node := range nodes {
		if node.LastLogIndex() < 2 {
			t.Errorf("node %v did not catch up: LastLogIndex=%d", id, node.LastLogIndex())
			continue
		}
		entry, err := node.GetLogEntry(2)
		if err != nil {
			t.Errorf("node %v: entry 2 missing: %v", id, err)
			continue
		}
		if string(entry.Command) != "SET y=2" {
			t.Errorf("node %v did not reconcile: entry 2 is %q, want \"SET y=2\"", id, entry.Command)
		}
	}

	for _, id := range minority {
		persisted, err := storages[id].ReadLog(1)
		if err != nil {
			t.Fatalf("node %v: could not read persisted log: %v", id, err)
		}
		if len(persisted) < 2 {
			t.Fatalf("node %v: persisted log did not catch up, len=%d", id, len(persisted))
		}
		if string(persisted[1].Command) != "SET y=2" {
			t.Fatalf("node %v: persisted entry 2 is stale/wrong: %q (TruncateLog was not actually persisted)", id, persisted[1].Command)
		}
	}
}
