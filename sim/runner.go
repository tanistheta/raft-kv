package sim

import (
	"fmt"
	"time"

	"raft-kv/checker"
	"raft-kv/kv"
	"raft-kv/raft"
)

type RunConfig struct {
	NumNodes     int
	Duration     time.Duration
	DropRate     float64
	TickEvery    time.Duration
	EnableFaults bool
}

func DefaultConfig() RunConfig {
	return RunConfig{
		NumNodes:     5,
		Duration:     10 * time.Second,
		DropRate:     0.05,
		TickEvery:    20 * time.Millisecond,
		EnableFaults: true,
	}
}

type PartitionEvent struct {
	Start    int64
	End      int64
	Minority []raft.NodeID
}

type Result struct {
	Seed       int64
	History    []checker.Op
	Linear     bool
	Violation  string
	Partitions []PartitionEvent
}

func RunSimulation(seed int64, cfg RunConfig) Result {
	scheduler := NewScheduler()
	injector := NewFaultInjector(seed)
	injector.dropRate = cfg.DropRate
	network := NewInMemoryNetwork(scheduler, injector)
	rng := NewSeededRNG(seed)

	ids := make([]raft.NodeID, cfg.NumNodes)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("N%d", i))
	}

	nodes := make(map[raft.NodeID]*raft.Node)
	storages := make(map[raft.NodeID]*MemStorage)
	stores := make(map[raft.NodeID]*kv.StateMachine)
	for _, id := range ids {
		storages[id] = NewMemStorage()
	}

	makeNode := func(id raft.NodeID) *raft.Node {
		peers := make([]raft.NodeID, 0, len(ids)-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		store := kv.NewStateMachine()
		stores[id] = store
		node := &raft.Node{
			NodeID:       id,
			Peers:        peers,
			Role:         raft.Follower,
			Clock:        scheduler,
			Network:      network,
			Storage:      storages[id],
			RNG:          NewSeededRNG(seed + int64(indexOf(ids, id)) + 1),
			StateMachine: store,
		}
		node.Start()
		return node
	}

	for _, id := range ids {
		nodes[id] = makeNode(id)
	}
	nodes[ids[0]].StartElection()

	workload := &Workload{
		Scheduler: scheduler,
		Nodes:     nodes,
		Stores:    stores,
		NodeOrder: ids,
		RNG:       rng,
		Keys:      []string{"x", "y", "z"},
		TickEvery: cfg.TickEvery,
	}
	workload.Start()

	var partitions []PartitionEvent
	if cfg.EnableFaults {
		scheduleFaults(scheduler, injector, network, nodes, ids, makeNode, rng, cfg.Duration, &partitions)
	}

	scheduler.RunFor(cfg.Duration)
	workload.Stop()

	ok, reason := checker.CheckLinearizable(workload.History)
	return Result{Seed: seed, History: workload.History, Linear: ok, Violation: reason, Partitions: partitions}
}

func scheduleFaults(scheduler *Scheduler, injector *FaultInjector, network *InMemoryNetwork,
	nodes map[raft.NodeID]*raft.Node, ids []raft.NodeID, makeNode func(raft.NodeID) *raft.Node,
	rng raft.RNG, total time.Duration, partitions *[]PartitionEvent) {

	numEvents := 3 + rng.Intn(4)
	for e := 0; e < numEvents; e++ {
		at := time.Duration(rng.Intn(int(total)))
		scheduler.AfterFunc(at, func() {
			victim := ids[rng.Intn(len(ids))]
			node, ok := nodes[victim]
			if !ok || node.Stopped {
				return
			}
			if rng.Intn(2) == 0 {
				node.Stop()
				network.Unregister(victim)
				delete(nodes, victim)
				restartAfter := 50*time.Millisecond + time.Duration(rng.Intn(200))*time.Millisecond
				scheduler.AfterFunc(restartAfter, func() {
					nodes[victim] = makeNode(victim)
				})
			} else {
				var rest []raft.NodeID
				for _, id := range ids {
					if id != victim {
						rest = append(rest, id)
					}
				}
				injector.Partition([]raft.NodeID{victim}, rest)
				startedAt := int64(scheduler.Now().UnixNano())
				ev := &PartitionEvent{Start: startedAt, End: -1, Minority: []raft.NodeID{victim}}
				*partitions = append(*partitions, *ev)
				evIdx := len(*partitions) - 1
				healAfter := 100*time.Millisecond + time.Duration(rng.Intn(300))*time.Millisecond
				scheduler.AfterFunc(healAfter, func() {
					injector.HealPartition()
					(*partitions)[evIdx].End = int64(scheduler.Now().UnixNano())
				})
			}
		})
	}
}

func indexOf(ids []raft.NodeID, id raft.NodeID) int {
	for i, x := range ids {
		if x == id {
			return i
		}
	}
	return 0
}