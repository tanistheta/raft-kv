package sim

import (
	"math/rand"
	"time"

	"raft-kv/raft"
)

type FaultInjector struct {
	rng      *rand.Rand
	dropRate float64
	minDelay time.Duration
	maxDelay time.Duration
	partitionA map[raft.NodeID]bool
	partitionB map[raft.NodeID]bool
}

func NewFaultInjector(seed int64) *FaultInjector {
	return &FaultInjector{
		rng:      rand.New(rand.NewSource(seed)),
		dropRate: 0,
		minDelay: 1 * time.Millisecond,
		maxDelay: 10 * time.Millisecond,
	}
}

func (f *FaultInjector) NetworkDelay() time.Duration {
	span := int64(f.maxDelay - f.minDelay)
	return f.minDelay + time.Duration(f.rng.Int63n(span+1))
}

func (f *FaultInjector) Partition(groupA, groupB []raft.NodeID) {
	f.partitionA = make(map[raft.NodeID]bool)
	f.partitionB = make(map[raft.NodeID]bool)
	for _, id := range groupA {
		f.partitionA[id] = true
	}
	for _, id := range groupB {
		f.partitionB[id] = true
	}
}

func (f *FaultInjector) HealPartition() {
	f.partitionA = nil
	f.partitionB = nil
}

func (f *FaultInjector) ShouldDrop(from, to raft.NodeID, msg raft.RPCMessage) bool {
	if f.partitionA != nil && f.partitionB != nil {
		fromA, toA := f.partitionA[from], f.partitionA[to]
		fromB, toB := f.partitionB[from], f.partitionB[to]
		if (fromA && toB) || (fromB && toA) {
			return true
		}
	}
	return f.rng.Float64() < f.dropRate
}