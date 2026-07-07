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

func (f *FaultInjector) ShouldDrop(from, to raft.NodeID, msg raft.RPCMessage) bool {
	return f.rng.Float64() < f.dropRate
}