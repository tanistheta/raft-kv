package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"raft-kv/sim"
)

func main() {
	seed := flag.Int64("seed", time.Now().UnixNano(), "RNG seed; same seed reproduces the exact same run")
	nodes := flag.Int("nodes", 5, "cluster size")
	duration := flag.Duration("duration", 10*time.Second, "virtual simulated duration")
	dropRate := flag.Float64("drop-rate", 0.05, "per-message drop probability")
	faults := flag.Bool("faults", true, "inject random partitions and node crash/restart")
	flag.Parse()

	cfg := sim.RunConfig{
		NumNodes:     *nodes,
		Duration:     *duration,
		DropRate:     *dropRate,
		TickEvery:    20 * time.Millisecond,
		EnableFaults: *faults,
	}

	res := sim.RunSimulation(*seed, cfg)

	fmt.Printf("seed=%d nodes=%d duration=%s ops=%d linearizable=%v\n",
		res.Seed, *nodes, *duration, len(res.History), res.Linear)

	if !res.Linear {
		fmt.Fprintf(os.Stderr, "VIOLATION: %s\nreproduce with: go run ./cmd/sim -seed %d\n", res.Violation, res.Seed)
		os.Exit(1)
	}
}
