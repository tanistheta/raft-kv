# raft-kv

A from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go, built to actually understand distributed consensus by implementing it rather than just reading about it. The end goal is a replicated key-value store that stays consistent and available as long as a majority of nodes are up.

## Status: Phase 2 complete (~44% overall)

| Phase | Description | Status |
|---|---|---|
| 1 | Leader election, heartbeats, term handling | Done |
| 2 | Log replication (AppendEntries), apply loop, KV state machine | Done |
| 3 | External validation via [Maelstrom](https://github.com/jepsen-io/maelstrom) | Next |
| 4 | Snapshotting / log compaction | Planned |
| 5 | Cluster membership changes | Planned |

## What's implemented

- **Leader election**: randomized election timeouts, term-based voting, and split-vote handling
- **Log replication**: the full `AppendEntries` RPC path, including log matching, conflict resolution, and commit index advancement
- **Apply loop**: committed entries are applied in order to a replicated key-value state machine
- **Deterministic Simulation Testing (DST)**: a custom simulation harness that runs the cluster under controlled network faults (delays, partitions, drops) with seeded randomness for reproducibility

## Testing

The project leans on deterministic simulation testing rather than relying only on unit tests against real timers and sockets:

- 9 unit and integration tests passing across election and replication logic
- A 2000-seed sweep of the DST harness surfaced 112 stale minority reads and zero linearizability violations, which is the expected, correct behavior for a system that doesn't yet implement read-index or lease-based reads on the leader

Run the test suite:

```bash
go test ./...
```

Run the DST sweep:

```bash
go run ./cmd/sim --seeds 2000
```

## Project structure

```
raft-kv/
├── raft/          # core consensus: elections, log replication, RPC handling
├── kv/            # key-value state machine and apply loop
├── sim/           # deterministic simulation framework (clock, network, storage, RNG interfaces)
├── checker/       # linearizability checker for DST runs
├── cmd/sim/       # seeded DST runner and simulated client workload
├── docs/          # design notes and known issues (see docs/bugs.md)
└── go.mod
```

## Roadmap

The immediate next step is Phase 3: Maelstrom validation, running the cluster against Maelstrom's Jepsen-style workloads to get external, adversarial validation of linearizability beyond what the in-house DST harness covers. After that, snapshotting/log compaction and dynamic membership changes are the remaining pieces before this is a complete Raft implementation per the original paper.

## Motivation

This project exists to build real intuition for how consensus systems fail and recover, not just to pass the Raft paper's test cases, but to understand why stale reads happen, what a genuine linearizability violation would look like, and how simulation testing catches bugs that real-network testing would miss or make nondeterministic.

## License

MIT