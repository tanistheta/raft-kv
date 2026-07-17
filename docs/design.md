# Design

## Goal

I built this to actually understand Raft by implementing it, not by reading the paper
and nodding along. The core design decision everything else follows from: `raft/`
contains the consensus algorithm and nothing else. It never touches a real socket, a
real clock, or real randomness directly. Everything it needs from the outside world
comes through four interfaces in `raft/interfaces.go`:

- `Clock` - `Now`, `After`, `AfterFunc`
- `Network` - `Send`, `Register`
- `Storage` - append/read/truncate the log, save/load persistent state
- `RNG` - `Intn`

`raft.Node` is built against these, never against `time.Now()` or `net.Conn` or
`math/rand` directly. That one decision is what makes both the DST harness and the
Maelstrom integration possible without forking the algorithm: the same `raft.Node` code
runs in a deterministic simulation, in an in-process integration test, and as a real OS
process, and the only thing that changes between them is which concrete type gets
plugged into each interface.

## Deterministic simulation testing (`sim/`)

`sim/` implements all four interfaces over a single-threaded, seeded, virtual-time
event loop (`sim/scheduler.go`): a min-heap of `(time, sequence, callback)` events, with
`sequence` breaking ties so that replaying the same seed always processes events in the
same order. `sim.Network` and `sim.FaultInjector` sit on top of that and can drop
messages, delay them, or partition the cluster into two groups, all driven by the same
seeded `math/rand` source the run started with. Given a seed, a run is exactly
reproducible: `go run ./cmd/sim -seed 42` replays the exact sequence of elections,
messages, drops, and partitions that seed produced on any machine, which is what made
tracking down bugs like the zombie election-timer (`docs/bugs.md`) tractable at all.
This is the payoff of the interface boundary: none of this fault injection or replay
logic lives in `raft/`, or could accidentally leak into it.

`sim/workload.go` drives a synthetic client against the simulated cluster (write/read/cas
against whichever node currently claims to be leader) and records every operation as a
`checker.Op` with `Start`/`End` timestamps and which node served it (`ServedBy`).
`checker/linearizability.go` then checks that recorded history against the real-time
constraints linearizability requires. `cmd/sim/main.go` wires a seed, a fault profile,
and a duration into one run and reports whether the resulting history was linearizable;
`docs/results.md` has the numbers from sweeping 2000 seeds this way.

## From simulation to a real process (`maelstrom/`)

The same `raft.Node` also runs as a real OS process speaking Maelstrom's JSON-over-
stdin/stdout protocol, via a different set of concrete implementations:

- `maelstrom.NetworkAdapter` implements `raft.Network` over a real Maelstrom node's
  message transport instead of `sim.Network`'s in-memory routing.
- `prod.RealClock` implements `raft.Clock` over real wall-clock time and `time.AfterFunc`,
  guarded by the same mutex that guards every other path into the `raft.Node` (see its
  doc comment; `raft.Node` has no locking of its own by design, since `sim/` never
  needed any).
- `prod.RealRNG` implements `raft.RNG` over `math/rand`'s global source instead of a
  seeded one, since a real deployment doesn't need or want reproducible randomness.
- Storage is still `sim.MemStorage` for now, not a real durable implementation. That's a
  known, tracked gap: a real Maelstrom process currently loses its log if it's actually
  killed and restarted, which is why Phase 3 validation so far only covers `--nemesis
  partition`, not crash nemeses.

`maelstrom.ClientHandler` and `maelstrom.Router` are the two pieces with no equivalent
in `sim/`: they exist because a real Maelstrom client speaks a different protocol
(`read`/`write`/`cas` JSON requests, arbitrary JSON scalars as keys/values) than
`sim/workload.go`'s synthetic Go-level client does, so something has to translate
between the two and decide, per incoming message, whether it's a Raft RPC (goes to
`NetworkAdapter`) or a client op (goes to `ClientHandler`). That's `Router.Dispatch`.

`maelstrom.RunProcess` (`maelstrom/process.go`) is the actual assembly point: it waits
for Maelstrom's `init` message to learn this node's ID and its peers (see `Node.OnInit`
in `protocol.go`), then builds one `raft.Node` plus its `NetworkAdapter`,
`ClientHandler`, and `Router`, exactly mirroring what
`TestClusterServesWriteReadCASOverMaelstromTransport` builds for three in-process nodes,
just wired to real stdin/stdout instead of in-memory pipes. `cmd/maelstrom/main.go` is
deliberately just a call into that function: the wiring lives in the library package,
where it can be read and eventually tested alongside the rest of `maelstrom/`, not
buried in an untestable `main`.

## What I'd still change

The propose/pending/resolve logic that answers "has this write committed yet" currently
lives in `maelstrom/client.go`, coupled to the Maelstrom wire format. It's the same
logic `sim/workload.go` has to do for its own synthetic client, duplicated rather than
shared, because there's no transport-agnostic `KVStore` type yet for both to sit on top
of. `kv/store.go` is the placeholder for that refactor.