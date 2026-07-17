# Results

## Phase 2: deterministic simulation testing (DST)

I ran a 2000-seed sweep of the full DST harness (`go run ./cmd/sim --seeds 2000`), with
partition and crash/restart faults enabled, checking every recorded operation history
against the linearizability checker in `checker/`.

**Headline result: zero linearizability violations across all 2000 seeds.**

Of those 2000 runs, 112 hit the stale-read guard in `sim/workload.go` (a freshly elected
leader with uncommitted inherited entries correctly refused to serve a read and retried,
rather than risk returning stale or missing data - see the "stale/missing reads" entry
in `docs/bugs.md`). That guard exists because of a real bug the sweep found; with it in
place, none of those 112 cases produced an incorrect read.

Four bugs surfaced by seeded runs during Phase 1 and 2, each with the reproducing seed
and fix recorded in `docs/bugs.md`: a zombie election timer firing after a simulated
crash, term/vote updates not persisted on the AppendEntries path, a freshly elected
leader serving stale reads, and a test harness bug that falsely marked a committed write
as abandoned.

## Phase 3: Maelstrom external validation

Building the Maelstrom transport, network adapter, real clock, client-op handler, and
router (all in `maelstrom/`) was validated first with an in-process 3-node integration
test (`TestClusterServesWriteReadCASOverMaelstromTransport`) that runs real leader
election and a full write/read/cas/cas-mismatch sequence over the actual protocol and
network code, not the DST simulator. Clean with the race detector on. Two real
concurrency bugs surfaced while building that stack (a missing mutex in message dispatch,
and a bus deadlock between two nodes messaging each other at once) that the
single-threaded DST simulator cannot find by construction - see `docs/bugs.md`.

That integration test is still a proxy: three `raft.Node`s in one Go test binary talking
over in-memory pipes I built. The actual external, adversarial check is running the real
`maelstrom` tool against a real compiled binary (`cmd/maelstrom/main.go`) as a real OS
process, and that's what this section covers.

**First real run found a genuine bug, not a Raft bug.** Every client op timed out and the
run failed outright, because `maelstrom`'s lin-kv workload sends integer keys and values
(`{"key":0,"value":4}`), and `clientBody` assumed Go strings, so every op silently failed
to decode and was dropped. Neither the DST harness nor the in-process integration test
exercises real JSON decoding, so nothing before this had a chance to catch it. Full
writeup and fix in `docs/bugs.md`, "Every client op silently dropped against real
Maelstrom."

**After the fix, `maelstrom test -w lin-kv` passes clean.** Two representative runs:

| Run | Nodes | Duration | Nemesis | Ops | OK | Failed | Result |
|---|---|---|---|---|---|---|---|
| No-fault | 3 | 30s | none | 592 | 255 | 337 | `:valid? true`, no anomalies |
| Partitions | 5 | 60s | `partition`, every 8s | 906 | 92 | 813 | `:valid? true`, no anomalies |

The partition run's actual Jepsen output (the verdict file, the Lamport-style timeline
visualization, latency and throughput plots under the partition schedule) is archived in
`docs/maelstrom-artifacts/` rather than just described here - see that folder's README
for what each file is and how to reproduce it.

Both runs: `:valid? true` at the top level, the linearizability checker's `:valid? true`
for every key with `:pending []` or fully-resolved configs, and Maelstrom prints
"Everything looks good!". The partition run is the actually adversarial one: `maelstrom`
injects real network partitions between real OS processes on a schedule I don't control,
and the recorded history still checks out as linearizable.

**Why the failed-op counts are high, and why that's expected, not a bug.** Only the
current leader can serve reads/writes/cas; a request routed to a follower gets
`temporarily-unavailable` and Maelstrom's default lin-kv client does not automatically
retry against a different node (there's no redirect message in the lin-kv protocol
either). With 3-5 nodes and one leader, a request has at best a 1-in-N chance of landing
on the leader per attempt, so a low OK-fraction is exactly what an unmodified Raft
KV store looks like under this workload's client. This is a real limitation worth noting
if I extend this further (a client-side retry loop or a redirect-style hint would raise
availability substantially) but it's not something the linearizability checker penalizes,
and it's not evidence of a correctness problem.

**What Phase 3 still doesn't cover:** crash/restart nemeses at the Maelstrom level (the
node uses `sim.MemStorage`, so a real killed-and-restarted process currently loses its
log - the durable-storage gap tracked as open work), and the `kv/store.go` refactor to
pull propose/pending/resolve logic out of `maelstrom/client.go` into a transport-agnostic
store.

## Reproducing

```bash
go build -o maelstrom-node ./cmd/maelstrom
./maelstrom test -w lin-kv --bin /path/to/maelstrom-node --nodes n1,n2,n3 \
  --time-limit 30 --rate 20 --concurrency 4n
```

Add `--nemesis partition --nemesis-interval 8` for the adversarial run.