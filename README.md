# Maelstrom / Jepsen artifacts

Real output from `maelstrom test -w lin-kv` run against `cmd/maelstrom` on 2026-07-16.
This is the archived evidence for Phase 3's exit criteria ("green `lin-kv` run under
partitions, artifacts archived"), not just a terminal screenshot.

**Run parameters:** 5 nodes, 60 second time limit, request rate 15/s, concurrency 4n
(20 client threads), `--nemesis partition --nemesis-interval 8` (a random network
partition every 8 seconds throughout the run).

**Verdict:** `:valid? true`. 895 operations attempted (223 reads, 217 writes, 455 cas),
136 succeeded, 754 failed cleanly (mostly `temporarily-unavailable` from requests
routed to a non-leader node - expected, see `docs/results.md`), 5 came back
indeterminate (`:info`, almost certainly requests in flight when a partition cut the
connection - Jepsen's checker treats these conservatively rather than assuming
either outcome). Zero linearizability violations across all of it, under real,
adversarial, Maelstrom-injected network partitions.

## Files

- **`results.edn`** - the actual machine-readable verdict Jepsen produced: per-operation
  stats, the linearizability checker's output, availability numbers. This is the ground
  truth; everything in `docs/results.md`'s Phase 3 table is read off a file like this one.
- **`timeline.html`** - open in a browser. Jepsen's Lamport-diagram-style visualization
  of every operation's invocation and completion, ordered by real time, colored by
  outcome. This is the closest thing to actually watching the test run.
- **`latency-quantiles.png`**, **`latency-raw.png`** - request latency over the run.
  Visible latency spikes line up with the partition nemesis firing every 8s.
- **`rate.png`** - operation throughput over time, same partition-correlated pattern.

Not archived here (too large/noisy to be worth committing, but reproducible - see
below): `jepsen.log` (~190KB full run log), `messages.svg` (~3.5MB full message-flow
diagram), `net-journal/`, `node-logs/`, `history.edn`/`history.txt` (raw operation
history Jepsen checked).

## Reproducing

```bash
go build -o maelstrom-node ./cmd/maelstrom
./maelstrom test -w lin-kv --bin /path/to/maelstrom-node \
  --nodes n1,n2,n3,n4,n5 --time-limit 60 --rate 15 --concurrency 4n \
  --nemesis partition --nemesis-interval 8
```

Full output (including everything not archived here) lands in `store/lin-kv/latest/`
inside wherever you extracted Maelstrom.