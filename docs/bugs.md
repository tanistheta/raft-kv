# Bug Log

Every bug found during fault-injection DST, with the seed that reproduces it.

---

## 2026-07-10 - Zombie election-timer fires after node "crash"

**Found via:** `TestLeaderCrashAndRestartRecoversState`, seed 42 (`sim/crash_recovery_test.go`)

**Symptom:** After crashing the leader (`leader.Stop()` - before the fix, this method
didn't exist - plus `network.Unregister(leaderID)` and `delete(nodes, leaderID)`),
the leader's still-pending election-timer closure (scheduled earlier via
`Clock.AfterFunc`) fired anyway and called `StartElection()` on a node that should
have been dead. Removing the node from the test's `map[NodeID]*Node` does nothing to
the scheduler - the closure captured the `*Node` pointer directly, not the map entry,
so deleting the map entry doesn't cancel the scheduled work.

**Root cause:** No lifecycle guard existed to let a node's own pending timers know it
had crashed. The scheduler has no per-node cancellation API, so any closure scheduled
before a crash will still run.

**Fix:** Added `Node.Stopped bool` + `Node.Stop()`. `resetElectionTimer`'s callback now
checks `myGen == n.timerGen && n.Role != Leader && !n.Stopped` before acting, so every
pending timer closure self-cancels once `Stop()` is called, regardless of whether the
scheduler still fires it. (`raft/node.go`, commit `831d068`)

---

## 2026-07-10 - Term/vote updates in AppendEntries path never persisted

**Found via:** `TestLeaderCrashAndRestartRecoversState` + `TestRestartedNodeDoesNotDoubleVote`,
seeds 42 and 7 (`sim/crash_recovery_test.go`)

**Symptom:** `Storage.SaveState` was only called from the RequestVote path
(`raft/election.go`). Term/vote changes that happen via AppendEntries - e.g. a node
stepping down when it sees a higher term from a leader's heartbeat - stayed in memory
only. A node that updated its term purely through replication traffic and then crashed
would reload a stale term/vote on restart, opening the door to a double vote or a term
regression after recovery.

**Root cause:** Persistence calls were added ad hoc while building Phase 1 election
logic and never extended when `raft/replication.go` grew its own term-mutating code
paths.

**Fix:** Added `n.Storage.SaveState(...)` at both term-mutation points in
`replication.go` - `handleAppendEntries` and `handleAppendEntriesReply` - mirroring the
persistence discipline already in `election.go`. (`raft/replication.go`, commit `831d068`)

---

## 2026-07-11 - Freshly-elected leader serves stale/missing reads for already-durable keys

**Found via:** `TestManySeededRunsAreLinearizable` (`sim/runner_test.go`), first observed at
seed 226 with a 5-node cluster, `sim -seed 226 -duration 1.2s`.

**Symptom:** A `GET` served directly from the current leader's local `kv.StateMachine`
returned "not found" for a key that had already been committed (and read back
successfully elsewhere) earlier in the run. Instrumentation showed the leader (freshly
elected, term 5) had `CommitIndex=0` but `LastLogIndex=3` - it had inherited three
already-durable entries from a previous term but had not yet locally counted any of them
as committed.

**Root cause:** This is not a bug in `raft/` - it's the textbook reason real Raft KV
stores need a ReadIndex/lease-read protocol before serving reads. Per the Raft paper
§5.4.2, a leader can only advance `CommitIndex` by counting replica acks for entries **in
its own current term** (`advanceCommitIndex` in `raft/replication.go` skips any entry
whose `Term != n.CurrentTerm`). A brand-new leader that hasn't yet gotten its own entry
committed has a log full of already-safe entries that its local `applyCommitted()` hasn't
been authorized to apply yet. Reading local state without that check is unsafe.

**Fix:** `sim/workload.go`'s read path now refuses to serve a `GET` unless
`leader.CommitIndex >= leader.LastLogIndex()` - i.e. the leader has nothing outstanding
that it can't yet vouch for - and simply retries next tick otherwise, rather than
recording a false read into the linearizability history.

---

## 2026-07-11 - Client workload falsely "abandoned" writes that later committed for real

**Found via:** `TestManySeededRunsAreLinearizable`, seeds 3 and 46 (5-node cluster,
partition + crash/restart faults enabled).

**Symptom:** A `SET` whose leader was later deposed sometimes vanished from the recorded
op history even though the value was demonstrably applied later (a subsequent `GET`
returned it). The checker correctly flagged this as "a read observed a value with no
matching write in history" - a bug in the test harness's bookkeeping, not in `raft/`.

**Root cause:** Two mistakes stacked in `sim/workload.go`'s pending-write resolution:
(1) it tracked commit status against the node that was leader *at write time*, so once
that specific node crashed or fell behind, the write was never re-checked even after the
node restarted or a different node with the same log took over; (2) it treated "the
current leader's log is shorter than this entry's index" as proof the entry was
permanently lost. That's false - Raft only overwrites a log slot when something else is
actually appended there. A transient leader with a shorter log can be deposed before ever
writing anything new, and a *later* leader that still has the original entry (because
nobody ever overwrote that slot) can go on to commit it, exactly as seed 46 showed: leader
`N2` (term 4, log length 18) was replaced by leader `N4` (term 5) which still had entry 19
from the original term-1 leader and committed it ~200ms later.

**Fix:** `resolvePending` now always re-checks against whoever the *current* leader is
(not a frozen historical node), and only declares a write dead when it reads an actual
**conflicting** entry at that exact index (different term). A leader whose log simply
hasn't reached that index yet leaves the write pending rather than abandoning it.
(`sim/workload.go`)

---

## 2026-07-16 - Every client op silently dropped against real Maelstrom (string-typed key/value)

**Found via:** the first real `maelstrom test -w lin-kv --bin cmd/maelstrom` run, the DST
harness can't surface this by construction since it never speaks real JSON off a real
process's stdin.

**Symptom:** Every single client operation in the run timed out (`:net-timeout`), and the
run failed analysis outright - not a linearizability violation, but total unavailability.
`node-logs/*.log` (stderr) were empty: the process wasn't crashing, it just never replied.

**Root cause:** `clientBody` in `maelstrom/client.go` declared `Key`, `Value`, `From`,
`To` as Go `string` fields. Maelstrom's actual lin-kv generator sends integer keys and
values (`{"type":"write","key":0,"value":4}`), not strings. `json.Unmarshal`-ing a JSON
number into a Go `string` field fails, so `ClientHandler.Handle` returned `false` for
every op without logging anything, and the message was silently dropped by the router.
Nothing in the DST harness or the in-memory integration test exercises this path, since
both construct Go values directly and never round-trip through real JSON with a real
Maelstrom client.

**Fix:** `clientBody`'s four fields are now `json.RawMessage`, and a new
`canonicalToken` helper re-encodes whatever scalar arrives (number, string, bool, null)
as compact JSON text before it's used as a `kv.StateMachine` key or folded into a
`SET`/`CAS` command string. Reads echo the value back as raw JSON so a stored number
comes back as a number, not a re-quoted string. (`maelstrom/client.go`)

**Result after the fix:** `maelstrom test -w lin-kv` passes clean (`:valid? true`, no
anomalies) against 3 and 5 node clusters, both with no nemesis and with `--nemesis
partition` injecting real network partitions - see `docs/results.md` for the full
numbers.

---

## 2026-07-16 - The 2000-seed stale-read sweep was never actually being run

**Found via:** re-verifying everything with a full test run after the `kv/store.go`
refactor, specifically noticing `go test -v ./...` never printed
`TestStaleMinorityReadsStayLinearizable` in its output despite that test existing in the
tree.

**Symptom:** No symptom in the traditional sense - `go build`, `go vet`, and `go test
./...` all passed clean, every time, for as long as this file existed. That's exactly
what made this easy to miss: nothing was red.

**Root cause:** The file was named `sim/staleread_sweeptest.go` - missing the
underscore before `test.go`. Go's test tooling only treats a file as a test file if its
name ends in exactly `_test.go`; `sweeptest.go` doesn't match, so the file compiled as
ordinary `sim` package code (its `TestStaleMinorityReadsStayLinearizable` function is a
perfectly valid, exported, unused function; nothing about that is a compile error) and
`go test` never ran it, silently, since whenever the underscore was dropped. The
documented "112 stale minority reads across 2000 seeds, zero violations" figure had
never been mechanically re-verified since.

**Fix:** renamed the file to `sim/staleread_sweep_test.go`. Ran it for the first time as
an actual test: same result as documented, 112 stale-minority reads across 2000 seeds,
zero violations among them. The number was correct; the process that was supposed to
keep re-checking it had quietly stopped working.

**Lesson, not just a fix:** a test suite passing green doesn't mean every test in the
tree ran - it means every test file `go test` recognized ran. Worth periodically
checking `go test -v ./... | grep RUN` against what actually exists in `*_test.go` files
if a claimed number matters.