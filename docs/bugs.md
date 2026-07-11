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