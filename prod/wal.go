package prod

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"raft-kv/raft"
)

// WAL is a durable implementation of raft.Storage backed by two files on
// disk in a directory given to NewWAL: an append-only log file (one
// JSON-encoded raft.LogEntry per line, fsync'd after every append) and a
// state file holding raft.PersistentState (CurrentTerm, VotedFor),
// rewritten atomically on every save since it changes on nearly every RPC
// and is small enough that "just rewrite the whole thing" is cheap.
//
// This exists to close a gap every other Storage in this repo has
// (sim.MemStorage, and what maelstrom/process.go currently uses): they're
// all in-memory, so a genuinely killed-and-restarted process loses its
// whole log. That's fine for Maelstrom's `lin-kv` workload (no crash
// nemeses) but not for Phase 4's actual exit criterion - "kill a
// container, cluster re-elects, no data loss" - which requires the
// restarted node to come back with the log and term/vote it had before.
//
// WAL keeps an in-memory copy of the log and state as its read path
// (ReadLog/LoadState/LastLogIndex never touch disk); every mutating call
// updates the on-disk file(s) before the in-memory copy, so a crash
// mid-write can only ever be caught by NewWAL's recovery on the next
// startup, never observed by a caller as "successfully saved" when it
// wasn't.
//
// Known limitation: writes are fsync'd, but the containing directory
// entry is not (no portable way to do that - directory fsync isn't
// meaningfully supported on Windows). On POSIX filesystems this is the
// one gap between this and a textbook-complete WAL; in practice, the
// rename in atomicWriteFile still lands correctly in virtually every real
// crash scenario short of "the whole disk's write cache lied," and this
// isn't claiming to survive that class of failure.
type WAL struct {
	mu sync.Mutex

	logPath   string
	statePath string

	log   []raft.LogEntry
	state raft.PersistentState

	logFile *os.File // kept open in append mode between AppendLog calls
}

// NewWAL opens (creating if needed) a WAL rooted at dir, recovering
// whatever log and state were durably saved there by a previous run.
func NewWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create dir %s: %w", dir, err)
	}
	w := &WAL{
		logPath:   filepath.Join(dir, "wal.log"),
		statePath: filepath.Join(dir, "wal.state"),
	}
	if err := w.recoverLog(); err != nil {
		return nil, fmt.Errorf("wal: recovering log: %w", err)
	}
	if err := w.recoverState(); err != nil {
		return nil, fmt.Errorf("wal: recovering state: %w", err)
	}
	f, err := os.OpenFile(w.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: opening log for append: %w", err)
	}
	w.logFile = f
	return w, nil
}

// recoverLog reads wal.log line by line. A line that fails to decode is
// tolerated in exactly one case: it's the very last line in the file,
// which almost certainly means the process crashed mid-write of that one
// entry. That's self-healed by truncating the file back to just before
// it - honest, since an entry that never finished its fsync'd write never
// really became durable either. A corrupt line anywhere else in the file
// is a real problem, not a torn write, and is reported as an error rather
// than silently discarded.
func (w *WAL) recoverLog() error {
	f, err := os.Open(w.logPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	var lines [][]byte
	var offsets []int64
	var offset int64

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		lines = append(lines, append([]byte(nil), line...))
		offsets = append(offsets, offset)
		offset += int64(len(line)) + 1 // +1 for the '\n' the scanner consumed
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	entries := make([]raft.LogEntry, 0, len(lines))
	for i, line := range lines {
		var entry raft.LogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			if i == len(lines)-1 {
				if terr := os.Truncate(w.logPath, offsets[i]); terr != nil {
					return fmt.Errorf("truncating torn write at line %d: %w", i, terr)
				}
				break
			}
			return fmt.Errorf("corrupt entry at line %d (not the last line, so this isn't a torn write): %w", i, err)
		}
		entries = append(entries, entry)
	}
	w.log = entries
	return nil
}

// recoverState loads wal.state. Unlike the log, a corrupt state file has
// no partial-recovery story: SaveState always writes atomically (temp
// file + fsync + rename), so on any real filesystem the file is either
// fully the old value or fully the new one - never torn. A decode failure
// here means real corruption, and silently falling back to a zero
// PersistentState would risk this node voting twice in a term it already
// voted in, which is exactly the kind of safety violation persistence
// exists to prevent. So this errors loudly instead.
func (w *WAL) recoverState() error {
	data, err := os.ReadFile(w.statePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var state raft.PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("state file exists but will not decode, refusing to guess: %w", err)
	}
	w.state = state
	return nil
}

// AppendLog implements raft.Storage.
func (w *WAL) AppendLog(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	var buf bytes.Buffer
	for _, e := range entries {
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("wal: marshal entry: %w", err)
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}
	if _, err := w.logFile.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("wal: write entries: %w", err)
	}
	if err := w.logFile.Sync(); err != nil {
		return fmt.Errorf("wal: fsync log: %w", err)
	}
	w.log = append(w.log, entries...)
	return nil
}

// ReadLog implements raft.Storage. Mirrors sim.MemStorage's semantics
// exactly (1-indexed, fromIndex<=0 treated as 1, out-of-range returns nil
// not an error) since raft/ is written against that contract.
func (w *WAL) ReadLog(fromIndex int) ([]raft.LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if fromIndex <= 0 {
		fromIndex = 1
	}
	if fromIndex > len(w.log) {
		return nil, nil
	}
	out := make([]raft.LogEntry, len(w.log)-(fromIndex-1))
	copy(out, w.log[fromIndex-1:])
	return out, nil
}

// LastLogIndex implements raft.Storage.
func (w *WAL) LastLogIndex() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.log), nil
}

// TruncateLog implements raft.Storage. Truncation only happens on a real
// log conflict (raft/replication.go), which is rare next to the
// steady-state append path, so this trades some write cost for a much
// simpler implementation: rewrite the whole log file from the kept
// entries rather than maintaining a byte-offset index just to support an
// in-place truncate.
func (w *WAL) TruncateLog(fromIndex int) error {
	if fromIndex <= 0 {
		return fmt.Errorf("wal: invalid truncate index %d", fromIndex)
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if fromIndex-1 >= len(w.log) {
		return nil
	}
	kept := w.log[:fromIndex-1]

	// Close the append handle before rewriting: Windows generally
	// refuses to rename over a file that still has an open handle
	// pointing at it, so this closes first, rewrites via
	// atomicWriteFile, then reopens - safe on both POSIX and Windows.
	if err := w.logFile.Close(); err != nil {
		return fmt.Errorf("wal: close log handle before truncate: %w", err)
	}

	var buf bytes.Buffer
	for _, e := range kept {
		data, err := json.Marshal(e)
		if err != nil {
			w.logFile, _ = os.OpenFile(w.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			return fmt.Errorf("wal: marshal entry during truncate: %w", err)
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}
	if err := atomicWriteFile(w.logPath, buf.Bytes()); err != nil {
		w.logFile, _ = os.OpenFile(w.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		return fmt.Errorf("wal: rewrite log during truncate: %w", err)
	}

	f, err := os.OpenFile(w.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: reopen log for append: %w", err)
	}
	w.logFile = f
	w.log = kept
	return nil
}

// SaveState implements raft.Storage.
func (w *WAL) SaveState(state raft.PersistentState) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("wal: marshal state: %w", err)
	}
	if err := atomicWriteFile(w.statePath, data); err != nil {
		return fmt.Errorf("wal: save state: %w", err)
	}
	w.state = state
	return nil
}

// LoadState implements raft.Storage.
func (w *WAL) LoadState() (raft.PersistentState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.state, nil
}

// Close releases the WAL's open file handle. Not part of raft.Storage;
// callers building a real node (cmd/node/main.go) should call this on
// shutdown.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.logFile.Close()
}

// atomicWriteFile writes data to path such that any reader - including
// this process on a future restart - only ever observes the fully-old or
// fully-new content, never a partial write: write to a temp file in the
// same directory, fsync it, close it, then rename over the real path.
// Rename is atomic on both POSIX filesystems and on Windows (Go's
// os.Rename uses MoveFileEx with MOVEFILE_REPLACE_EXISTING there).
func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename below succeeds

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}