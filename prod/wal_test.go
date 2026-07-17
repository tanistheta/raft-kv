package prod

import (
	"os"
	"path/filepath"
	"testing"

	"raft-kv/raft"
)

func TestWALAppendAndReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer w.Close()

	entries := []raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("SET x=1")},
		{Term: 1, Index: 2, Command: []byte("SET y=2")},
	}
	if err := w.AppendLog(entries); err != nil {
		t.Fatalf("AppendLog: %v", err)
	}

	got, err := w.ReadLog(1)
	if err != nil {
		t.Fatalf("ReadLog: %v", err)
	}
	if len(got) != 2 || string(got[0].Command) != "SET x=1" || string(got[1].Command) != "SET y=2" {
		t.Fatalf("ReadLog = %+v, want the two appended entries", got)
	}

	last, err := w.LastLogIndex()
	if err != nil || last != 2 {
		t.Fatalf("LastLogIndex = %d, %v, want 2, nil", last, err)
	}
}

// TestWALSurvivesRestart is the actual point of this file existing: a new
// WAL instance pointed at the same directory - standing in for a real
// process restart - must recover exactly what was durably appended before.
func TestWALSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	w1, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL (first): %v", err)
	}
	entries := []raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("SET x=1")},
		{Term: 2, Index: 2, Command: []byte("SET y=2")},
		{Term: 2, Index: 3, Command: []byte("CAS x 1 9")},
	}
	if err := w1.AppendLog(entries); err != nil {
		t.Fatalf("AppendLog: %v", err)
	}
	if err := w1.SaveState(raft.PersistentState{CurrentTerm: 2, VotedFor: "n2"}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// "Restart": a fresh WAL value, same directory, nothing carried over
	// in memory from w1.
	w2, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL (second, after restart): %v", err)
	}
	defer w2.Close()

	got, err := w2.ReadLog(1)
	if err != nil {
		t.Fatalf("ReadLog after restart: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("ReadLog after restart returned %d entries, want 3", len(got))
	}
	for i, e := range entries {
		if got[i].Term != e.Term || got[i].Index != e.Index || string(got[i].Command) != string(e.Command) {
			t.Errorf("entry %d after restart = %+v, want %+v", i, got[i], e)
		}
	}

	state, err := w2.LoadState()
	if err != nil {
		t.Fatalf("LoadState after restart: %v", err)
	}
	if state.CurrentTerm != 2 || state.VotedFor != "n2" {
		t.Errorf("LoadState after restart = %+v, want {CurrentTerm:2 VotedFor:n2}", state)
	}
}

func TestWALTruncateThenAppendSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	w1, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	w1.AppendLog([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("SET x=1")},
		{Term: 1, Index: 2, Command: []byte("SET y=2")},
		{Term: 1, Index: 3, Command: []byte("SET z=3")}, // will be truncated away
	})
	if err := w1.TruncateLog(3); err != nil { // drop index 3 onward
		t.Fatalf("TruncateLog: %v", err)
	}
	if err := w1.AppendLog([]raft.LogEntry{
		{Term: 2, Index: 3, Command: []byte("SET z=99")}, // conflicting entry, different term
	}); err != nil {
		t.Fatalf("AppendLog after truncate: %v", err)
	}
	w1.Close()

	w2, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL after restart: %v", err)
	}
	defer w2.Close()

	got, _ := w2.ReadLog(1)
	if len(got) != 3 {
		t.Fatalf("got %d entries after restart, want 3", len(got))
	}
	if got[2].Term != 2 || string(got[2].Command) != "SET z=99" {
		t.Errorf("entry 3 = %+v, want the post-truncate replacement, not the original", got[2])
	}
}

// TestWALRecoversFromTornWrite simulates the actual crash scenario the
// self-healing logic in recoverLog exists for: a process dies mid-fsync of
// its last log entry, leaving an incomplete final line in wal.log.
func TestWALRecoversFromTornWrite(t *testing.T) {
	dir := t.TempDir()
	w1, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	w1.AppendLog([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("SET x=1")},
		{Term: 1, Index: 2, Command: []byte("SET y=2")},
	})
	w1.Close()

	// Simulate a crash mid-write of a third entry: append a truncated,
	// unparseable JSON fragment with no trailing newline, exactly what
	// an interrupted os.File.Write of the third entry could leave behind.
	logPath := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("opening log to simulate torn write: %v", err)
	}
	if _, err := f.WriteString(`{"Term":1,"Index":3,"Comm`); err != nil {
		t.Fatalf("writing torn fragment: %v", err)
	}
	f.Close()

	w2, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL after torn write should self-heal, not error: %v", err)
	}
	defer w2.Close()

	got, _ := w2.ReadLog(1)
	if len(got) != 2 {
		t.Fatalf("got %d entries after torn-write recovery, want 2 (the torn 3rd entry should be discarded)", len(got))
	}

	// The WAL must also be writable after self-healing - the file should
	// have actually been truncated on disk, not just ignored in memory.
	if err := w2.AppendLog([]raft.LogEntry{{Term: 1, Index: 3, Command: []byte("SET z=3")}}); err != nil {
		t.Fatalf("AppendLog after self-heal: %v", err)
	}
	got, _ = w2.ReadLog(1)
	if len(got) != 3 || string(got[2].Command) != "SET z=3" {
		t.Fatalf("post-self-heal append didn't take: got %+v", got)
	}
}

// TestWALRejectsMidFileCorruption is the flip side of the torn-write test:
// a corrupt line that ISN'T the last one is real corruption, not a torn
// write, and must be reported rather than silently dropped.
func TestWALRejectsMidFileCorruption(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "wal.log")
	// A garbage first line followed by a valid second line - corruption
	// in the middle of the file, not at the tail.
	content := "not json at all\n{\"Term\":1,\"Index\":2,\"Command\":\"c2s9zg==\"}\n"
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatalf("writing corrupt fixture: %v", err)
	}

	if _, err := NewWAL(dir); err == nil {
		t.Fatal("NewWAL with mid-file corruption should return an error, got nil")
	}
}

func TestWALStateSurvivesTornWriteAttempt(t *testing.T) {
	// SaveState is always atomic (temp file + rename), so a state file
	// should never actually end up torn in practice. What IS worth
	// covering: a state file that's corrupt for some other reason (disk
	// corruption, manual edit) must fail loudly rather than silently
	// resetting to a zero PersistentState, since that could let a node
	// vote twice in a term it already voted in.
	dir := t.TempDir()
	statePath := filepath.Join(dir, "wal.state")
	if err := os.WriteFile(statePath, []byte("not valid json"), 0o644); err != nil {
		t.Fatalf("writing corrupt state fixture: %v", err)
	}

	if _, err := NewWAL(dir); err == nil {
		t.Fatal("NewWAL with corrupt state file should return an error, got nil")
	}
}

func TestWALFreshDirectoryStartsEmpty(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL on fresh dir: %v", err)
	}
	defer w.Close()

	last, _ := w.LastLogIndex()
	if last != 0 {
		t.Errorf("LastLogIndex on fresh WAL = %d, want 0", last)
	}
	state, _ := w.LoadState()
	if state != (raft.PersistentState{}) {
		t.Errorf("LoadState on fresh WAL = %+v, want zero value", state)
	}
}