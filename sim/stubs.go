package sim

import (
	mathrand "math/rand"
	"time"

	"raft-kv/raft"
)

type StubStorage struct{}

func (StubStorage) AppendLog(entries []raft.LogEntry) error        { return nil }
func (StubStorage) ReadLog(fromIndex int) ([]raft.LogEntry, error) { return nil, nil }
func (StubStorage) LastLogIndex() (int, error)                    { return 0, nil }
func (StubStorage) TruncateLog(fromIndex int) error                { return nil }
func (StubStorage) SaveState(state raft.PersistentState) error     { return nil }
func (StubStorage) LoadState() (raft.PersistentState, error) {
	return raft.PersistentState{}, nil
}

type StubRNG struct{}

func (StubRNG) Intn(n int) int { return 0 }

type SeededRNG struct {
	r *mathrand.Rand
}

func NewSeededRNG(seed int64) *SeededRNG {
	return &SeededRNG{r: mathrand.New(mathrand.NewSource(seed))}
}

func (s *SeededRNG) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return s.r.Intn(n)
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }
func (RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}
func (RealClock) AfterFunc(d time.Duration, fn func()) {
	time.AfterFunc(d, fn)
}