package prod

import (
	"sync"
	"testing"
	"time"
)

// TryLock succeeding inside the callback would mean AfterFunc didn't hold
// the mutex while running it.
func TestRealClockAfterFuncHoldsMutexDuringCallback(t *testing.T) {
	var mu sync.Mutex
	clock := NewRealClock(&mu)

	done := make(chan bool, 1)
	clock.AfterFunc(5*time.Millisecond, func() {
		done <- mu.TryLock()
	})

	locked := <-done
	if locked {
		mu.Unlock()
		t.Errorf("TryLock succeeded inside callback: mutex was not held")
	}
}

func TestRealClockAfterFuncCallbackCanAcquireMutexAfterReturning(t *testing.T) {
	var mu sync.Mutex
	clock := NewRealClock(&mu)

	fired := make(chan struct{})
	clock.AfterFunc(5*time.Millisecond, func() {
		close(fired)
	})
	<-fired

	time.Sleep(5 * time.Millisecond)
	if !mu.TryLock() {
		t.Fatalf("mutex still held after callback returned")
	}
	mu.Unlock()
}

func TestRealClockNowReturnsCurrentTime(t *testing.T) {
	var mu sync.Mutex
	clock := NewRealClock(&mu)

	before := time.Now()
	got := clock.Now()
	after := time.Now()

	if got.Before(before) || got.After(after) {
		t.Errorf("Now() = %v, want between %v and %v", got, before, after)
	}
}

func TestRealClockAfterFiresChannel(t *testing.T) {
	var mu sync.Mutex
	clock := NewRealClock(&mu)

	select {
	case <-clock.After(5 * time.Millisecond):
	case <-time.After(200 * time.Millisecond):
		t.Fatal("After channel never fired")
	}
}