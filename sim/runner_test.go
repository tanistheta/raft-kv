package sim

import (
	"reflect"
	"testing"
)

func TestSeededRunIsDeterministic(t *testing.T) {
	cfg := DefaultConfig()
	first := RunSimulation(12345, cfg)
	second := RunSimulation(12345, cfg)

	if !reflect.DeepEqual(first.History, second.History) {
		t.Fatalf("seed 12345 produced different histories across runs:\nfirst:  %+v\nsecond: %+v", first.History, second.History)
	}
	if first.Linear != second.Linear {
		t.Fatalf("seed 12345 verdict differs across runs: %v vs %v", first.Linear, second.Linear)
	}
}

func TestManySeededRunsAreLinearizable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping bulk seeded-run sweep in -short mode")
	}
	const numSeeds = 5000
	cfg := DefaultConfig()
	cfg.Duration = 3 * cfg.TickEvery * 20

	failures := 0
	for seed := int64(0); seed < numSeeds; seed++ {
		res := RunSimulation(seed, cfg)
		if !res.Linear {
			failures++
			t.Errorf("seed %d: linearizability violation: %s", seed, res.Violation)
		}
	}
	if failures > 0 {
		t.Fatalf("%d/%d seeded runs violated linearizability", failures, numSeeds)
	}
}
