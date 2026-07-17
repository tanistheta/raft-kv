package sim

import (
	"testing"
	"time"
)

func TestStaleMinorityReadsStayLinearizable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stale-read sweep in -short mode")
	}
	cfg := DefaultConfig()
	cfg.TickEvery = 5 * time.Millisecond
	cfg.Duration = 2 * time.Second

	const numSeeds = 2000
	totalStaleReads := 0
	violationsAmongStale := 0

	for seed := int64(0); seed < numSeeds; seed++ {
		res := RunSimulation(seed, cfg)
		stale := FindStaleReads(res.History, res.Partitions)
		totalStaleReads += len(stale)
		if len(stale) > 0 && !res.Linear {
			violationsAmongStale++
			t.Errorf("seed %d: %d stale-minority read(s) present AND linearizability violated: %s",
				seed, len(stale), res.Violation)
		}
	}

	t.Logf("swept %d seeds, %d total stale-minority reads observed, %d runs with both stale reads and a violation",
		numSeeds, totalStaleReads, violationsAmongStale)
}