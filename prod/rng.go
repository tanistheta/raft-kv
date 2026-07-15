package prod

import "math/rand"

// RealRNG implements raft.RNG using math/rand's global source.
type RealRNG struct{}

func (RealRNG) Intn(n int) int {
	return rand.Intn(n)
}