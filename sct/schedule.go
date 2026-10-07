package sct

import "math/rand"

// iterator enumerates schedules systematically: every observed run
// contributes, for each step, one new prefix per enabled-but-unchosen
// thread. Exploration is deterministic for a fixed seed.
type iterator struct {
	rng   *rand.Rand
	seen  map[string]struct{}
	stack []Schedule
}

func newIterator(seed int64) *iterator {
	it := &iterator{
		rng:  rand.New(rand.NewSource(seed)),
		seen: map[string]struct{}{},
	}
	// The empty prefix: a free run steered only by the runtime's rng.
	it.stack = append(it.stack, nil)
	it.seen[""] = struct{}{}
	return it
}

// next returns the next schedule prefix to explore.
func (it *iterator) next() (Schedule, bool) {
	n := len(it.stack)
	if n == 0 {
		return nil, false
	}
	s := it.stack[n-1]
	it.stack = it.stack[:n-1]
	return s, true
}

// observe records a completed run and derives new prefixes from it:
// for every step i and every thread u that was enabled but not chosen
// at i, the prefix sched[:i]+[u] is a new candidate schedule.
func (it *iterator) observe(sched Schedule, enabled [][]ThreadID) {
	var fresh []Schedule
	for i := 0; i < len(sched) && i < len(enabled); i++ {
		for _, u := range enabled[i] {
			if u == sched[i] {
				continue
			}
			cand := make(Schedule, i+1)
			copy(cand, sched[:i])
			cand[i] = u
			key := cand.String()
			if _, dup := it.seen[key]; dup {
				continue
			}
			it.seen[key] = struct{}{}
			fresh = append(fresh, cand)
		}
	}
	// Shuffle with the seeded rng so the exploration order depends on
	// the seed but stays fully deterministic for a fixed seed.
	it.rng.Shuffle(len(fresh), func(i, j int) {
		fresh[i], fresh[j] = fresh[j], fresh[i]
	})
	it.stack = append(it.stack, fresh...)
}

// pending returns the number of known-but-unexplored prefixes.
func (it *iterator) pending() int { return len(it.stack) }
