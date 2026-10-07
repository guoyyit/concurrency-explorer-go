package sct

import (
	"fmt"
	"math/rand"
	"time"
)

// Explore systematically enumerates schedules of program within the
// budgets of cfg and returns an aggregate report. Given the same cfg
// (including Seed) and the same program, the report is identical.
//
// The report is assembled once, after the exploration loop ends, from
// fully completed runs only; a budget stop never produces contradictory
// partial conclusions, it only marks the exploration incomplete.
func Explore(cfg Config, program func(*Env)) *Report {
	cfg = cfg.withDefaults()
	it := newIterator(cfg.Seed)
	rng := rand.New(rand.NewSource(cfg.Seed))

	rep := &Report{Complete: true}
	failureIdx := map[string]int{}
	raceIdx := map[string]int{}

	var deadline time.Time
	if cfg.Timeout > 0 {
		deadline = time.Now().Add(cfg.Timeout)
	}
	var stopReason string

	for stopReason == "" {
		prefix, ok := it.next()
		if !ok {
			break // space exhausted: exploration complete
		}
		if cfg.MaxSchedules > 0 && rep.Stats.SchedulesExplored >= cfg.MaxSchedules {
			stopReason = fmt.Sprintf("schedule budget exhausted (%d schedules)", cfg.MaxSchedules)
			break
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			stopReason = fmt.Sprintf("timeout after %s", cfg.Timeout)
			break
		}

		rt := newRuntime(cfg, prefix, false, rng)
		res := rt.run(program)

		rep.Stats.SchedulesExplored++
		rep.Stats.Transitions += len(res.Schedule)

		if res.Failed() {
			key := fmt.Sprintf("%d|%s", res.Verdict, res.Message)
			if i, seen := failureIdx[key]; seen {
				rep.Failures[i].Occurrences++
			} else {
				failureIdx[key] = len(rep.Failures)
				rep.Failures = append(rep.Failures, *res)
			}
		}
		for _, race := range res.Races {
			key := fmt.Sprintf("%s|%s|%s|%v|%v",
				race.Location, race.First.Site, race.Second.Site,
				race.First.Write, race.Second.Write)
			if _, seen := raceIdx[key]; !seen {
				raceIdx[key] = len(rep.Races)
				rep.Races = append(rep.Races, race)
			}
		}
		it.observe(res.Schedule, res.enabled)
	}

	rep.Stats.PendingPrefixes = it.pending()
	if stopReason != "" {
		rep.Complete = false
		rep.IncompleteReason = fmt.Sprintf("%s: explored %d schedules, %d known prefixes left unexplored; conclusions cover explored schedules only",
			stopReason, rep.Stats.SchedulesExplored, rep.Stats.PendingPrefixes)
	}
	return rep
}
