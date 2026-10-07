package explorer

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
)

// Explore executes prog under many schedules and aggregates defects into
// one Report. Given the same Options (including Seed) and the same prog,
// the report is identical, except that a Timeout may reduce coverage.
// The Report is built as a whole and only returned at the end, so an
// interruption never yields contradictory partial conclusions.
func Explore(prog func(*Env), opts Options) (rep Report) {
	opts = opts.withDefaults()
	rep.Complete = true
	defer func() {
		if r := recover(); r != nil {
			rep.Complete = false
			rep.Truncated = true
			rep.TruncatedReason = fmt.Sprintf("internal error: %v", r)
		}
	}()
	var deadline time.Time
	if opts.Timeout > 0 {
		deadline = time.Now().Add(opts.Timeout)
	}
	agg := newAggregator()
	if opts.Strategy == StrategyRandom {
		exploreRandom(prog, opts, deadline, agg)
	} else {
		exploreSystematic(prog, opts, deadline, agg)
	}
	rep = agg.report()
	if rep.Truncated {
		rep.Complete = false
	}
	return rep
}

// aggregator merges per-schedule outcomes into one consistent report.
type aggregator struct {
	rep          Report
	raceKeys     map[string]bool
	failureKeys  map[string]bool
	deadlockKeys map[string]bool
	nonTermSeen  int
}

func newAggregator() *aggregator {
	return &aggregator{
		rep:          Report{Complete: true},
		raceKeys:     map[string]bool{},
		failureKeys:  map[string]bool{},
		deadlockKeys: map[string]bool{},
	}
}

func (a *aggregator) merge(out runOutcome) {
	a.rep.SchedulesExplored++
	a.rep.StepsTotal += len(out.events)
	for _, r := range out.races {
		key := raceKey(r)
		if !a.raceKeys[key] {
			a.raceKeys[key] = true
			a.rep.Races = append(a.rep.Races, r)
		}
	}
	if out.failure != nil {
		f := out.failure
		key := fmt.Sprintf("%s|%d|%s|%s", f.Verdict, f.ThreadID, f.CallSite, f.Message)
		if !a.failureKeys[key] {
			a.failureKeys[key] = true
			a.rep.Failures = append(a.rep.Failures, *f)
		}
	}
	if out.deadlock != nil {
		d := out.deadlock
		key := fmt.Sprintf("%v|%v", d.Participants, d.Resources)
		if !a.deadlockKeys[key] {
			a.deadlockKeys[key] = true
			a.rep.Deadlocks = append(a.rep.Deadlocks, *d)
		}
	}
	switch out.verdict {
	case VerdictNonTerminating:
		a.nonTermSeen++
		if len(a.rep.NonTerminating) < 3 {
			a.rep.NonTerminating = append(a.rep.NonTerminating, out.schedule)
		}
	case VerdictThreadLimit:
		a.rep.LimitExceeded++
		a.truncate("thread limit exceeded; affected schedules were aborted and their subtrees remain unexplored")
	}
	if out.note != "" {
		a.note(out.note)
	}
}

func (a *aggregator) note(n string) {
	for _, x := range a.rep.Notes {
		if x == n {
			return
		}
	}
	a.rep.Notes = append(a.rep.Notes, n)
}

func (a *aggregator) truncate(reason string) {
	a.rep.Truncated = true
	if a.rep.TruncatedReason == "" {
		a.rep.TruncatedReason = reason
	}
}

func (a *aggregator) report() Report {
	r := a.rep
	if a.nonTermSeen > len(r.NonTerminating) {
		r.Notes = append(r.Notes,
			fmt.Sprintf("%d non-terminating schedules found (only first %d recorded)",
				a.nonTermSeen, len(r.NonTerminating)))
	}
	if r.LimitExceeded > 0 {
		r.Notes = append(r.Notes,
			fmt.Sprintf("%d schedules aborted: thread limit exceeded", r.LimitExceeded))
	}
	sort.Slice(r.Races, func(i, j int) bool { return raceKey(r.Races[i]) < raceKey(r.Races[j]) })
	return r
}

func raceKey(r RaceReport) string {
	return fmt.Sprintf("%s|%d:%s:%t|%d:%s:%t", r.Resource,
		r.A.ThreadID, r.A.CallSite, r.A.Write,
		r.B.ThreadID, r.B.CallSite, r.B.Write)
}

func budgetLeft(opts Options, deadline time.Time, explored int) (string, bool) {
	if explored >= opts.MaxSchedules {
		return fmt.Sprintf("schedule budget exhausted (%d schedules)", opts.MaxSchedules), false
	}
	if !deadline.IsZero() && time.Now().After(deadline) {
		return "exploration timeout reached", false
	}
	return "", true
}

// exploreSystematic enumerates the schedule tree depth-first in
// deterministic thread-id order, replaying prefixes to backtrack.
func exploreSystematic(prog func(*Env), opts Options, deadline time.Time, agg *aggregator) {
	var frames []frame
	for {
		if reason, ok := budgetLeft(opts, deadline, agg.rep.SchedulesExplored); !ok {
			agg.truncate(reason + "; unexplored schedules remain")
			return
		}
		guide := make([]int, len(frames))
		for i, f := range frames {
			guide[i] = f.enabled[f.next]
		}
		out := runOnce(prog, opts, guide, false, nil)
		agg.merge(out)
		frames = out.frames
		// Backtrack to the deepest frame with an untried alternative.
		for len(frames) > 0 && frames[len(frames)-1].next+1 >= len(frames[len(frames)-1].enabled) {
			frames = frames[:len(frames)-1]
		}
		if len(frames) == 0 {
			return // search space exhausted: complete
		}
		frames[len(frames)-1].next++
	}
}

// exploreRandom samples schedules with a PRNG derived from Options.Seed
// and the schedule index, so the sampled set is reproducible.
func exploreRandom(prog func(*Env), opts Options, deadline time.Time, agg *aggregator) {
	seen := map[string]bool{}
	for i := 0; i < opts.MaxSchedules; i++ {
		if reason, ok := budgetLeft(opts, deadline, i); !ok {
			if i < opts.MaxSchedules {
				agg.truncate(reason + "; unexplored schedules remain")
			}
			return
		}
		rnd := rand.New(rand.NewSource(opts.Seed*1000003 + int64(i)))
		out := runOnce(prog, opts, nil, false, rnd)
		key := fmt.Sprint(out.schedule)
		if seen[key] {
			continue
		}
		seen[key] = true
		agg.merge(out)
	}
}
