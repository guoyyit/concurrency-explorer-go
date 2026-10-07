package sct

import (
	"fmt"
	"strings"
)

// Clean reports whether exploration found neither failures nor races.
func (r *Report) Clean() bool {
	return len(r.Failures) == 0 && len(r.Races) == 0
}

// String renders a human-readable, deterministic summary.
func (r *Report) String() string {
	var b strings.Builder
	if r.Complete {
		b.WriteString("exploration COMPLETE\n")
	} else {
		fmt.Fprintf(&b, "exploration INCOMPLETE: %s\n", r.IncompleteReason)
	}
	fmt.Fprintf(&b, "schedules explored: %d, transitions: %d, pending prefixes: %d\n",
		r.Stats.SchedulesExplored, r.Stats.Transitions, r.Stats.PendingPrefixes)
	if r.Clean() {
		b.WriteString("no defects found in explored schedules\n")
		return b.String()
	}
	for i := range r.Failures {
		f := &r.Failures[i]
		fmt.Fprintf(&b, "failure: %s: %s (occurrences: %d)\n", f.Verdict, f.Message, f.Occurrences)
		fmt.Fprintf(&b, "  reproducing schedule: %s\n", f.Schedule)
		if f.Verdict == VerdictDeadlock {
			for _, e := range f.WaitGraph {
				if e.HeldBy >= 0 {
					fmt.Fprintf(&b, "  wait: T%d -> %s (held by T%d)\n", e.Thread, e.Resource, e.HeldBy)
				} else {
					fmt.Fprintf(&b, "  wait: T%d -> %s\n", e.Thread, e.Resource)
				}
			}
		}
	}
	for _, race := range r.Races {
		b.WriteString(race.String())
		b.WriteString("\n")
	}
	return b.String()
}
