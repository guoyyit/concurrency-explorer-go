package explorer

import "errors"

// ReplayResult is the outcome of deterministically replaying a schedule.
type ReplayResult struct {
	Verdict  Verdict
	Trace    Trace
	Schedule Schedule
	Races    []RaceReport
	Failure  *FailureReport
	Deadlock *DeadlockReport
	// Err is set when the schedule cannot be replayed exactly
	// (divergence or premature end).
	Err error
}

// Replay executes prog forcing exactly the choices of sched. It never
// changes scheduling decisions or introduces new interleavings: any
// divergence is reported via Err. Use the same Options budgets that
// produced the schedule (notably MaxSteps for non-terminating runs).
func Replay(prog func(*Env), sched Schedule, opts Options) ReplayResult {
	out := runOnce(prog, opts, []int(sched), true, nil)
	res := ReplayResult{
		Verdict:  out.verdict,
		Trace:    Trace{Events: out.events},
		Schedule: out.schedule,
		Races:    out.races,
		Failure:  out.failure,
		Deadlock: out.deadlock,
	}
	if out.diverged {
		res.Err = errors.New(out.note)
	}
	return res
}
