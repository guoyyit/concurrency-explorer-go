package explorer

// Schedule is the sequence of thread ids chosen at each scheduling step.
// Replaying a Schedule reproduces the exact same interleaving.
type Schedule []int

// Event records one granted scheduling step.
type Event struct {
	Step     int
	ThreadID int
	Op       OpKind
	Res      string
	CallSite string
}

// Trace is the ordered list of events of one run.
type Trace struct {
	Events []Event
}

// frame is one node of the systematic search stack: the enabled thread
// ids at that depth and the index of the choice to try next.
type frame struct {
	enabled []int
	next    int
}

// runOutcome is the result of executing a single schedule.
type runOutcome struct {
	verdict  Verdict
	schedule Schedule
	events   []Event
	races    []RaceReport
	failure  *FailureReport
	deadlock *DeadlockReport
	frames   []frame
	note     string
	diverged bool
}
