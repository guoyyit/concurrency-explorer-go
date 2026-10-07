package explorer

// Verdict classifies the outcome of one schedule execution.
type Verdict string

const (
	VerdictCompleted      Verdict = "completed"
	VerdictAssertFailed   Verdict = "assertion-violation"
	VerdictPanic          Verdict = "panic"
	VerdictDeadlock       Verdict = "deadlock"
	VerdictNonTerminating Verdict = "non-terminating"
	VerdictThreadLimit    Verdict = "thread-limit-exceeded"
	VerdictInternalError  Verdict = "internal-error"
)

// Access describes one memory access participating in a data race.
type Access struct {
	ThreadID int
	Write    bool
	CallSite string
	Step     int
}

// RaceReport describes two conflicting, unordered accesses and the
// schedule that triggers them.
type RaceReport struct {
	Resource string
	A, B     Access
	Schedule Schedule
}

// FailureReport describes an assertion violation or a panic together
// with the schedule that reproduces it.
type FailureReport struct {
	Verdict  Verdict
	ThreadID int
	Message  string
	CallSite string
	Schedule Schedule
}

// WaitEdge is an edge of the wait-for graph. To == -1 means the thread
// waits on a resource with no identifiable owner thread.
type WaitEdge struct {
	From     int
	To       int
	Resource string
}

// DeadlockReport describes a detected deadlock: the involved threads,
// the resources they wait on, and the schedule that reproduces it.
type DeadlockReport struct {
	Participants []int
	Resources    []string
	Cycle        []WaitEdge
	Schedule     Schedule
}

// Report is the aggregated, consistent result of an exploration. It is
// only returned as a whole; partial results are never exposed.
type Report struct {
	// Complete is true only if the whole bounded search space was covered.
	Complete bool
	// Truncated is true if a budget/limit stopped exploration early.
	Truncated       bool
	TruncatedReason string

	SchedulesExplored int
	StepsTotal        int

	Races          []RaceReport
	Failures       []FailureReport
	Deadlocks      []DeadlockReport
	NonTerminating []Schedule

	// LimitExceeded counts schedules aborted due to the thread limit.
	LimitExceeded int
	Notes         []string
}

// HasDefects reports whether any defect was found.
func (r *Report) HasDefects() bool {
	return len(r.Races) > 0 || len(r.Failures) > 0 ||
		len(r.Deadlocks) > 0 || len(r.NonTerminating) > 0
}
