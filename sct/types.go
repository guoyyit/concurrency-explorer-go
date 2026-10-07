// Package sct provides systematic concurrency testing: deterministic
// exploration of thread interleavings for programs written against
// controlled concurrency primitives.
//
// A program under test is a func(*Env) that uses only the controlled
// primitives of this package (Mutex, Chan, WaitGroup, Var, Assert, Go).
// Given the same seed and the same program, Explore always produces the
// same conclusions and the same schedule sequences.
package sct

import (
	"fmt"
	"strings"
	"time"
)

// ThreadID identifies a controlled thread. Thread 1 is the main thread.
type ThreadID int

// OpKind is the kind of a controlled operation. Every controlled
// operation is a scheduling point.
type OpKind int

const (
	OpNewRes OpKind = iota
	OpYield
	OpSpawn
	OpLock
	OpUnlock
	OpSend
	OpRecv
	OpClose
	OpWGAdd
	OpWGDone
	OpWGWait
	OpRead
	OpWrite
	OpAssert
	OpDone
	OpPanic
)

var opKindNames = map[OpKind]string{
	OpNewRes: "newres",
	OpYield:  "yield",
	OpSpawn:  "spawn",
	OpLock:   "lock",
	OpUnlock: "unlock",
	OpSend:   "send",
	OpRecv:   "recv",
	OpClose:  "close",
	OpWGAdd:  "wg.add",
	OpWGDone: "wg.done",
	OpWGWait: "wg.wait",
	OpRead:   "read",
	OpWrite:  "write",
	OpAssert: "assert",
	OpDone:   "done",
	OpPanic:  "panic",
}

func (k OpKind) String() string {
	if s, ok := opKindNames[k]; ok {
		return s
	}
	return fmt.Sprintf("op(%d)", int(k))
}

// ResourceID identifies a controlled resource (mutex, channel,
// wait group or tracked variable).
type ResourceID int

// resKind classifies controlled resources.
type resKind int

const (
	resMutex resKind = iota
	resChan
	resWaitGroup
	resVar
)

// resSpec describes a resource to be created by the scheduler.
type resSpec struct {
	kind     resKind
	capacity int
	name     string
}

// assertArg carries an assertion's outcome and message.
type assertArg struct {
	ok  bool
	msg string
}

// Op describes a pending controlled operation posted by a thread.
type Op struct {
	Kind OpKind
	Res  ResourceID
	Arg  any
	Site string // caller location "file:line"
}

// opResult is the scheduler's answer to a posted op.
type opResult struct {
	val any
}

// Event is one executed step of a run.
type Event struct {
	Step   int
	Thread ThreadID
	Op     OpKind
	Detail string
}

func (e Event) String() string {
	return fmt.Sprintf("[%04d] T%d %-8s %s", e.Step, int(e.Thread), e.Op, e.Detail)
}

// Schedule is the sequence of thread choices that fully determines a run.
type Schedule []ThreadID

func (s Schedule) String() string {
	parts := make([]string, len(s))
	for i, t := range s {
		parts[i] = fmt.Sprint(int(t))
	}
	return strings.Join(parts, ",")
}

// VerdictKind classifies the outcome of a single run.
type VerdictKind int

const (
	VerdictPass VerdictKind = iota
	VerdictAssertFailed
	VerdictPanic
	VerdictDeadlock
	VerdictNonTerminating
	VerdictError
)

func (v VerdictKind) String() string {
	switch v {
	case VerdictPass:
		return "pass"
	case VerdictAssertFailed:
		return "assertion failed"
	case VerdictPanic:
		return "panic"
	case VerdictDeadlock:
		return "deadlock"
	case VerdictNonTerminating:
		return "non-terminating"
	case VerdictError:
		return "error"
	}
	return fmt.Sprintf("verdict(%d)", int(v))
}

// WaitEdge describes one blocked thread and the resource it waits on.
// HeldBy is >= 0 when the resource is a mutex currently owned.
type WaitEdge struct {
	Thread   ThreadID
	Resource string
	HeldBy   ThreadID
}

// AccessRef pinpoints one memory access in a run.
type AccessRef struct {
	Step   int
	Thread ThreadID
	Write  bool
	Site   string
}

func (a AccessRef) String() string {
	rw := "read"
	if a.Write {
		rw = "write"
	}
	return fmt.Sprintf("%s by T%d at step %d (%s)", rw, int(a.Thread), a.Step, a.Site)
}

// Race describes a detected data race together with a witness schedule.
type Race struct {
	Location string
	First    AccessRef
	Second   AccessRef
	Schedule Schedule
}

func (r Race) String() string {
	return fmt.Sprintf("data race on %s:\n    %s\n    %s\n  witness schedule: %s",
		r.Location, r.First, r.Second, r.Schedule)
}

// RunResult is the outcome of one complete execution.
type RunResult struct {
	Verdict   VerdictKind
	Message   string
	Schedule  Schedule
	Events    []Event
	Races     []Race
	WaitGraph []WaitEdge

	// Occurrences counts how many explored runs produced this same
	// (verdict, message) outcome. Only meaningful inside Report.Failures.
	Occurrences int

	enabled [][]ThreadID // enabled thread set at each step (exploration aid)
}

// Failed reports whether the run ended in anything other than VerdictPass.
func (r *RunResult) Failed() bool { return r.Verdict != VerdictPass }

// Config controls exploration and replay.
type Config struct {
	Seed         int64         // random seed steering schedule enumeration
	MaxSchedules int           // bound on explored schedules; 0 = unlimited
	MaxSteps     int           // per-run step bound; 0 = default 10000
	Timeout      time.Duration // wall-clock bound for Explore; 0 = none
	MaxThreads   int           // bound on live+finished threads; 0 = default 64
}

func (c Config) withDefaults() Config {
	if c.MaxSteps <= 0 {
		c.MaxSteps = 10000
	}
	if c.MaxThreads <= 0 {
		c.MaxThreads = 64
	}
	return c
}

// Stats summarizes exploration effort.
type Stats struct {
	SchedulesExplored int
	Transitions       int
	PendingPrefixes   int // schedules known but not yet explored
}

// Report is the aggregate outcome of Explore. It is built once, after
// the exploration loop ends, from fully completed runs only, so an
// interrupted exploration never yields contradictory partial results.
type Report struct {
	Complete         bool   // false when a budget stopped exploration early
	IncompleteReason string // human-readable uncovered-range description
	Stats            Stats
	Failures         []RunResult // first witness per distinct (verdict, message)
	Races            []Race      // first witness per distinct (location, sites)
}
