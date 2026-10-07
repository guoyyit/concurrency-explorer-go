package sct_test

import (
	"strings"
	"testing"
	"time"

	"github.com/guoyyit/concurrency-explorer-go/sct"
)

// A wide program exhausts the schedule budget: the report must be
// marked incomplete and describe the uncovered range.
func TestScheduleBudgetExhausted(t *testing.T) {
	program := func(e *sct.Env) {
		for i := 0; i < 4; i++ {
			e.Go(func(c *sct.Env) {
				for j := 0; j < 3; j++ {
					sct.Yield(c)
				}
			})
		}
	}
	rep := sct.Explore(sct.Config{Seed: 31, MaxSchedules: 5}, program)
	t.Log("\n" + rep.String())
	if rep.Complete {
		t.Fatal("exploration must be incomplete when the budget stops it")
	}
	if rep.Stats.SchedulesExplored != 5 {
		t.Errorf("expected exactly 5 explored schedules, got %d", rep.Stats.SchedulesExplored)
	}
	if rep.Stats.PendingPrefixes == 0 {
		t.Error("pending prefixes must describe the uncovered range")
	}
	if !strings.Contains(rep.IncompleteReason, "budget") {
		t.Errorf("incomplete reason should name the budget, got %q", rep.IncompleteReason)
	}
	// A budget stop is not a defect.
	if len(rep.Failures) != 0 {
		t.Errorf("budget exhaustion must not fabricate failures: %+v", rep.Failures[0])
	}
}

// The same program with an adequate budget explores completely.
func TestScheduleBudgetAdequate(t *testing.T) {
	program := func(e *sct.Env) {
		e.Go(func(c *sct.Env) { sct.Yield(c) })
	}
	rep := sct.Explore(sct.Config{Seed: 32, MaxSchedules: 1000}, program)
	if !rep.Complete {
		t.Errorf("small program should explore completely: %s", rep.IncompleteReason)
	}
}

// The thread limit turns unbounded spawning into a bounded, reported
// error instead of an explosion.
func TestThreadLimit(t *testing.T) {
	program := func(e *sct.Env) {
		for i := 0; i < 10; i++ {
			e.Go(func(c *sct.Env) { sct.Yield(c) })
		}
	}
	rep := sct.Explore(sct.Config{Seed: 33, MaxThreads: 4}, program)
	t.Log("\n" + rep.String())
	if len(rep.Failures) == 0 {
		t.Fatal("expected a thread-limit failure")
	}
	f := rep.Failures[0]
	if f.Verdict != sct.VerdictError {
		t.Fatalf("expected VerdictError, got %s", f.Verdict)
	}
	if !strings.Contains(f.Message, "thread limit") {
		t.Errorf("message should name the thread limit, got %q", f.Message)
	}
}

// A near-zero timeout stops exploration immediately and marks the
// report incomplete without contradictions.
func TestTimeoutStopsExploration(t *testing.T) {
	program := func(e *sct.Env) {
		for i := 0; i < 4; i++ {
			e.Go(func(c *sct.Env) {
				for j := 0; j < 3; j++ {
					sct.Yield(c)
				}
			})
		}
	}
	rep := sct.Explore(sct.Config{Seed: 34, Timeout: time.Nanosecond}, program)
	t.Log("\n" + rep.String())
	if rep.Complete {
		t.Fatal("a near-zero timeout must stop exploration")
	}
	if !strings.Contains(rep.IncompleteReason, "timeout") {
		t.Errorf("incomplete reason should name the timeout, got %q", rep.IncompleteReason)
	}
	if rep.Stats.SchedulesExplored > 1 {
		t.Errorf("timeout should stop exploration almost immediately, explored %d", rep.Stats.SchedulesExplored)
	}
}

// The step budget is a per-run bound: a long-but-finite program is
// reported non-terminating only when it actually exceeds the budget.
func TestStepBudgetPerRun(t *testing.T) {
	program := func(e *sct.Env) {
		for i := 0; i < 20; i++ {
			sct.Yield(e)
		}
	}
	rep := sct.Explore(sct.Config{Seed: 35, MaxSteps: 10}, program)
	t.Log("\n" + rep.String())
	if len(rep.Failures) == 0 || rep.Failures[0].Verdict != sct.VerdictNonTerminating {
		t.Fatalf("expected non-terminating verdict, got:\n%s", rep.String())
	}

	rep = sct.Explore(sct.Config{Seed: 35, MaxSteps: 100}, program)
	if !rep.Clean() {
		t.Errorf("with adequate step budget the program must pass, got:\n%s", rep.String())
	}
}
