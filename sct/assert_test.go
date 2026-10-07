package sct_test

import (
	"strings"
	"testing"

	"github.com/guoyyit/concurrency-explorer-go/sct"
)

// An assertion that fails only under specific interleavings must be
// reported with a reproducing schedule.
func TestAssertFailureFound(t *testing.T) {
	program := func(e *sct.Env) {
		ready := sct.NewVar[bool](e, "ready")
		e.Go(func(c *sct.Env) { ready.Set(c, true) })
		e.Go(func(c *sct.Env) {
			sct.Assert(c, ready.Get(c), "ready must be set before use")
		})
	}
	rep := sct.Explore(sct.Config{Seed: 7}, program)
	t.Log("\n" + rep.String())
	if len(rep.Failures) == 0 {
		t.Fatal("expected an assertion failure, got none")
	}
	f := rep.Failures[0]
	if f.Verdict != sct.VerdictAssertFailed {
		t.Fatalf("expected assert verdict, got %s", f.Verdict)
	}
	if !strings.Contains(f.Message, "ready must be set before use") {
		t.Errorf("failure message should carry the assertion text, got %q", f.Message)
	}
	if len(f.Schedule) == 0 {
		t.Fatal("failure must carry a reproducing schedule")
	}
}

// An assertion that cannot fail must yield a clean, complete report
// describing the explored range.
func TestAssertNeverFails(t *testing.T) {
	program := func(e *sct.Env) {
		x := sct.NewVar[int](e, "x")
		m := sct.NewMutex(e)
		e.Go(func(c *sct.Env) {
			m.Lock(c)
			x.Set(c, 1)
			m.Unlock(c)
		})
		m.Lock(e)
		sct.Assert(e, x.Get(e) >= 0, "x is always non-negative")
		m.Unlock(e)
	}
	rep := sct.Explore(sct.Config{Seed: 8}, program)
	t.Log("\n" + rep.String())
	if !rep.Complete {
		t.Errorf("exploration should be complete: %s", rep.IncompleteReason)
	}
	if !rep.Clean() {
		t.Errorf("expected clean report, got:\n%s", rep.String())
	}
	if rep.Stats.SchedulesExplored == 0 {
		t.Error("report should state the explored range (schedules > 0)")
	}
}

// A panic inside a thread must be located with a reproducing schedule.
func TestPanicLocated(t *testing.T) {
	program := func(e *sct.Env) {
		e.Go(func(c *sct.Env) {
			sct.Yield(c)
			panic("boom")
		})
		sct.Yield(e)
	}
	rep := sct.Explore(sct.Config{Seed: 9}, program)
	t.Log("\n" + rep.String())
	if len(rep.Failures) == 0 {
		t.Fatal("expected a panic failure")
	}
	f := rep.Failures[0]
	if f.Verdict != sct.VerdictPanic {
		t.Fatalf("expected panic verdict, got %s", f.Verdict)
	}
	if !strings.Contains(f.Message, "boom") {
		t.Errorf("panic message should be propagated, got %q", f.Message)
	}
	if len(f.Schedule) == 0 {
		t.Error("panic must carry a reproducing schedule")
	}
}
