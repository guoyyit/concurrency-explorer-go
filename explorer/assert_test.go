package explorer_test

import (
	"strings"
	"testing"

	"github.com/guoyyit/concurrency-explorer-go/explorer"
	"github.com/guoyyit/concurrency-explorer-go/syncx"
)

// assertProg: the assertion fails only in schedules where the writer
// runs before the reader.
func assertProg(env *explorer.Env) {
	x := syncx.NewVar(env, 0)
	wg := syncx.NewWaitGroup(env)
	wg.Add(2)
	syncx.Go(env, func() {
		x.Set(1)
		wg.Done()
	})
	syncx.Go(env, func() {
		syncx.Assert(env, x.Get() == 0, "x must still be 0")
		wg.Done()
	})
	wg.Wait()
}

// safeAssertProg: the assertion holds under every schedule.
func safeAssertProg(env *explorer.Env) {
	x := syncx.NewVar(env, 0)
	mu := syncx.NewMutex(env)
	wg := syncx.NewWaitGroup(env)
	wg.Add(2)
	syncx.Go(env, func() {
		mu.Lock()
		x.Set(1)
		mu.Unlock()
		wg.Done()
	})
	syncx.Go(env, func() {
		mu.Lock()
		syncx.Assert(env, x.Get() >= 0, "x is never negative")
		mu.Unlock()
		wg.Done()
	})
	wg.Wait()
}

// panicProg: a thread panics when it observes a specific interleaving.
func panicProg(env *explorer.Env) {
	x := syncx.NewVar(env, 0)
	wg := syncx.NewWaitGroup(env)
	wg.Add(2)
	syncx.Go(env, func() {
		x.Set(1)
		wg.Done()
	})
	syncx.Go(env, func() {
		if x.Get() == 1 {
			panic("boom")
		}
		wg.Done()
	})
	wg.Wait()
}

func TestAssertViolationFound(t *testing.T) {
	rep := explorer.Explore(assertProg, explorer.Options{})
	if len(rep.Failures) == 0 {
		t.Fatal("expected an assertion violation")
	}
	f := rep.Failures[0]
	if f.Verdict != explorer.VerdictAssertFailed {
		t.Fatalf("unexpected verdict %s", f.Verdict)
	}
	if f.Message != "x must still be 0" {
		t.Fatalf("unexpected message %q", f.Message)
	}
	if f.CallSite == "" {
		t.Error("missing call site")
	}
	if len(f.Schedule) == 0 {
		t.Fatal("missing failing schedule")
	}

	// The failing schedule must deterministically reproduce the failure.
	res := explorer.Replay(assertProg, f.Schedule, explorer.Options{})
	if res.Err != nil {
		t.Fatalf("replay diverged: %v", res.Err)
	}
	if res.Verdict != explorer.VerdictAssertFailed {
		t.Fatalf("replay verdict = %s, want %s", res.Verdict, explorer.VerdictAssertFailed)
	}
	if res.Failure == nil || res.Failure.Message != f.Message {
		t.Fatalf("replay failure mismatch: %+v", res.Failure)
	}
}

func TestUnreachableAssertReportedAsCovered(t *testing.T) {
	rep := explorer.Explore(safeAssertProg, explorer.Options{})
	if !rep.Complete {
		t.Fatalf("expected complete exploration: %s", rep.TruncatedReason)
	}
	if len(rep.Failures) != 0 {
		t.Fatalf("unexpected failures: %+v", rep.Failures)
	}
	// Complete means: every schedule within the budgets was explored
	// and none violated the assertion.
	if rep.SchedulesExplored == 0 {
		t.Fatal("no schedules explored")
	}
}

func TestPanicFoundAndReplayed(t *testing.T) {
	rep := explorer.Explore(panicProg, explorer.Options{})
	if len(rep.Failures) == 0 {
		t.Fatal("expected a panic report")
	}
	f := rep.Failures[0]
	if f.Verdict != explorer.VerdictPanic {
		t.Fatalf("unexpected verdict %s", f.Verdict)
	}
	if !strings.Contains(f.Message, "boom") {
		t.Fatalf("unexpected panic message %q", f.Message)
	}
	res := explorer.Replay(panicProg, f.Schedule, explorer.Options{})
	if res.Err != nil {
		t.Fatalf("replay diverged: %v", res.Err)
	}
	if res.Verdict != explorer.VerdictPanic {
		t.Fatalf("replay verdict = %s, want panic", res.Verdict)
	}
}
