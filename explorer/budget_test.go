package explorer_test

import (
	"testing"
	"time"

	"github.com/guoyyit/concurrency-explorer-go/explorer"
	"github.com/guoyyit/concurrency-explorer-go/syncx"
)

func TestScheduleBudgetTruncates(t *testing.T) {
	// racyProg has far more than 3 schedules.
	full := explorer.Explore(racyProg, explorer.Options{})
	if full.SchedulesExplored <= 3 {
		t.Fatalf("test premise broken: only %d schedules", full.SchedulesExplored)
	}
	rep := explorer.Explore(racyProg, explorer.Options{MaxSchedules: 3})
	if rep.SchedulesExplored != 3 {
		t.Fatalf("expected exactly 3 schedules, got %d", rep.SchedulesExplored)
	}
	if rep.Complete {
		t.Fatal("report must not claim completeness")
	}
	if !rep.Truncated || rep.TruncatedReason == "" {
		t.Fatal("expected truncation with reason")
	}
}

func TestTimeoutTruncates(t *testing.T) {
	rep := explorer.Explore(racyProg, explorer.Options{Timeout: time.Nanosecond})
	if rep.Complete {
		t.Fatal("report must not claim completeness after timeout")
	}
	if !rep.Truncated {
		t.Fatal("expected truncated report")
	}
}

func TestThreadLimitReported(t *testing.T) {
	prog := func(env *explorer.Env) {
		wg := syncx.NewWaitGroup(env)
		for i := 0; i < 3; i++ {
			wg.Add(1)
			syncx.Go(env, func() { wg.Done() })
		}
		wg.Wait()
	}
	rep := explorer.Explore(prog, explorer.Options{MaxThreads: 2})
	if rep.LimitExceeded == 0 {
		t.Fatal("expected thread-limit exceedances")
	}
	if rep.Complete {
		t.Fatal("coverage must be marked incomplete when the thread limit aborts schedules")
	}
	if len(rep.Notes) == 0 {
		t.Fatal("expected an explanatory note")
	}
}

func TestStepBudgetMarksNonTermination(t *testing.T) {
	prog := func(env *explorer.Env) {
		x := syncx.NewVar(env, 0)
		for i := 0; i < 100; i++ {
			x.Set(i)
		}
	}
	rep := explorer.Explore(prog, explorer.Options{MaxSteps: 10})
	if len(rep.NonTerminating) == 0 {
		t.Fatal("expected non-terminating verdict from step budget")
	}
	if len(rep.Deadlocks) != 0 {
		t.Fatal("step-budget exhaustion must not be reported as deadlock")
	}
}

func TestDeterministicAcrossRuns(t *testing.T) {
	opts := explorer.Options{}
	a := explorer.Explore(racyProg, opts)
	b := explorer.Explore(racyProg, opts)
	if a.SchedulesExplored != b.SchedulesExplored || a.StepsTotal != b.StepsTotal {
		t.Fatalf("coverage differs: %d/%d vs %d/%d",
			a.SchedulesExplored, a.StepsTotal, b.SchedulesExplored, b.StepsTotal)
	}
	if len(a.Races) != len(b.Races) || len(a.Failures) != len(b.Failures) {
		t.Fatal("defect counts differ")
	}
	for i := range a.Races {
		ra, rb := a.Races[i], b.Races[i]
		if ra.Resource != rb.Resource || ra.A != rb.A || ra.B != rb.B {
			t.Fatalf("race %d differs", i)
		}
		if len(ra.Schedule) != len(rb.Schedule) {
			t.Fatalf("race %d schedule length differs", i)
		}
		for j := range ra.Schedule {
			if ra.Schedule[j] != rb.Schedule[j] {
				t.Fatalf("race %d schedule differs at step %d", i, j)
			}
		}
	}
}

func TestRandomStrategyDeterministic(t *testing.T) {
	opts := explorer.Options{Strategy: explorer.StrategyRandom, Seed: 42, MaxSchedules: 50}
	a := explorer.Explore(racyProg, opts)
	b := explorer.Explore(racyProg, opts)
	if a.SchedulesExplored != b.SchedulesExplored || a.StepsTotal != b.StepsTotal {
		t.Fatal("random strategy not reproducible under the same seed")
	}
	if len(a.Races) == 0 {
		t.Fatal("random strategy should find the race")
	}
	if len(a.Races) != len(b.Races) {
		t.Fatal("random strategy race count not reproducible")
	}
}
