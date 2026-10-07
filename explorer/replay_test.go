package explorer_test

import (
	"testing"

	"github.com/guoyyit/concurrency-explorer-go/explorer"
	"github.com/guoyyit/concurrency-explorer-go/syncx"
)

// counterProg: a clean program whose schedules all terminate.
func counterProg(env *explorer.Env) {
	x := syncx.NewVar(env, 0)
	mu := syncx.NewMutex(env)
	wg := syncx.NewWaitGroup(env)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		syncx.Go(env, func() {
			mu.Lock()
			x.Set(x.Get() + 1)
			mu.Unlock()
			wg.Done()
		})
	}
	wg.Wait()
	syncx.Assert(env, x.Get() == 3, "all increments applied")
}

// anchorProg fails an assertion on its very last step, exposing the
// full schedule of an otherwise clean run.
func anchorProg(env *explorer.Env) {
	counterProg(env)
	syncx.Assert(env, false, "anchor")
}

func TestReplayCompletedSchedule(t *testing.T) {
	rep := explorer.Explore(anchorProg, explorer.Options{MaxSchedules: 1})
	if len(rep.Failures) != 1 {
		t.Fatalf("expected anchor failure, got %+v", rep.Failures)
	}
	sched := rep.Failures[0].Schedule

	res := explorer.Replay(anchorProg, sched, explorer.Options{})
	if res.Err != nil {
		t.Fatalf("replay diverged: %v", res.Err)
	}
	if res.Verdict != explorer.VerdictAssertFailed {
		t.Fatalf("verdict = %s", res.Verdict)
	}
	if len(res.Trace.Events) != len(sched) {
		t.Fatalf("trace length %d != schedule length %d", len(res.Trace.Events), len(sched))
	}
	for i, ev := range res.Trace.Events {
		if ev.ThreadID != sched[i] {
			t.Fatalf("step %d: trace thread %d != schedule thread %d", i, ev.ThreadID, sched[i])
		}
	}

	// Replaying twice must produce byte-identical traces (no new
	// interleavings introduced).
	res2 := explorer.Replay(anchorProg, sched, explorer.Options{})
	if res2.Err != nil {
		t.Fatalf("second replay diverged: %v", res2.Err)
	}
	if len(res.Trace.Events) != len(res2.Trace.Events) {
		t.Fatal("replays differ in length")
	}
	for i := range res.Trace.Events {
		if res.Trace.Events[i] != res2.Trace.Events[i] {
			t.Fatalf("replays differ at event %d", i)
		}
	}
}

func TestReplayRejectsInvalidSchedule(t *testing.T) {
	rep := explorer.Explore(anchorProg, explorer.Options{MaxSchedules: 1})
	if len(rep.Failures) != 1 {
		t.Fatalf("expected anchor failure")
	}
	sched := rep.Failures[0].Schedule

	// A proper prefix must be rejected (the program would continue).
	res := explorer.Replay(anchorProg, sched[:len(sched)-1], explorer.Options{})
	if res.Err == nil {
		t.Fatal("expected divergence error for truncated schedule")
	}

	// A schedule choosing a non-enabled thread must be rejected.
	bad := append(explorer.Schedule(nil), sched...)
	bad[0] = 999
	res = explorer.Replay(anchorProg, bad, explorer.Options{})
	if res.Err == nil {
		t.Fatal("expected divergence error for invalid thread choice")
	}
}
