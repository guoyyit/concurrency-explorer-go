package sct_test

import (
	"testing"

	"github.com/guoyyit/concurrency-explorer-go/sct"
)

func assertProgram(e *sct.Env) {
	ready := sct.NewVar[bool](e, "ready")
	e.Go(func(c *sct.Env) { ready.Set(c, true) })
	e.Go(func(c *sct.Env) {
		sct.Assert(c, ready.Get(c), "ready must be set before use")
	})
}

// A reported failing schedule must replay to the same verdict and the
// same event sequence.
func TestReplayReproducesAssertFailure(t *testing.T) {
	rep := sct.Explore(sct.Config{Seed: 21}, assertProgram)
	if len(rep.Failures) == 0 {
		t.Fatal("expected a failure to replay")
	}
	f := rep.Failures[0]

	res := sct.Replay(assertProgram, f.Schedule)
	if res.Verdict != f.Verdict {
		t.Fatalf("replay verdict %s != explored verdict %s", res.Verdict, f.Verdict)
	}
	if res.Message != f.Message {
		t.Errorf("replay message %q != explored %q", res.Message, f.Message)
	}
	if len(res.Events) != len(f.Events) {
		t.Fatalf("replay produced %d events, exploration produced %d", len(res.Events), len(f.Events))
	}
	for i := range res.Events {
		if res.Events[i] != f.Events[i] {
			t.Fatalf("event %d diverged: replay %+v vs explored %+v", i, res.Events[i], f.Events[i])
		}
	}
	if res.Schedule.String() != f.Schedule.String() {
		t.Errorf("replay changed the schedule: %s != %s", res.Schedule, f.Schedule)
	}
}

// A race witness schedule must replay to the same race.
func TestReplayReproducesRace(t *testing.T) {
	program := func(e *sct.Env) {
		x := sct.NewVar[int](e, "x")
		e.Go(func(c *sct.Env) { x.Set(c, 1) })
		e.Go(func(c *sct.Env) { x.Set(c, 2) })
	}
	rep := sct.Explore(sct.Config{Seed: 22}, program)
	if len(rep.Races) == 0 {
		t.Fatal("expected a race to replay")
	}
	witness := rep.Races[0]
	res := sct.Replay(program, witness.Schedule)
	if len(res.Races) == 0 {
		t.Fatal("replay of the witness schedule did not reproduce the race")
	}
	got := res.Races[0]
	if got.Location != witness.Location || got.First != witness.First || got.Second != witness.Second {
		t.Errorf("replayed race differs:\nexplored: %s\nreplayed: %s", witness, got)
	}
}

// A deadlock schedule must replay to the same deadlock with the same
// wait graph.
func TestReplayReproducesDeadlock(t *testing.T) {
	program := func(e *sct.Env) {
		m1 := sct.NewMutex(e)
		m2 := sct.NewMutex(e)
		e.Go(func(c *sct.Env) {
			m1.Lock(c)
			m2.Lock(c)
			m2.Unlock(c)
			m1.Unlock(c)
		})
		e.Go(func(c *sct.Env) {
			m2.Lock(c)
			m1.Lock(c)
			m1.Unlock(c)
			m2.Unlock(c)
		})
	}
	rep := sct.Explore(sct.Config{Seed: 23}, program)
	var dl *sct.RunResult
	for i := range rep.Failures {
		if rep.Failures[i].Verdict == sct.VerdictDeadlock {
			dl = &rep.Failures[i]
		}
	}
	if dl == nil {
		t.Fatal("expected a deadlock to replay")
	}
	res := sct.Replay(program, dl.Schedule)
	if res.Verdict != sct.VerdictDeadlock {
		t.Fatalf("replay verdict %s, want deadlock", res.Verdict)
	}
	if len(res.WaitGraph) != len(dl.WaitGraph) {
		t.Fatalf("replay wait graph has %d edges, want %d", len(res.WaitGraph), len(dl.WaitGraph))
	}
	for i := range res.WaitGraph {
		if res.WaitGraph[i] != dl.WaitGraph[i] {
			t.Errorf("wait edge %d differs: %+v vs %+v", i, res.WaitGraph[i], dl.WaitGraph[i])
		}
	}
}

// Replaying a truncated schedule must not silently introduce new
// interleavings: it reports divergence instead.
func TestReplayRejectsTruncatedSchedule(t *testing.T) {
	rep := sct.Explore(sct.Config{Seed: 24}, assertProgram)
	if len(rep.Failures) == 0 {
		t.Fatal("expected a failure")
	}
	f := rep.Failures[0]
	if len(f.Schedule) < 2 {
		t.Skip("schedule too short to truncate")
	}
	truncated := f.Schedule[:len(f.Schedule)-2]
	res := sct.Replay(assertProgram, truncated)
	if res.Verdict != sct.VerdictError {
		t.Fatalf("truncated schedule must yield VerdictError, got %s", res.Verdict)
	}
}

// Same seed, same program: identical conclusions and identical
// schedule sequences.
func TestExploreIsDeterministic(t *testing.T) {
	program := func(e *sct.Env) {
		x := sct.NewVar[int](e, "x")
		m := sct.NewMutex(e)
		worker := func(c *sct.Env) {
			for i := 0; i < 2; i++ {
				m.Lock(c)
				x.Set(c, x.Get(c)+1)
				m.Unlock(c)
			}
		}
		e.Go(worker)
		e.Go(worker)
		e.Go(worker)
	}
	cfg := sct.Config{Seed: 42, MaxSchedules: 40}
	r1 := sct.Explore(cfg, program)
	r2 := sct.Explore(cfg, program)
	if r1.String() != r2.String() {
		t.Errorf("same seed must give identical reports:\n--- first ---\n%s\n--- second ---\n%s", r1, r2)
	}
}
