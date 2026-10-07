package sct_test

import (
	"strings"
	"testing"

	"github.com/guoyyit/concurrency-explorer-go/sct"
)

// Two unsynchronized writers on the same variable must be flagged,
// with locations, access kinds and a witness schedule.
func TestRaceDetected(t *testing.T) {
	program := func(e *sct.Env) {
		x := sct.NewVar[int](e, "x")
		e.Go(func(c *sct.Env) { x.Set(c, 1) })
		e.Go(func(c *sct.Env) { x.Set(c, 2) })
	}
	rep := sct.Explore(sct.Config{Seed: 1}, program)
	t.Log("\n" + rep.String())
	if len(rep.Races) == 0 {
		t.Fatal("expected a data race, got none")
	}
	r := rep.Races[0]
	if !strings.Contains(r.Location, "x") {
		t.Errorf("race location should mention variable name, got %q", r.Location)
	}
	if !r.First.Write || !r.Second.Write {
		t.Errorf("expected write/write race, got %+v vs %+v", r.First, r.Second)
	}
	if r.First.Thread == r.Second.Thread {
		t.Errorf("racing accesses must come from different threads: %+v", r)
	}
	if len(r.Schedule) == 0 {
		t.Error("race must carry a witness schedule")
	}
}

// Mutex-protected accesses are always ordered: no race may be reported.
func TestNoRaceWithMutex(t *testing.T) {
	program := func(e *sct.Env) {
		x := sct.NewVar[int](e, "x")
		m := sct.NewMutex(e)
		worker := func(c *sct.Env) {
			m.Lock(c)
			x.Set(c, 1)
			m.Unlock(c)
		}
		e.Go(worker)
		e.Go(worker)
	}
	rep := sct.Explore(sct.Config{Seed: 2}, program)
	t.Log("\n" + rep.String())
	if !rep.Complete {
		t.Errorf("exploration should be complete: %s", rep.IncompleteReason)
	}
	if len(rep.Races) != 0 {
		t.Errorf("false positive race on mutex-protected variable:\n%s", rep.Races[0])
	}
	if !rep.Clean() {
		t.Errorf("expected clean report, got:\n%s", rep.String())
	}
}

// Accesses ordered by a channel handshake are always ordered: the
// potential conflict must not be reported.
func TestNoRaceWithChannelOrdering(t *testing.T) {
	program := func(e *sct.Env) {
		x := sct.NewVar[int](e, "x")
		ch := sct.NewChan[int](e, 0)
		e.Go(func(c *sct.Env) {
			x.Set(c, 1)
			ch.Send(c, 0)
		})
		e.Go(func(c *sct.Env) {
			ch.Recv(c)
			x.Set(c, 2)
		})
	}
	rep := sct.Explore(sct.Config{Seed: 3}, program)
	t.Log("\n" + rep.String())
	if !rep.Complete {
		t.Errorf("exploration should be complete: %s", rep.IncompleteReason)
	}
	if len(rep.Races) != 0 {
		t.Errorf("false positive race on channel-ordered accesses:\n%s", rep.Races[0])
	}
}

// Read-read conflicts are not races.
func TestNoRaceReadRead(t *testing.T) {
	program := func(e *sct.Env) {
		x := sct.NewVar[int](e, "x")
		x.Set(e, 42)
		e.Go(func(c *sct.Env) { _ = x.Get(c) })
		e.Go(func(c *sct.Env) { _ = x.Get(c) })
	}
	rep := sct.Explore(sct.Config{Seed: 4}, program)
	if len(rep.Races) != 0 {
		t.Errorf("unexpected race: %s", rep.Races[0])
	}
}
