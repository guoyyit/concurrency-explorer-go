package explorer_test

import (
	"testing"

	"github.com/guoyyit/concurrency-explorer-go/explorer"
	"github.com/guoyyit/concurrency-explorer-go/syncx"
)

// abbaProg: two threads lock two mutexes in opposite order; some
// schedules deadlock.
func abbaProg(env *explorer.Env) {
	a := syncx.NewMutex(env)
	b := syncx.NewMutex(env)
	wg := syncx.NewWaitGroup(env)
	wg.Add(2)
	syncx.Go(env, func() {
		a.Lock()
		b.Lock()
		b.Unlock()
		a.Unlock()
		wg.Done()
	})
	syncx.Go(env, func() {
		b.Lock()
		a.Lock()
		a.Unlock()
		b.Unlock()
		wg.Done()
	})
	wg.Wait()
}

// chanDeadlockProg: both threads receive from an empty rendezvous
// channel nobody sends to.
func chanDeadlockProg(env *explorer.Env) {
	ch := syncx.NewChan[int](env, 0)
	syncx.Go(env, func() { ch.Recv() })
	ch.Recv()
}

// liveLockProg: threads keep making progress forever without ever
// blocking: non-termination, not deadlock.
func liveLockProg(env *explorer.Env) {
	x := syncx.NewVar(env, 0)
	wg := syncx.NewWaitGroup(env)
	wg.Add(1)
	syncx.Go(env, func() {
		for {
			x.Set(x.Get() + 1)
		}
	})
	wg.Wait()
}

func TestDeadlockDetected(t *testing.T) {
	rep := explorer.Explore(abbaProg, explorer.Options{})
	if len(rep.Deadlocks) == 0 {
		t.Fatal("expected a deadlock report")
	}
	d := rep.Deadlocks[0]
	if len(d.Participants) != 2 {
		t.Fatalf("expected 2 participants, got %v", d.Participants)
	}
	if len(d.Resources) != 2 {
		t.Fatalf("expected 2 resources, got %v", d.Resources)
	}
	if len(d.Cycle) == 0 {
		t.Fatal("expected wait-for edges")
	}
	t.Logf("deadlock: participants=%v resources=%v", d.Participants, d.Resources)

	res := explorer.Replay(abbaProg, d.Schedule, explorer.Options{})
	if res.Err != nil {
		t.Fatalf("replay diverged: %v", res.Err)
	}
	if res.Verdict != explorer.VerdictDeadlock {
		t.Fatalf("replay verdict = %s, want deadlock", res.Verdict)
	}
	if res.Deadlock == nil || len(res.Deadlock.Participants) != 2 {
		t.Fatalf("replay deadlock mismatch: %+v", res.Deadlock)
	}
}

func TestChannelDeadlockReported(t *testing.T) {
	rep := explorer.Explore(chanDeadlockProg, explorer.Options{})
	if len(rep.Deadlocks) != 1 {
		t.Fatalf("expected exactly 1 deadlock, got %d", len(rep.Deadlocks))
	}
	d := rep.Deadlocks[0]
	if len(d.Resources) == 0 {
		t.Fatal("expected the channel among deadlock resources")
	}
}

func TestNonTerminatingIsNotDeadlock(t *testing.T) {
	opts := explorer.Options{MaxSteps: 200}
	rep := explorer.Explore(liveLockProg, opts)
	if len(rep.Deadlocks) != 0 {
		t.Fatalf("livelock misclassified as deadlock: %+v", rep.Deadlocks)
	}
	if len(rep.NonTerminating) == 0 {
		t.Fatal("expected non-terminating schedules")
	}
	// A non-terminating schedule replays to the same verdict under the
	// same step budget.
	res := explorer.Replay(liveLockProg, rep.NonTerminating[0], opts)
	if res.Err != nil {
		t.Fatalf("replay diverged: %v", res.Err)
	}
	if res.Verdict != explorer.VerdictNonTerminating {
		t.Fatalf("replay verdict = %s, want non-terminating", res.Verdict)
	}
}

func TestNormalTerminationNotConfused(t *testing.T) {
	// A program that merely blocks briefly (WaitGroup) and then
	// terminates must be reported as clean.
	rep := explorer.Explore(waitGroupProg, explorer.Options{})
	if !rep.Complete || rep.HasDefects() {
		t.Fatalf("expected clean complete report, got %+v", rep)
	}
}
