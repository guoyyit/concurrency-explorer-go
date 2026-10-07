package sct_test

import (
	"strings"
	"testing"

	"github.com/guoyyit/concurrency-explorer-go/sct"
)

// Lock-order inversion must be reported as a deadlock with the
// participating threads and the mutexes they wait on.
func TestDeadlockMutexCycle(t *testing.T) {
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
	rep := sct.Explore(sct.Config{Seed: 11}, program)
	t.Log("\n" + rep.String())
	var dl *sct.RunResult
	for i := range rep.Failures {
		if rep.Failures[i].Verdict == sct.VerdictDeadlock {
			dl = &rep.Failures[i]
		}
	}
	if dl == nil {
		t.Fatalf("expected a deadlock, got:\n%s", rep.String())
	}
	if len(dl.WaitGraph) != 2 {
		t.Errorf("wait graph should have 2 participants, got %d", len(dl.WaitGraph))
	}
	// Both mutexes and both threads must appear.
	var hasM1, hasM2 bool
	seenThreads := map[sct.ThreadID]bool{}
	for _, e := range dl.WaitGraph {
		seenThreads[e.Thread] = true
		if strings.Contains(e.Resource, "mutex#1") {
			hasM1 = true
		}
		if strings.Contains(e.Resource, "mutex#2") {
			hasM2 = true
		}
		if e.HeldBy < 0 {
			t.Errorf("mutex wait edge must name the holder: %+v", e)
		}
	}
	if !hasM1 || !hasM2 || len(seenThreads) != 2 {
		t.Errorf("wait graph must name both mutexes and both threads: %+v", dl.WaitGraph)
	}
	if !strings.Contains(dl.Message, "wait cycle") {
		t.Errorf("deadlock message should describe the wait cycle, got %q", dl.Message)
	}
	if len(dl.Schedule) == 0 {
		t.Error("deadlock must carry a reproducing schedule")
	}
}

// A rendezvous channel with only receivers is a deadlock, and the
// report must name the channel resource.
func TestDeadlockChannel(t *testing.T) {
	program := func(e *sct.Env) {
		ch := sct.NewChan[int](e, 0)
		e.Go(func(c *sct.Env) { ch.Recv(c) })
		e.Go(func(c *sct.Env) { ch.Recv(c) })
	}
	rep := sct.Explore(sct.Config{Seed: 12}, program)
	t.Log("\n" + rep.String())
	if len(rep.Failures) == 0 || rep.Failures[0].Verdict != sct.VerdictDeadlock {
		t.Fatalf("expected channel deadlock, got:\n%s", rep.String())
	}
	if !strings.Contains(rep.Failures[0].Message, "chan#") {
		t.Errorf("deadlock message should name the channel, got %q", rep.Failures[0].Message)
	}
}

// A thread that blocks temporarily and is then released is normal
// termination, not a deadlock.
func TestBlockedThenReleasedIsNotDeadlock(t *testing.T) {
	program := func(e *sct.Env) {
		wg := sct.NewWaitGroup(e)
		wg.Add(e, 1)
		e.Go(func(c *sct.Env) {
			sct.Yield(c)
			sct.Yield(c)
			wg.Done(c)
		})
		wg.Wait(e)
	}
	rep := sct.Explore(sct.Config{Seed: 13}, program)
	t.Log("\n" + rep.String())
	if !rep.Complete || !rep.Clean() {
		t.Fatalf("temporary blocking must complete cleanly, got:\n%s", rep.String())
	}
}

// Unbounded spinning is non-terminating, not a deadlock: the step
// budget stops the run and the verdicts stay distinct.
func TestNonTerminatingIsNotDeadlock(t *testing.T) {
	program := func(e *sct.Env) {
		e.Go(func(c *sct.Env) {
			for {
				sct.Yield(c)
			}
		})
		for {
			sct.Yield(e)
		}
	}
	rep := sct.Explore(sct.Config{Seed: 14, MaxSteps: 50, MaxSchedules: 3}, program)
	t.Log("\n" + rep.String())
	if len(rep.Failures) == 0 {
		t.Fatal("expected a non-terminating verdict")
	}
	f := rep.Failures[0]
	if f.Verdict != sct.VerdictNonTerminating {
		t.Fatalf("spinning must be non-terminating, got %s", f.Verdict)
	}
	if f.Verdict == sct.VerdictDeadlock {
		t.Error("non-termination must not be confused with deadlock")
	}
	if !strings.Contains(f.Message, "step budget") {
		t.Errorf("message should explain the budget stop, got %q", f.Message)
	}
}

// Producer/consumer over an unbuffered channel terminates normally.
func TestProducerConsumerTerminates(t *testing.T) {
	program := func(e *sct.Env) {
		ch := sct.NewChan[int](e, 0)
		e.Go(func(c *sct.Env) {
			for i := 0; i < 3; i++ {
				ch.Send(c, i)
			}
			ch.Close(c)
		})
		e.Go(func(c *sct.Env) {
			sum := 0
			for {
				v, ok := ch.Recv(c)
				if !ok {
					break
				}
				sum += v
			}
			sct.Assert(c, sum == 3, "sum of 0+1+2")
		})
	}
	rep := sct.Explore(sct.Config{Seed: 15}, program)
	t.Log("\n" + rep.String())
	if !rep.Complete || !rep.Clean() {
		t.Fatalf("producer/consumer must pass cleanly, got:\n%s", rep.String())
	}
}
