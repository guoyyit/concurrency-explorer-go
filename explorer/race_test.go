package explorer_test

import (
	"testing"

	"github.com/guoyyit/concurrency-explorer-go/explorer"
	"github.com/guoyyit/concurrency-explorer-go/syncx"
)

// racyProg: two threads increment a shared variable without any
// synchronization; the read and the write of the two threads conflict.
func racyProg(env *explorer.Env) {
	x := syncx.NewVar(env, 0)
	wg := syncx.NewWaitGroup(env)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		syncx.Go(env, func() {
			x.Set(x.Get() + 1)
			wg.Done()
		})
	}
	wg.Wait()
}

func TestRaceDetected(t *testing.T) {
	rep := explorer.Explore(racyProg, explorer.Options{})
	if !rep.Complete {
		t.Fatalf("expected complete exploration, got truncated: %s", rep.TruncatedReason)
	}
	if len(rep.Races) == 0 {
		t.Fatal("expected at least one data race")
	}
	r := rep.Races[0]
	if r.Resource == "" {
		t.Error("race report missing resource")
	}
	if !r.A.Write && !r.B.Write {
		t.Error("race report has no write access")
	}
	if len(r.Schedule) == 0 {
		t.Error("race report missing triggering schedule")
	}
	t.Logf("race: %s A=%+v B=%+v steps=%d schedules=%d",
		r.Resource, r.A, r.B, len(r.Schedule), rep.SchedulesExplored)
}

func TestRaceReplayReproduces(t *testing.T) {
	rep := explorer.Explore(racyProg, explorer.Options{})
	if len(rep.Races) == 0 {
		t.Fatal("expected at least one data race")
	}
	res := explorer.Replay(racyProg, rep.Races[0].Schedule, explorer.Options{})
	if res.Err != nil {
		t.Fatalf("replay diverged: %v", res.Err)
	}
	if len(res.Races) == 0 {
		t.Fatal("replay did not reproduce the race")
	}
}
