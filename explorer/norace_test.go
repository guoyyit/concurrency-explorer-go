package explorer_test

import (
	"testing"

	"github.com/guoyyit/concurrency-explorer-go/explorer"
	"github.com/guoyyit/concurrency-explorer-go/syncx"
)

// mutexProg: the same conflicting accesses as racyProg but always
// ordered by a mutex, so no race may be reported.
func mutexProg(env *explorer.Env) {
	x := syncx.NewVar(env, 0)
	mu := syncx.NewMutex(env)
	wg := syncx.NewWaitGroup(env)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		syncx.Go(env, func() {
			mu.Lock()
			x.Set(x.Get() + 1)
			mu.Unlock()
			wg.Done()
		})
	}
	wg.Wait()
}

// chanProg: the write happens-before the read via a channel handoff.
func chanProg(env *explorer.Env) {
	x := syncx.NewVar(env, 0)
	ch := syncx.NewChan[int](env, 0)
	syncx.Go(env, func() {
		x.Set(42)
		ch.Send(1)
	})
	syncx.Go(env, func() {
		ch.Recv()
		_ = x.Get()
	})
}

// waitGroupProg: the write happens-before the read via Done/Wait.
func waitGroupProg(env *explorer.Env) {
	x := syncx.NewVar(env, 0)
	wg := syncx.NewWaitGroup(env)
	wg.Add(1)
	syncx.Go(env, func() {
		x.Set(7)
		wg.Done()
	})
	wg.Wait()
	_ = x.Get()
}

func TestNoFalseRaces(t *testing.T) {
	progs := map[string]func(*explorer.Env){
		"mutex":     mutexProg,
		"channel":   chanProg,
		"waitgroup": waitGroupProg,
	}
	for name, prog := range progs {
		t.Run(name, func(t *testing.T) {
			rep := explorer.Explore(prog, explorer.Options{})
			if !rep.Complete {
				t.Fatalf("expected complete exploration: %s", rep.TruncatedReason)
			}
			if len(rep.Races) != 0 {
				t.Fatalf("unexpected races: %+v", rep.Races)
			}
			if rep.HasDefects() {
				t.Fatalf("unexpected defects: %+v", rep)
			}
		})
	}
}
