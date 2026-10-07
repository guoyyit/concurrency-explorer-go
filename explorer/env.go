package explorer

import (
	"fmt"
	"runtime"
)

// Env is the handle a program under test uses to reach the scheduler of
// the current run. It is only valid during a single run.
type Env struct {
	s *scheduler
}

// Submit performs one scheduling point and blocks until the scheduler
// grants this thread. It returns a value for operations that deliver one
// (e.g. channel receive). op.ThreadID is filled in by Submit.
func (e *Env) Submit(op Op) any {
	s := e.s
	if s == nil || s.done.Load() {
		panic("explorer: Env used outside of an active run")
	}
	ts := s.current()
	if ts == nil {
		panic("explorer: controlled operation from an unmanaged goroutine; use syncx.Go")
	}
	op.ThreadID = ts.id
	return s.submitFrom(ts, op)
}

// NewResource allocates a deterministic resource identity derived from
// the creation call site.
func (e *Env) NewResource(callSite string) string {
	s := e.s
	s.resMu.Lock()
	defer s.resMu.Unlock()
	n := s.resSeq[callSite]
	s.resSeq[callSite] = n + 1
	return fmt.Sprintf("%s#%d", callSite, n)
}

// NewChanResource is NewResource plus registration of the channel
// capacity with the scheduler.
func (e *Env) NewChanResource(callSite string, capacity int) string {
	id := e.NewResource(callSite)
	s := e.s
	s.resMu.Lock()
	defer s.resMu.Unlock()
	s.chans[id] = &chanState{cap: capacity, delivered: map[int]chanItem{}}
	return id
}

// goid returns the current goroutine's id, used only as an internal
// lookup key (never part of any reported output).
func goid() int64 {
	var buf [32]byte
	n := runtime.Stack(buf[:], false)
	b := buf[:n]
	// format: "goroutine 123 [..."
	const prefix = len("goroutine ")
	id := int64(0)
	for i := prefix; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
		id = id*10 + int64(b[i]-'0')
	}
	return id
}
