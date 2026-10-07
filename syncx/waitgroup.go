package syncx

import "github.com/guoyyit/concurrency-explorer-go/explorer"

// WaitGroup is a controlled wait group.
type WaitGroup struct {
	env *explorer.Env
	id  string
}

// NewWaitGroup creates a controlled wait group.
func NewWaitGroup(env *explorer.Env) *WaitGroup {
	return &WaitGroup{env: env, id: env.NewResource(callSite(1))}
}

// Add changes the counter by delta (negative to signal completion).
func (w *WaitGroup) Add(delta int) {
	w.env.Submit(explorer.Op{Kind: explorer.OpWaitGroupAdd, Res: w.id, Val: delta, CallSite: callSite(1)})
}

// Done is Add(-1).
func (w *WaitGroup) Done() { w.Add(-1) }

// Wait blocks until the counter reaches zero.
func (w *WaitGroup) Wait() {
	w.env.Submit(explorer.Op{Kind: explorer.OpWaitGroupWait, Res: w.id, CallSite: callSite(1)})
}
