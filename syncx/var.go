package syncx

import "github.com/guoyyit/concurrency-explorer-go/explorer"

// Var is an instrumented shared memory cell. All shared state of a
// program under test must live in Vars so that accesses are scheduling
// points and data races can be detected precisely.
type Var[T any] struct {
	env *explorer.Env
	id  string
	val T
}

// NewVar creates a controlled shared variable with an initial value.
func NewVar[T any](env *explorer.Env, init T) *Var[T] {
	return &Var[T]{env: env, id: env.NewResource(callSite(1)), val: init}
}

// Get reads the variable.
func (v *Var[T]) Get() T {
	v.env.Submit(explorer.Op{Kind: explorer.OpVarRead, Res: v.id, CallSite: callSite(1)})
	return v.val
}

// Set writes the variable.
func (v *Var[T]) Set(val T) {
	v.env.Submit(explorer.Op{Kind: explorer.OpVarWrite, Res: v.id, CallSite: callSite(1)})
	v.val = val
}
