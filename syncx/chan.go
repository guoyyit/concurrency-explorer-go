package syncx

import "github.com/guoyyit/concurrency-explorer-go/explorer"

// Chan is a controlled channel with Go-like blocking semantics.
type Chan[T any] struct {
	env *explorer.Env
	id  string
}

// NewChan creates a controlled channel with the given buffer capacity
// (0 for a rendezvous channel).
func NewChan[T any](env *explorer.Env, capacity int) *Chan[T] {
	return &Chan[T]{env: env, id: env.NewChanResource(callSite(1), capacity)}
}

// Send blocks until the value can be delivered.
func (c *Chan[T]) Send(v T) {
	c.env.Submit(explorer.Op{Kind: explorer.OpChanSend, Res: c.id, Val: v, CallSite: callSite(1)})
}

// Recv blocks until a value is available and returns it.
func (c *Chan[T]) Recv() T {
	v := c.env.Submit(explorer.Op{Kind: explorer.OpChanRecv, Res: c.id, CallSite: callSite(1)})
	return v.(T)
}
