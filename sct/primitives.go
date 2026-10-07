package sct

// Mutex is a controlled mutual-exclusion lock. It is not reentrant.
type Mutex struct {
	id ResourceID
}

// NewMutex creates a controlled mutex.
func NewMutex(e *Env) *Mutex {
	res := e.rt.step(e.ts, Op{Kind: OpNewRes, Arg: resSpec{kind: resMutex}, Site: callerSite(1)})
	return &Mutex{id: res.val.(ResourceID)}
}

// Lock acquires the mutex, blocking while another thread holds it.
func (m *Mutex) Lock(e *Env) {
	e.rt.step(e.ts, Op{Kind: OpLock, Res: m.id, Site: callerSite(1)})
}

// Unlock releases the mutex. Unlocking a mutex not held by the caller
// is recorded as a panic (Go semantics).
func (m *Mutex) Unlock(e *Env) {
	e.rt.step(e.ts, Op{Kind: OpUnlock, Res: m.id, Site: callerSite(1)})
}

// WaitGroup is a controlled sync.WaitGroup.
type WaitGroup struct {
	id ResourceID
}

// NewWaitGroup creates a controlled wait group.
func NewWaitGroup(e *Env) *WaitGroup {
	res := e.rt.step(e.ts, Op{Kind: OpNewRes, Arg: resSpec{kind: resWaitGroup}, Site: callerSite(1)})
	return &WaitGroup{id: res.val.(ResourceID)}
}

// Add changes the counter by delta.
func (w *WaitGroup) Add(e *Env, delta int) {
	e.rt.step(e.ts, Op{Kind: OpWGAdd, Res: w.id, Arg: delta, Site: callerSite(1)})
}

// Done decrements the counter.
func (w *WaitGroup) Done(e *Env) {
	e.rt.step(e.ts, Op{Kind: OpWGDone, Res: w.id, Site: callerSite(1)})
}

// Wait blocks until the counter reaches zero.
func (w *WaitGroup) Wait(e *Env) {
	e.rt.step(e.ts, Op{Kind: OpWGWait, Res: w.id, Site: callerSite(1)})
}

// Chan is a controlled channel. Capacity 0 means rendezvous.
type Chan[T any] struct {
	id ResourceID
}

// NewChan creates a controlled channel with the given buffer capacity.
func NewChan[T any](e *Env, capacity int) *Chan[T] {
	res := e.rt.step(e.ts, Op{Kind: OpNewRes, Arg: resSpec{kind: resChan, capacity: capacity}, Site: callerSite(1)})
	return &Chan[T]{id: res.val.(ResourceID)}
}

// Send sends v, blocking per channel semantics. Sending on a closed
// channel is recorded as a panic.
func (c *Chan[T]) Send(e *Env, v T) {
	e.rt.step(e.ts, Op{Kind: OpSend, Res: c.id, Arg: v, Site: callerSite(1)})
}

// Recv receives a value; ok is false when receiving from a closed,
// drained channel.
func (c *Chan[T]) Recv(e *Env) (v T, ok bool) {
	res := e.rt.step(e.ts, Op{Kind: OpRecv, Res: c.id, Site: callerSite(1)})
	rr := res.val.(recvResult)
	if !rr.ok {
		var zero T
		return zero, false
	}
	return rr.val.(T), true
}

// Close closes the channel.
func (c *Chan[T]) Close(e *Env) {
	e.rt.step(e.ts, Op{Kind: OpClose, Res: c.id, Site: callerSite(1)})
}

// Var is a controlled, race-tracked memory cell.
type Var[T any] struct {
	id ResourceID
}

// NewVar creates a tracked variable with the given name (used in reports).
func NewVar[T any](e *Env, name string) *Var[T] {
	res := e.rt.step(e.ts, Op{Kind: OpNewRes, Arg: resSpec{kind: resVar, name: name}, Site: callerSite(1)})
	return &Var[T]{id: res.val.(ResourceID)}
}

// Get reads the variable (a scheduling point, race-tracked).
func (v *Var[T]) Get(e *Env) T {
	res := e.rt.step(e.ts, Op{Kind: OpRead, Res: v.id, Site: callerSite(1)})
	if res.val == nil {
		var zero T
		return zero
	}
	return res.val.(T)
}

// Set writes the variable (a scheduling point, race-tracked).
func (v *Var[T]) Set(e *Env, x T) {
	e.rt.step(e.ts, Op{Kind: OpWrite, Res: v.id, Arg: x, Site: callerSite(1)})
}

// Assert records an assertion violation (with the current schedule)
// when cond is false, and ends the run.
func Assert(e *Env, cond bool, msg string) {
	e.rt.step(e.ts, Op{Kind: OpAssert, Arg: assertArg{ok: cond, msg: msg}, Site: callerSite(1)})
}

// Yield is a pure scheduling point with no other effect.
func Yield(e *Env) {
	e.rt.step(e.ts, Op{Kind: OpYield, Site: callerSite(1)})
}
