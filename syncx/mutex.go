package syncx

import "github.com/guoyyit/concurrency-explorer-go/explorer"

// Mutex is a controlled mutual exclusion lock. The zero value is not
// usable; create one with NewMutex.
type Mutex struct {
	env *explorer.Env
	id  string
}

// NewMutex creates a controlled mutex.
func NewMutex(env *explorer.Env) *Mutex {
	return &Mutex{env: env, id: env.NewResource(callSite(1))}
}

// Lock blocks until the mutex is acquired by the current thread.
func (m *Mutex) Lock() {
	m.env.Submit(explorer.Op{Kind: explorer.OpLock, Res: m.id, CallSite: callSite(1)})
}

// Unlock releases the mutex. Unlocking a mutex not held by the current
// thread is reported as a runtime error.
func (m *Mutex) Unlock() {
	m.env.Submit(explorer.Op{Kind: explorer.OpUnlock, Res: m.id, CallSite: callSite(1)})
}
