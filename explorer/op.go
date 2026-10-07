package explorer

// OpKind identifies the kind of a controlled concurrency operation.
// Every operation is a scheduling point: the executing thread parks and
// the scheduler decides which parked thread proceeds next.
type OpKind int

const (
	OpThreadStart OpKind = iota
	OpThreadEnd
	OpSpawn
	OpLock
	OpUnlock
	OpVarRead
	OpVarWrite
	OpWaitGroupAdd
	OpWaitGroupWait
	OpChanSend
	OpChanRecv
	OpAssert
	OpPanic
)

func (k OpKind) String() string {
	switch k {
	case OpThreadStart:
		return "thread-start"
	case OpThreadEnd:
		return "thread-end"
	case OpSpawn:
		return "spawn"
	case OpLock:
		return "lock"
	case OpUnlock:
		return "unlock"
	case OpVarRead:
		return "var-read"
	case OpVarWrite:
		return "var-write"
	case OpWaitGroupAdd:
		return "wg-add"
	case OpWaitGroupWait:
		return "wg-wait"
	case OpChanSend:
		return "chan-send"
	case OpChanRecv:
		return "chan-recv"
	case OpAssert:
		return "assert"
	case OpPanic:
		return "panic"
	}
	return "unknown"
}

// Op describes one scheduling point submitted by a thread.
type Op struct {
	Kind     OpKind
	ThreadID int
	Res      string // resource identity ("" if none)
	Val      any    // send value / assert cond / panic value / spawn func / wg delta
	Msg      string // assert message
	CallSite string // file:line of the user call site
}

// request is sent from a thread goroutine to the scheduler.
type request struct {
	ts *threadState
	op Op
}

// grant is the scheduler's reply that unblocks a parked thread.
type grant struct {
	abort bool
	val   any
}
