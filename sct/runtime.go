package sct

import (
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// errAbort is panicked inside a thread goroutine to unwind it when the
// scheduler aborts a run. It is always recovered by the thread wrapper.
var errAbort = errors.New("sct: run aborted")

// threadState is the scheduler-side view of one controlled thread.
// posted and grant implement a strict handshake: a thread posts exactly
// one op, then blocks until the scheduler grants it.
type threadState struct {
	id      ThreadID
	posted  chan Op       // buffered(1); thread -> scheduler
	grant   chan opResult // scheduler -> thread
	pending *Op           // current unconsumed post
	done    bool
}

type mutexState struct {
	owner ThreadID // -1 when free
	vc    vectorClock
}

type chanMsg struct {
	val any
	vc  vectorClock
}

type chanState struct {
	capacity int
	buf      []chanMsg
	closed   bool
	vc       vectorClock // carries close()
}

type wgState struct {
	count int
	vc    vectorClock
}

type varState struct {
	name string
	val  any
}

// recvResult is delivered to a completed Recv.
type recvResult struct {
	val any
	ok  bool
}

// grantReq is a pending scheduler grant for one thread.
type grantReq struct {
	ts  *threadState
	res opResult
}

// Runtime executes one controlled run of a program. All runtime state
// is owned by the scheduler goroutine; thread goroutines only touch
// their own threadState channels, so the runtime itself is race-free.
type Runtime struct {
	cfg    Config
	guide  Schedule // forced choices; nil = free choice
	strict bool     // replay mode: guide must match exactly

	threads []*threadState
	abortCh chan struct{}
	wg      sync.WaitGroup

	nextRes int
	mutexes map[ResourceID]*mutexState
	chans   map[ResourceID]*chanState
	wgs     map[ResourceID]*wgState
	vars    map[ResourceID]*varState

	vc     map[ThreadID]vectorClock
	doneVC map[ThreadID]vectorClock
	rd     *raceDetector

	rng      *rand.Rand
	schedule Schedule
	events   []Event
	enabled  [][]ThreadID
	races    []Race

	verdict   VerdictKind
	message   string
	waitGraph []WaitEdge
}

func newRuntime(cfg Config, guide Schedule, strict bool, rng *rand.Rand) *Runtime {
	return &Runtime{
		cfg:     cfg.withDefaults(),
		guide:   guide,
		strict:  strict,
		abortCh: make(chan struct{}),
		nextRes: 1,
		mutexes: map[ResourceID]*mutexState{},
		chans:   map[ResourceID]*chanState{},
		wgs:     map[ResourceID]*wgState{},
		vars:    map[ResourceID]*varState{},
		vc:      map[ThreadID]vectorClock{},
		doneVC:  map[ThreadID]vectorClock{},
		rd:      newRaceDetector(),
		rng:     rng,
		verdict: VerdictPass,
	}
}

// callerSite returns "file:line" of the caller's caller.
func callerSite(skip int) string {
	_, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return "?"
	}
	return fmt.Sprintf("%s:%d", filepath.Base(file), line)
}

// launch creates and starts a controlled thread. Scheduler goroutine only.
func (rt *Runtime) launch(fn func(*Env)) *threadState {
	ts := &threadState{
		id:     ThreadID(len(rt.threads) + 1),
		posted: make(chan Op, 1),
		grant:  make(chan opResult),
	}
	rt.threads = append(rt.threads, ts)
	rt.vc[ts.id] = vectorClock{}
	rt.wg.Add(1)
	go func() {
		defer rt.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				if r == errAbort {
					return
				}
				// Best effort: posted is buffered and empty here.
				ts.posted <- Op{Kind: OpPanic, Arg: fmt.Sprint(r), Site: callerSite(2)}
			}
		}()
		fn(&Env{rt: rt, ts: ts})
		ts.posted <- Op{Kind: OpDone}
	}()
	return ts
}

// step is called by a thread goroutine at every controlled operation.
func (rt *Runtime) step(ts *threadState, op Op) opResult {
	ts.posted <- op
	select {
	case res := <-ts.grant:
		return res
	case <-rt.abortCh:
		panic(errAbort)
	}
}

// run executes the program once and returns the full result.
func (rt *Runtime) run(program func(*Env)) *RunResult {
	rt.launch(program)
	for {
		if !rt.collect() {
			break
		}
		if rt.allDone() {
			break
		}
		enabled := rt.enabledSet()
		if len(enabled) == 0 {
			rt.verdict = VerdictDeadlock
			rt.buildDeadlock()
			break
		}
		if len(rt.schedule) >= rt.cfg.MaxSteps {
			rt.verdict = VerdictNonTerminating
			rt.message = fmt.Sprintf("step budget %d exceeded after %d steps (possible livelock or unbounded computation)",
				rt.cfg.MaxSteps, len(rt.schedule))
			break
		}
		rt.enabled = append(rt.enabled, enabled)
		pick, ok := rt.choose(enabled)
		if !ok {
			break
		}
		rt.apply(pick)
		if rt.verdict != VerdictPass {
			break
		}
	}
	return rt.finish()
}

// collect waits until every live thread has posted its next op.
// Returns false (verdict set) when a thread never reaches a
// scheduling point.
func (rt *Runtime) collect() bool {
	timeout := 10 * time.Second
	if rt.cfg.Timeout > 0 && rt.cfg.Timeout < timeout {
		timeout = rt.cfg.Timeout
	}
	deadline := time.Now().Add(timeout)
	for _, ts := range rt.threads {
		if ts.done || ts.pending != nil {
			continue
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			remaining = time.Millisecond
		}
		select {
		case op := <-ts.posted:
			posted := op
			ts.pending = &posted
		case <-time.After(remaining):
			rt.verdict = VerdictNonTerminating
			rt.message = fmt.Sprintf("thread T%d did not reach a scheduling point (stuck in uncontrolled code?)", ts.id)
			return false
		}
	}
	return true
}

func (rt *Runtime) allDone() bool {
	for _, ts := range rt.threads {
		if !ts.done {
			return false
		}
	}
	return true
}

func (rt *Runtime) enabledSet() []ThreadID {
	var out []ThreadID
	for _, ts := range rt.threads {
		if ts.done || ts.pending == nil {
			continue
		}
		if rt.opEnabled(ts) {
			out = append(out, ts.id)
		}
	}
	return out
}

func (rt *Runtime) opEnabled(ts *threadState) bool {
	switch ts.pending.Kind {
	case OpLock:
		return rt.mutexes[ts.pending.Res].owner == -1
	case OpSend:
		c := rt.chans[ts.pending.Res]
		if c.closed {
			return true // applies as a panic
		}
		return len(c.buf) < c.capacity || rt.findPending(ts.pending.Res, OpRecv) != nil
	case OpRecv:
		c := rt.chans[ts.pending.Res]
		return len(c.buf) > 0 || c.closed || rt.findPending(ts.pending.Res, OpSend) != nil
	case OpWGWait:
		return rt.wgs[ts.pending.Res].count == 0
	default:
		return true
	}
}

func (rt *Runtime) findPending(res ResourceID, kind OpKind) *threadState {
	for _, ts := range rt.threads {
		if ts.done || ts.pending == nil {
			continue
		}
		if ts.pending.Kind == kind && ts.pending.Res == res {
			return ts
		}
	}
	return nil
}

// choose picks the next thread to run: forced by the guide when
// present, otherwise random (seeded). Returns false when the guide
// cannot be followed.
func (rt *Runtime) choose(enabled []ThreadID) (ThreadID, bool) {
	step := len(rt.schedule)
	if step < len(rt.guide) {
		pick := rt.guide[step]
		for _, u := range enabled {
			if u == pick {
				return pick, true
			}
		}
		rt.verdict = VerdictError
		rt.message = fmt.Sprintf("schedule diverged at step %d: thread T%d is not enabled (program not schedule-deterministic?)", step, pick)
		return 0, false
	}
	if rt.strict {
		rt.verdict = VerdictError
		rt.message = fmt.Sprintf("execution continues past end of schedule at step %d", step)
		return 0, false
	}
	return enabled[rt.rng.Intn(len(enabled))], true
}

// apply executes the pending op of the chosen thread. Scheduler
// goroutine only.
func (rt *Runtime) apply(tid ThreadID) {
	ts := rt.threads[tid-1]
	op := *ts.pending
	ts.pending = nil
	step := len(rt.schedule)
	rt.schedule = append(rt.schedule, tid)
	rt.vc[tid].tick(tid)

	grants := []grantReq{{ts: ts}}
	detail := ""

	switch op.Kind {
	case OpNewRes:
		spec := op.Arg.(resSpec)
		id := ResourceID(rt.nextRes)
		rt.nextRes++
		switch spec.kind {
		case resMutex:
			rt.mutexes[id] = &mutexState{owner: -1, vc: vectorClock{}}
			detail = fmt.Sprintf("new mutex#%d", id)
		case resChan:
			rt.chans[id] = &chanState{capacity: spec.capacity, vc: vectorClock{}}
			detail = fmt.Sprintf("new chan#%d cap=%d", id, spec.capacity)
		case resWaitGroup:
			rt.wgs[id] = &wgState{vc: vectorClock{}}
			detail = fmt.Sprintf("new wg#%d", id)
		case resVar:
			rt.vars[id] = &varState{name: spec.name}
			detail = fmt.Sprintf("new var#%d %q", id, spec.name)
		}
		grants[0].res = opResult{val: id}

	case OpYield:
		detail = "yield"

	case OpSpawn:
		fn := op.Arg.(func(*Env))
		if len(rt.threads) >= rt.cfg.MaxThreads {
			rt.verdict = VerdictError
			rt.message = fmt.Sprintf("thread limit %d exceeded", rt.cfg.MaxThreads)
			detail = "spawn rejected: thread limit"
			break
		}
		child := rt.launch(fn)
		rt.vc[child.id] = rt.vc[tid].clone()
		detail = fmt.Sprintf("spawn T%d", child.id)

	case OpLock:
		m := rt.mutexes[op.Res]
		m.owner = tid
		rt.vc[tid].join(m.vc)
		detail = fmt.Sprintf("lock mutex#%d", op.Res)

	case OpUnlock:
		m := rt.mutexes[op.Res]
		if m.owner != tid {
			rt.verdict = VerdictPanic
			rt.message = fmt.Sprintf("sync: unlock of unlocked mutex#%d (T%d, %s)", op.Res, tid, op.Site)
			detail = "unlock of unlocked mutex"
			break
		}
		m.owner = -1
		m.vc = rt.vc[tid].clone()
		detail = fmt.Sprintf("unlock mutex#%d", op.Res)

	case OpSend:
		c := rt.chans[op.Res]
		if c.closed {
			rt.verdict = VerdictPanic
			rt.message = fmt.Sprintf("send on closed channel#%d (T%d, %s)", op.Res, tid, op.Site)
			detail = "send on closed channel"
			break
		}
		if recvTs := rt.findPending(op.Res, OpRecv); recvTs != nil {
			var msg chanMsg
			if c.capacity > 0 && len(c.buf) == c.capacity {
				// Full buffer with a waiting receiver: receiver takes
				// the head, sender enqueues.
				msg = c.buf[0]
				c.buf = c.buf[1:]
				c.buf = append(c.buf, chanMsg{val: op.Arg, vc: rt.vc[tid].clone()})
			} else {
				// Direct handoff.
				msg = chanMsg{val: op.Arg, vc: rt.vc[tid].clone()}
			}
			grants = rt.sideCompleteRecv(grants, recvTs, msg)
			detail = fmt.Sprintf("send chan#%d (handoff to T%d)", op.Res, recvTs.id)
		} else {
			c.buf = append(c.buf, chanMsg{val: op.Arg, vc: rt.vc[tid].clone()})
			detail = fmt.Sprintf("send chan#%d (buffered)", op.Res)
		}

	case OpRecv:
		c := rt.chans[op.Res]
		switch {
		case len(c.buf) > 0:
			msg := c.buf[0]
			c.buf = c.buf[1:]
			rt.vc[tid].join(msg.vc)
			grants[0].res = opResult{val: recvResult{val: msg.val, ok: true}}
			detail = fmt.Sprintf("recv chan#%d", op.Res)
		case c.closed:
			rt.vc[tid].join(c.vc)
			grants[0].res = opResult{val: recvResult{ok: false}}
			detail = fmt.Sprintf("recv chan#%d (closed)", op.Res)
		default:
			// Rendezvous with a pending sender.
			sendTs := rt.findPending(op.Res, OpSend)
			rt.vc[sendTs.id].tick(sendTs.id)
			rt.vc[tid].join(rt.vc[sendTs.id])
			grants[0].res = opResult{val: recvResult{val: sendTs.pending.Arg, ok: true}}
			sendTs.pending = nil
			grants = append(grants, grantReq{ts: sendTs})
			detail = fmt.Sprintf("recv chan#%d (rendezvous with T%d)", op.Res, sendTs.id)
		}

	case OpClose:
		c := rt.chans[op.Res]
		if c.closed {
			rt.verdict = VerdictPanic
			rt.message = fmt.Sprintf("close of closed channel#%d (T%d, %s)", op.Res, tid, op.Site)
			detail = "close of closed channel"
			break
		}
		c.closed = true
		c.vc = rt.vc[tid].clone()
		detail = fmt.Sprintf("close chan#%d", op.Res)

	case OpWGAdd:
		w := rt.wgs[op.Res]
		w.count += op.Arg.(int)
		if w.count < 0 {
			rt.verdict = VerdictPanic
			rt.message = fmt.Sprintf("sync: negative WaitGroup counter (T%d, %s)", tid, op.Site)
			detail = "negative WaitGroup counter"
			break
		}
		detail = fmt.Sprintf("wg#%d.add(%d) count=%d", op.Res, op.Arg.(int), w.count)

	case OpWGDone:
		w := rt.wgs[op.Res]
		w.count--
		if w.count < 0 {
			rt.verdict = VerdictPanic
			rt.message = fmt.Sprintf("sync: negative WaitGroup counter (T%d, %s)", tid, op.Site)
			detail = "negative WaitGroup counter"
			break
		}
		w.vc.join(rt.vc[tid])
		detail = fmt.Sprintf("wg#%d.done count=%d", op.Res, w.count)

	case OpWGWait:
		rt.vc[tid].join(rt.wgs[op.Res].vc)
		detail = fmt.Sprintf("wg#%d.wait", op.Res)

	case OpRead:
		v := rt.vars[op.Res]
		grants[0].res = opResult{val: v.val}
		ref := AccessRef{Step: step, Thread: tid, Write: false, Site: op.Site}
		if prior, found := rt.rd.access(tid, op.Res, false, rt.vc[tid], ref); found {
			rt.recordRace(op.Res, prior, ref)
		}
		detail = fmt.Sprintf("read %s", rt.varName(op.Res))

	case OpWrite:
		v := rt.vars[op.Res]
		v.val = op.Arg
		ref := AccessRef{Step: step, Thread: tid, Write: true, Site: op.Site}
		if prior, found := rt.rd.access(tid, op.Res, true, rt.vc[tid], ref); found {
			rt.recordRace(op.Res, prior, ref)
		}
		detail = fmt.Sprintf("write %s", rt.varName(op.Res))

	case OpAssert:
		a := op.Arg.(assertArg)
		if a.ok {
			detail = "assert ok"
		} else {
			rt.verdict = VerdictAssertFailed
			rt.message = fmt.Sprintf("assertion failed: %s (T%d, %s)", a.msg, tid, op.Site)
			detail = "assert FAILED: " + a.msg
		}

	case OpDone:
		ts.done = true
		rt.doneVC[tid] = rt.vc[tid].clone()
		grants = nil // thread already exited
		detail = "done"

	case OpPanic:
		rt.verdict = VerdictPanic
		rt.message = fmt.Sprintf("panic in T%d: %v (%s)", tid, op.Arg, op.Site)
		grants = nil // thread already exited
		detail = fmt.Sprintf("panic: %v", op.Arg)

	default:
		rt.verdict = VerdictError
		rt.message = fmt.Sprintf("unknown op %d", op.Kind)
		grants = nil
	}

	rt.events = append(rt.events, Event{Step: step, Thread: tid, Op: op.Kind, Detail: detail})
	if rt.verdict != VerdictPass {
		return // finish() aborts the remaining threads
	}
	for _, g := range grants {
		g.ts.grant <- g.res
	}
}

// sideCompleteRecv completes a pending receiver as a side effect of a
// send, appending its grant to grants.
func (rt *Runtime) sideCompleteRecv(grants []grantReq, recvTs *threadState, msg chanMsg) []grantReq {
	rt.vc[recvTs.id].tick(recvTs.id)
	rt.vc[recvTs.id].join(msg.vc)
	recvTs.pending = nil
	return append(grants, grantReq{ts: recvTs, res: opResult{val: recvResult{val: msg.val, ok: true}}})
}

func (rt *Runtime) varName(res ResourceID) string {
	if v, ok := rt.vars[res]; ok && v.name != "" {
		return fmt.Sprintf("%s(var#%d)", v.name, res)
	}
	return fmt.Sprintf("var#%d", res)
}

func (rt *Runtime) recordRace(res ResourceID, first, second AccessRef) {
	rt.races = append(rt.races, Race{
		Location: rt.varName(res),
		First:    first,
		Second:   second,
		// Schedule is filled in by finish().
	})
}

// buildDeadlock records the wait-for graph and a human-readable
// message, including any mutex wait cycle.
func (rt *Runtime) buildDeadlock() {
	var parts []string
	for _, ts := range rt.threads {
		if ts.done || ts.pending == nil {
			continue
		}
		op := ts.pending
		switch op.Kind {
		case OpLock:
			owner := rt.mutexes[op.Res].owner
			rt.waitGraph = append(rt.waitGraph, WaitEdge{
				Thread:   ts.id,
				Resource: fmt.Sprintf("mutex#%d", op.Res),
				HeldBy:   owner,
			})
			parts = append(parts, fmt.Sprintf("T%d waits for mutex#%d held by T%d", ts.id, op.Res, owner))
		case OpSend:
			rt.waitGraph = append(rt.waitGraph, WaitEdge{
				Thread:   ts.id,
				Resource: fmt.Sprintf("chan#%d (send)", op.Res),
				HeldBy:   -1,
			})
			parts = append(parts, fmt.Sprintf("T%d blocked sending on chan#%d (no receiver)", ts.id, op.Res))
		case OpRecv:
			rt.waitGraph = append(rt.waitGraph, WaitEdge{
				Thread:   ts.id,
				Resource: fmt.Sprintf("chan#%d (recv)", op.Res),
				HeldBy:   -1,
			})
			parts = append(parts, fmt.Sprintf("T%d blocked receiving on chan#%d (no sender)", ts.id, op.Res))
		case OpWGWait:
			rt.waitGraph = append(rt.waitGraph, WaitEdge{
				Thread:   ts.id,
				Resource: fmt.Sprintf("wg#%d (count=%d)", op.Res, rt.wgs[op.Res].count),
				HeldBy:   -1,
			})
			parts = append(parts, fmt.Sprintf("T%d waits on wg#%d (count=%d)", ts.id, op.Res, rt.wgs[op.Res].count))
		}
	}
	rt.message = fmt.Sprintf("%d live thread(s) permanently blocked: %s",
		len(parts), strings.Join(parts, "; "))
	if cyc := waitCycle(rt.waitGraph); cyc != "" {
		rt.message += "; wait cycle: " + cyc
	}
}

// waitCycle finds a thread->thread cycle through mutex edges.
func waitCycle(edges []WaitEdge) string {
	next := map[ThreadID]ThreadID{}
	for _, e := range edges {
		if e.HeldBy >= 0 {
			next[e.Thread] = e.HeldBy
		}
	}
	for start := range next {
		seen := map[ThreadID]bool{}
		path := []ThreadID{start}
		cur := start
		for {
			nxt, ok := next[cur]
			if !ok {
				break
			}
			if seen[nxt] || nxt == start {
				if nxt == start {
					names := make([]string, len(path))
					for i, t := range path {
						names[i] = fmt.Sprintf("T%d", t)
					}
					return strings.Join(names, " -> ") + " -> " + fmt.Sprintf("T%d", start)
				}
				break
			}
			seen[cur] = true
			path = append(path, nxt)
			cur = nxt
		}
	}
	return ""
}

// finish aborts remaining threads, waits for them, and builds the
// result. The result is assembled exactly once per run.
func (rt *Runtime) finish() *RunResult {
	close(rt.abortCh)
	done := make(chan struct{})
	go func() { rt.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// Threads stuck in uncontrolled code cannot be preempted;
		// they leak but cannot corrupt this result.
	}
	for i := range rt.races {
		rt.races[i].Schedule = append(Schedule(nil), rt.schedule...)
	}
	return &RunResult{
		Verdict:     rt.verdict,
		Message:     rt.message,
		Schedule:    rt.schedule,
		Events:      rt.events,
		Races:       rt.races,
		WaitGraph:   rt.waitGraph,
		Occurrences: 1,
		enabled:     rt.enabled,
	}
}

// Env is the user-facing handle to the controlled runtime. Each thread
// gets its own Env; sharing an Env across threads is a bug.
type Env struct {
	rt *Runtime
	ts *threadState
}

// Go spawns a controlled child thread.
func (e *Env) Go(fn func(*Env)) {
	e.rt.step(e.ts, Op{Kind: OpSpawn, Arg: fn, Site: callerSite(1)})
}
