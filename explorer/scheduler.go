package explorer

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// errAbort is panicked inside a thread when its schedule is aborted; it
// is always recovered by the thread wrapper.
var errAbort = errors.New("explorer: schedule aborted")

// threadState tracks one controlled thread.
type threadState struct {
	id      int
	grantCh chan grant
}

type mutexState struct {
	holder int // -1 means free
	vc     vectorClock
}

type wgState struct {
	count int
	vc    vectorClock
}

type chanItem struct {
	val any
	vc  vectorClock
}

type chanState struct {
	cap       int
	buf       []chanItem
	delivered map[int]chanItem // rendezvous handoff, keyed by receiver thread id
}

// scheduler owns the whole state of one schedule execution. Its loop is
// single-goroutine; thread goroutines only run when granted, so at most
// one thread executes user code at any moment.
type scheduler struct {
	opts Options

	reqCh  chan request
	byGo   sync.Map // goid int64 -> *threadState
	resMu  sync.Mutex
	resSeq map[string]int
	done   atomic.Bool

	gwg sync.WaitGroup // joins all thread goroutines before runOnce returns

	// The fields below are owned by the scheduler loop goroutine.
	threads  map[int]*threadState
	parked   map[int]request
	live     int
	nextID   int
	vcs      map[int]vectorClock
	mutexes  map[string]*mutexState
	wgs      map[string]*wgState
	chans    map[string]*chanState
	rd       *raceDetector
	events   []Event
	schedule []int
	frames   []frame
	steps    int
	verdict  Verdict
	failure  *FailureReport
	deadlock *DeadlockReport
	note     string
	diverged bool
}

func newScheduler(opts Options) *scheduler {
	s := &scheduler{
		opts:    opts,
		reqCh:   make(chan request, 2*opts.MaxThreads+16),
		resSeq:  map[string]int{},
		threads: map[int]*threadState{},
		parked:  map[int]request{},
		vcs:     map[int]vectorClock{},
		mutexes: map[string]*mutexState{},
		wgs:     map[string]*wgState{},
		chans:   map[string]*chanState{},
	}
	if !opts.SkipRaceDetection {
		s.rd = newRaceDetector()
	}
	return s
}

// runOnce executes prog under one schedule. guide (when non-nil) forces
// the leading choices; strict turns a mismatch or a proper prefix into a
// divergence. rnd (when non-nil) chooses randomly among enabled threads.
func runOnce(prog func(*Env), opts Options, guide []int, strict bool, rnd *rand.Rand) runOutcome {
	opts = opts.withDefaults()
	s := newScheduler(opts)
	env := &Env{s: s}
	s.spawnThread(nil, func() { prog(env) })
	s.loop(guide, strict, rnd)
	s.finish()
	return s.outcome()
}

// spawnThread registers and starts a new controlled thread goroutine.
// pvc is the parent vector clock at spawn time (nil for the main thread).
func (s *scheduler) spawnThread(pvc vectorClock, fn func()) {
	ts := &threadState{id: s.nextID, grantCh: make(chan grant, 1)}
	s.nextID++
	s.threads[ts.id] = ts
	s.live++
	vc := make(vectorClock)
	if pvc != nil {
		vc = pvc.clone()
	}
	s.vcs[ts.id] = vc
	s.gwg.Add(1)
	go func() {
		defer s.gwg.Done()
		gid := goid()
		s.byGo.Store(gid, ts)
		defer s.byGo.Delete(gid)
		defer func() {
			if r := recover(); r != nil {
				if r == errAbort {
					return
				}
				s.submitFrom(ts, Op{Kind: OpPanic, ThreadID: ts.id, Val: fmt.Sprintf("%v", r)})
			}
		}()
		s.submitFrom(ts, Op{Kind: OpThreadStart, ThreadID: ts.id})
		fn()
		s.submitFrom(ts, Op{Kind: OpThreadEnd, ThreadID: ts.id})
	}()
}

// submitFrom implements the thread side of a scheduling point.
func (s *scheduler) submitFrom(ts *threadState, op Op) any {
	switch op.Kind {
	case OpThreadEnd, OpPanic: // fire and forget
		s.reqCh <- request{ts: ts, op: op}
		return nil
	}
	s.reqCh <- request{ts: ts, op: op}
	g := <-ts.grantCh
	if g.abort {
		panic(errAbort)
	}
	return g.val
}

// current maps the calling goroutine to its controlled thread.
func (s *scheduler) current() *threadState {
	if v, ok := s.byGo.Load(goid()); ok {
		return v.(*threadState)
	}
	return nil
}

// loop is the scheduler main loop.
func (s *scheduler) loop(guide []int, strict bool, rnd *rand.Rand) {
	watchdog := time.NewTimer(s.opts.StepTimeout)
	defer watchdog.Stop()
	for s.verdict == "" {
		// Collect phase: wait until every live thread is parked.
		for len(s.parked) < s.live && s.verdict == "" {
			if !watchdog.Stop() {
				select {
				case <-watchdog.C:
				default:
				}
			}
			watchdog.Reset(s.opts.StepTimeout)
			select {
			case req := <-s.reqCh:
				s.handleRequest(req)
			case <-watchdog.C:
				s.verdict = VerdictNonTerminating
				s.note = "a thread made no scheduling progress (stuck or busy-looping without syncx operations)"
			}
		}
		if s.verdict != "" {
			break
		}
		if s.live == 0 {
			s.verdict = VerdictCompleted
			break
		}
		enabled := s.enabledThreads()
		if len(enabled) == 0 {
			s.makeDeadlock()
			break
		}
		if s.steps >= s.opts.MaxSteps {
			s.verdict = VerdictNonTerminating
			s.note = fmt.Sprintf("step budget %d exceeded", s.opts.MaxSteps)
			break
		}
		tid, idx, ok := s.choose(enabled, guide, strict, rnd)
		if !ok {
			break
		}
		s.frames = append(s.frames, frame{enabled: enabled, next: idx})
		s.schedule = append(s.schedule, tid)
		s.grant(tid)
	}
}

func (s *scheduler) handleRequest(req request) {
	switch req.op.Kind {
	case OpThreadEnd:
		delete(s.threads, req.ts.id)
		s.live--
	case OpPanic:
		s.verdict = VerdictPanic
		s.failure = &FailureReport{
			Verdict:  VerdictPanic,
			ThreadID: req.op.ThreadID,
			Message:  fmt.Sprint(req.op.Val),
		}
	default:
		s.parked[req.ts.id] = req
	}
}

// choose picks the next thread to grant. enabled is sorted ascending.
func (s *scheduler) choose(enabled []int, guide []int, strict bool, rnd *rand.Rand) (tid, idx int, ok bool) {
	depth := s.steps
	if depth < len(guide) {
		want := guide[depth]
		i := sort.SearchInts(enabled, want)
		if i < len(enabled) && enabled[i] == want {
			return want, i, true
		}
		s.verdict = VerdictInternalError
		s.diverged = true
		s.note = fmt.Sprintf("schedule diverged at step %d: thread %d is not enabled", depth, want)
		return 0, 0, false
	}
	if strict {
		s.verdict = VerdictInternalError
		s.diverged = true
		s.note = fmt.Sprintf("schedule ended at step %d but the program would continue", depth)
		return 0, 0, false
	}
	if rnd != nil {
		i := rnd.Intn(len(enabled))
		return enabled[i], i, true
	}
	return enabled[0], 0, true
}

// enabledThreads returns the sorted ids of parked threads whose pending
// operation can proceed.
func (s *scheduler) enabledThreads() []int {
	ids := make([]int, 0, len(s.parked))
	for id := range s.parked {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	out := ids[:0]
	for _, id := range ids {
		if s.enabled(s.parked[id].op) {
			out = append(out, id)
		}
	}
	return out
}

func (s *scheduler) enabled(op Op) bool {
	switch op.Kind {
	case OpLock:
		return s.mutex(op.Res).holder == -1
	case OpUnlock:
		return true // validated at grant time
	case OpWaitGroupWait:
		return s.wg(op.Res).count == 0
	case OpChanSend:
		c := s.ch(op.Res)
		if len(c.buf) < c.cap {
			return true
		}
		return c.cap == 0 && s.firstParkedRecv(op.Res) >= 0
	case OpChanRecv:
		c := s.ch(op.Res)
		if _, ok := c.delivered[op.ThreadID]; ok {
			return true
		}
		return len(c.buf) > 0
	default:
		return true // thread-start, spawn, var ops, wg-add, assert
	}
}

// firstParkedRecv returns the lowest thread id parked on Recv of res,
// or -1 if none.
func (s *scheduler) firstParkedRecv(res string) int {
	best := -1
	for id, req := range s.parked {
		if req.op.Kind == OpChanRecv && req.op.Res == res {
			if best == -1 || id < best {
				best = id
			}
		}
	}
	return best
}

// grant applies the effect of the chosen operation and unblocks the
// thread. It may set a verdict instead (assert failure, misuse, limit).
func (s *scheduler) grant(tid int) {
	req := s.parked[tid]
	delete(s.parked, tid)
	op := req.op
	vc := s.vcs[tid]
	vc.tick(tid)
	s.events = append(s.events, Event{Step: s.steps, ThreadID: tid, Op: op.Kind, Res: op.Res, CallSite: op.CallSite})

	fail := func(v Verdict, f *FailureReport, note string) {
		s.verdict = v
		s.failure = f
		s.note = note
		req.ts.grantCh <- grant{abort: true}
	}

	var gval any
	switch op.Kind {
	case OpThreadStart:
		// nothing to do
	case OpSpawn:
		if s.live+1 > s.opts.MaxThreads {
			s.verdict = VerdictThreadLimit
			s.note = fmt.Sprintf("thread limit %d exceeded", s.opts.MaxThreads)
			req.ts.grantCh <- grant{abort: true}
			return
		}
		s.spawnThread(vc, op.Val.(func()))
	case OpLock:
		m := s.mutex(op.Res)
		m.holder = tid
		vc.join(m.vc)
	case OpUnlock:
		m := s.mutex(op.Res)
		if m.holder != tid {
			fail(VerdictPanic, &FailureReport{
				Verdict: VerdictPanic, ThreadID: tid,
				Message: "unlock of unlocked mutex " + op.Res, CallSite: op.CallSite,
			}, "")
			return
		}
		m.holder = -1
		m.vc = vc.clone()
	case OpVarRead:
		if s.rd != nil {
			s.rd.access(op.Res, tid, false, op.CallSite, s.steps, vc.clone())
		}
	case OpVarWrite:
		if s.rd != nil {
			s.rd.access(op.Res, tid, true, op.CallSite, s.steps, vc.clone())
		}
	case OpWaitGroupAdd:
		w := s.wg(op.Res)
		delta := op.Val.(int)
		w.count += delta
		if delta < 0 {
			w.vc.join(vc)
		}
		if w.count < 0 {
			fail(VerdictPanic, &FailureReport{
				Verdict: VerdictPanic, ThreadID: tid,
				Message: "negative WaitGroup counter", CallSite: op.CallSite,
			}, "")
			return
		}
	case OpWaitGroupWait:
		vc.join(s.wg(op.Res).vc)
	case OpChanSend:
		c := s.ch(op.Res)
		item := chanItem{val: op.Val, vc: vc.clone()}
		if len(c.buf) == 0 {
			if rid := s.firstParkedRecv(op.Res); rid >= 0 {
				c.delivered[rid] = item
				break
			}
		}
		c.buf = append(c.buf, item)
	case OpChanRecv:
		c := s.ch(op.Res)
		if item, ok := c.delivered[tid]; ok {
			delete(c.delivered, tid)
			gval = item.val
			vc.join(item.vc)
		} else {
			item := c.buf[0]
			c.buf = c.buf[1:]
			gval = item.val
			vc.join(item.vc)
		}
	case OpAssert:
		if !op.Val.(bool) {
			fail(VerdictAssertFailed, &FailureReport{
				Verdict: VerdictAssertFailed, ThreadID: tid,
				Message: op.Msg, CallSite: op.CallSite,
			}, "")
			return
		}
	}
	s.steps++
	req.ts.grantCh <- grant{val: gval}
}

// makeDeadlock records a deadlock verdict with a wait-for graph.
func (s *scheduler) makeDeadlock() {
	s.verdict = VerdictDeadlock
	ids := make([]int, 0, len(s.parked))
	for id := range s.parked {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	var edges []WaitEdge
	resSet := map[string]bool{}
	for _, id := range ids {
		op := s.parked[id].op
		switch op.Kind {
		case OpLock:
			edges = append(edges, WaitEdge{From: id, To: s.mutex(op.Res).holder, Resource: op.Res})
			resSet[op.Res] = true
		case OpWaitGroupWait:
			// Over-approximation: the waiter may depend on any other
			// live thread's missing Done.
			for _, u := range ids {
				if u != id {
					edges = append(edges, WaitEdge{From: id, To: u, Resource: op.Res})
				}
			}
			resSet[op.Res] = true
		case OpChanSend, OpChanRecv:
			edges = append(edges, WaitEdge{From: id, To: -1, Resource: op.Res})
			resSet[op.Res] = true
		}
	}
	participants := ids
	if cycle := findCycle(edges, ids); cycle != nil {
		// A circular wait exists: report exactly the cycle participants
		// and the resources on the cycle's edges; other blocked threads
		// are victims, not causes.
		participants = cycle
		inCycle := map[int]bool{}
		for _, id := range cycle {
			inCycle[id] = true
		}
		resSet = map[string]bool{}
		for _, e := range edges {
			if inCycle[e.From] && inCycle[e.To] {
				resSet[e.Resource] = true
			}
		}
	}
	resources := make([]string, 0, len(resSet))
	for r := range resSet {
		resources = append(resources, r)
	}
	sort.Strings(resources)
	s.deadlock = &DeadlockReport{Participants: participants, Resources: resources, Cycle: edges}
}

// finish aborts all remaining threads and waits for their goroutines.
func (s *scheduler) finish() {
	s.done.Store(true)
	for _, req := range s.parked {
		req.ts.grantCh <- grant{abort: true}
	}
	s.parked = map[int]request{}
	waitDone := make(chan struct{})
	go func() { s.gwg.Wait(); close(waitDone) }()
	timer := time.NewTimer(s.opts.StepTimeout)
	defer timer.Stop()
	for {
		select {
		case <-waitDone:
			return
		case req := <-s.reqCh:
			if req.op.Kind == OpThreadEnd || req.op.Kind == OpPanic {
				continue
			}
			req.ts.grantCh <- grant{abort: true}
		case <-timer.C:
			if s.note == "" {
				s.note = "warning: a thread never reached a scheduling point and was left running"
			}
			return
		}
	}
}

// outcome snapshots the run result.
func (s *scheduler) outcome() runOutcome {
	out := runOutcome{
		verdict:  s.verdict,
		schedule: append(Schedule(nil), s.schedule...),
		events:   s.events,
		frames:   s.frames,
		note:     s.note,
		diverged: s.diverged,
	}
	if s.rd != nil {
		for _, r := range s.rd.races {
			r.Schedule = append(Schedule(nil), s.schedule...)
			out.races = append(out.races, r)
		}
	}
	if s.failure != nil {
		f := *s.failure
		f.Schedule = append(Schedule(nil), s.schedule...)
		out.failure = &f
	}
	if s.deadlock != nil {
		d := *s.deadlock
		d.Schedule = append(Schedule(nil), s.schedule...)
		out.deadlock = &d
	}
	return out
}

func (s *scheduler) mutex(res string) *mutexState {
	m, ok := s.mutexes[res]
	if !ok {
		m = &mutexState{holder: -1, vc: make(vectorClock)}
		s.mutexes[res] = m
	}
	return m
}

func (s *scheduler) wg(res string) *wgState {
	w, ok := s.wgs[res]
	if !ok {
		w = &wgState{vc: make(vectorClock)}
		s.wgs[res] = w
	}
	return w
}

func (s *scheduler) ch(res string) *chanState {
	c, ok := s.chans[res]
	if !ok {
		c = &chanState{delivered: map[int]chanItem{}}
		s.chans[res] = c
	}
	return c
}
