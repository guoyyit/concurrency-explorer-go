package explorer

import "fmt"

// accessRec records one memory access for happens-before comparison.
type accessRec struct {
	vc       vectorClock
	threadID int
	site     string
	step     int
	write    bool
}

// varState keeps the not-yet-ordered accesses of one shared variable:
// writes holds an antichain of mutually concurrent writes, reads holds
// the latest read per thread.
type varState struct {
	writes []accessRec
	reads  map[int]accessRec
}

// raceDetector tracks accesses and reports unordered conflicting pairs.
type raceDetector struct {
	vars  map[string]*varState
	races []RaceReport
	seen  map[string]bool
}

func newRaceDetector() *raceDetector {
	return &raceDetector{vars: map[string]*varState{}, seen: map[string]bool{}}
}

// access records one access and appends any detected race to d.races.
// vc must be the accessing thread's clock snapshot (cloned).
func (d *raceDetector) access(res string, tid int, write bool, site string, step int, vc vectorClock) {
	st, ok := d.vars[res]
	if !ok {
		st = &varState{reads: map[int]accessRec{}}
		d.vars[res] = st
	}
	a := accessRec{vc: vc, threadID: tid, site: site, step: step, write: write}

	// A new access conflicts with every stored access to the same
	// variable that is not happens-before ordered and where at least
	// one side is a write.
	for _, w := range st.writes {
		if w.threadID == tid {
			continue // same thread: program order already orders them
		}
		if !leq(w.vc, a.vc) {
			d.report(res, w, a)
		}
	}
	if write {
		for _, r := range st.reads {
			if r.threadID == tid {
				continue
			}
			if !leq(r.vc, a.vc) {
				d.report(res, r, a)
			}
		}
		// Keep only writes not dominated by the new one (antichain).
		kept := st.writes[:0]
		for _, w := range st.writes {
			if !leq(w.vc, a.vc) {
				kept = append(kept, w)
			}
		}
		st.writes = append(kept, a)
	} else {
		st.reads[tid] = a // latest read per thread dominates older ones
	}
}

func (d *raceDetector) report(res string, x, y accessRec) {
	key := fmt.Sprintf("%s|%d|%s|%t|%d|%s|%t", res,
		x.threadID, x.site, x.write, y.threadID, y.site, y.write)
	if d.seen[key] {
		return
	}
	d.seen[key] = true
	d.races = append(d.races, RaceReport{
		Resource: res,
		A:        Access{ThreadID: x.threadID, Write: x.write, CallSite: x.site, Step: x.step},
		B:        Access{ThreadID: y.threadID, Write: y.write, CallSite: y.site, Step: y.step},
	})
}
