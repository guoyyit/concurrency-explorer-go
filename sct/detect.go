package sct

// vectorClock is a happens-before clock indexed by thread.
type vectorClock map[ThreadID]int

func (v vectorClock) tick(tid ThreadID) { v[tid]++ }

// lessEqual reports whether v happens-before-or-equals o.
func (v vectorClock) lessEqual(o vectorClock) bool {
	for t, n := range v {
		if o[t] < n {
			return false
		}
	}
	return true
}

// join merges o into v (pointwise max).
func (v vectorClock) join(o vectorClock) {
	for t, n := range o {
		if v[t] < n {
			v[t] = n
		}
	}
}

func (v vectorClock) clone() vectorClock {
	out := make(vectorClock, len(v))
	for t, n := range v {
		out[t] = n
	}
	return out
}

// locAccess records one thread's latest access to a location.
type locAccess struct {
	vc  vectorClock
	ref AccessRef
}

// locState tracks accesses to one memory location for race detection.
type locState struct {
	name   string
	reads  map[ThreadID]locAccess
	writes map[ThreadID]locAccess
}

// raceDetector detects data races via vector clocks. It reports a race
// only when two accesses to the same location, at least one of them a
// write, are unordered by happens-before; accesses that are always
// ordered (e.g. mutex-protected) are never flagged.
type raceDetector struct {
	locs map[ResourceID]*locState
}

func newRaceDetector() *raceDetector {
	return &raceDetector{locs: map[ResourceID]*locState{}}
}

func (d *raceDetector) state(loc ResourceID) *locState {
	s, ok := d.locs[loc]
	if !ok {
		s = &locState{
			reads:  map[ThreadID]locAccess{},
			writes: map[ThreadID]locAccess{},
		}
		d.locs[loc] = s
	}
	return s
}

// access records an access by tid with the thread's current clock vc.
// It returns the conflicting prior access when the new access is
// concurrent (unordered by happens-before) with a prior access by a
// different thread and at least one of the two is a write.
func (d *raceDetector) access(tid ThreadID, loc ResourceID, write bool, vc vectorClock, ref AccessRef) (AccessRef, bool) {
	s := d.state(loc)

	// A prior write always conflicts when unordered.
	for other, wa := range s.writes {
		if other == tid {
			continue
		}
		if !wa.vc.lessEqual(vc) {
			return wa.ref, true
		}
	}
	// A prior read conflicts only with a new write.
	if write {
		for other, ra := range s.reads {
			if other == tid {
				continue
			}
			if !ra.vc.lessEqual(vc) {
				return ra.ref, true
			}
		}
	}

	cur := locAccess{vc: vc.clone(), ref: ref}
	if write {
		s.writes[tid] = cur
	} else {
		s.reads[tid] = cur
	}
	return AccessRef{}, false
}
