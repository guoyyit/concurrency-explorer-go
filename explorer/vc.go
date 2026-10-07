package explorer

// vectorClock is a sparse vector clock used for happens-before tracking.
type vectorClock map[int]uint64

func (v vectorClock) clone() vectorClock {
	c := make(vectorClock, len(v))
	for k, x := range v {
		c[k] = x
	}
	return c
}

func (v vectorClock) tick(tid int) { v[tid]++ }

// join merges o into v (element-wise maximum).
func (v vectorClock) join(o vectorClock) {
	for k, x := range o {
		if x > v[k] {
			v[k] = x
		}
	}
}

// leq reports whether a happens-before-or-equal b.
func leq(a, b vectorClock) bool {
	for k, x := range a {
		if x > b[k] {
			return false
		}
	}
	return true
}
