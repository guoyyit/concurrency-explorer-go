package sct

import "math/rand"

// Replay deterministically re-executes program under sched. Every
// scheduling choice is forced by sched; replay never introduces new
// interleavings. If the program would deviate from sched (a choice not
// enabled, or execution longer than sched), the result carries
// VerdictError.
func Replay(program func(*Env), sched Schedule) *RunResult {
	return ReplayWithConfig(Config{}, program, sched)
}

// ReplayWithConfig is Replay with explicit limits (e.g. MaxSteps must
// match the exploration config to reproduce non-terminating verdicts).
func ReplayWithConfig(cfg Config, program func(*Env), sched Schedule) *RunResult {
	cfg = cfg.withDefaults()
	// The rng is never consulted in strict mode; it is created only to
	// satisfy the runtime.
	rt := newRuntime(cfg, sched, true, rand.New(rand.NewSource(cfg.Seed)))
	return rt.run(program)
}
