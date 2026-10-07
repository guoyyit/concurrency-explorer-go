package explorer

import "time"

// Strategy selects how schedules are enumerated.
type Strategy int

const (
	// StrategySystematic enumerates schedules depth-first in deterministic
	// thread-id order (default).
	StrategySystematic Strategy = iota
	// StrategyRandom samples schedules using a PRNG seeded by Options.Seed.
	StrategyRandom
)

// Options controls exploration budgets and limits.
type Options struct {
	// MaxSchedules bounds the number of explored schedules (default 1000).
	MaxSchedules int
	// MaxSteps bounds scheduling steps per schedule; exceeding it yields a
	// non-terminating verdict for that schedule (default 10000).
	MaxSteps int
	// MaxThreads bounds concurrently live threads (default 32).
	MaxThreads int
	// Timeout bounds wall-clock exploration time; 0 means no timeout.
	Timeout time.Duration
	// Seed seeds StrategyRandom (default 1).
	Seed int64
	// Strategy selects the enumeration strategy.
	Strategy Strategy
	// SkipRaceDetection disables happens-before race detection.
	SkipRaceDetection bool
	// StepTimeout is a watchdog for user code that never reaches a
	// scheduling point; 0 uses the default (5s). A fired watchdog makes
	// coverage environment-dependent; conclusions stay valid.
	StepTimeout time.Duration
}

func (o Options) withDefaults() Options {
	if o.MaxSchedules <= 0 {
		o.MaxSchedules = 1000
	}
	if o.MaxSteps <= 0 {
		o.MaxSteps = 10000
	}
	if o.MaxThreads <= 0 {
		o.MaxThreads = 32
	}
	if o.Seed == 0 {
		o.Seed = 1
	}
	if o.StepTimeout == 0 {
		o.StepTimeout = 5 * time.Second
	}
	return o
}
