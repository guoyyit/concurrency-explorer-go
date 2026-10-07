# concurrency-explorer-go

A local, reproducible concurrency schedule explorer and defect detector
for Go. Programs under test are written against the controlled
primitives in `syncx`; the `explorer` package then enumerates schedules
systematically (or samples them randomly), detects data races, assertion
violations, panics, deadlocks and non-termination, and can replay any
reported schedule deterministically.

## Packages

- **`syncx`** — controlled concurrency primitives: `Go`, `Mutex`,
  `WaitGroup`, `Chan[T]`, `Var[T]`, `Assert`. Every operation is a
  *scheduling point*.
- **`explorer`** — the schedule explorer: `Explore` (systematic or
  random enumeration) and `Replay` (deterministic re-execution of one
  recorded schedule).

## Quick start

```go
prog := func(env *explorer.Env) {
    x := syncx.NewVar(env, 0)
    wg := syncx.NewWaitGroup(env)
    for i := 0; i < 2; i++ {
        wg.Add(1)
        syncx.Go(env, func() {
            x.Set(x.Get() + 1) // racy: unsynchronized read+write
            wg.Done()
        })
    }
    wg.Wait()
}

rep := explorer.Explore(prog, explorer.Options{})
// rep.Races[0] names the variable, both conflicting accesses
// (thread, read/write, file:line) and the exact triggering schedule.

res := explorer.Replay(prog, rep.Races[0].Schedule, explorer.Options{})
// res reproduces the identical interleaving and the identical race.
```

## Scheduling model

- Each `syncx` thread runs on a real goroutine, but execution is
  **cooperative**: a thread runs only while holding a grant from the
  central scheduler, and every `syncx` operation returns the grant.
  At most one thread executes user code at any moment, so the scheduler
  controls the exact interleaving at operation granularity.
- A **schedule** is the sequence of thread ids chosen at each
  scheduling point. It fully determines the run.
- `Explore` with the default `StrategySystematic` enumerates the
  schedule tree depth-first in deterministic thread-id order.
  `StrategyRandom` samples schedules from a PRNG seeded by
  `Options.Seed`. Both are fully reproducible: same options + same
  program ⇒ same explored schedules, same conclusions.
- `Replay` forces exactly the given schedule. It never changes
  scheduling decisions; if the schedule is invalid (a non-enabled
  thread, or a proper prefix of a longer run) it reports divergence
  via `ReplayResult.Err`.

## What is detected

| Defect | Report contents |
|---|---|
| Data race | Variable id, both accesses (thread, read/write, file:line, step), triggering schedule |
| Assertion violation | Message, call site, thread, failing schedule |
| Panic / misuse (e.g. unlock of unlocked mutex) | Message, thread, failing schedule |
| Deadlock | Wait-cycle participants, involved resources, wait-for edges, schedule |
| Non-termination | Example schedule(s); distinct from deadlock |

- **Races** are detected with vector-clock happens-before tracking.
  Two accesses to the same `Var` conflict iff at least one is a write
  and neither happens-before the other. Accesses that are always
  ordered (via `Mutex`, `WaitGroup`, or channel handoff) are *not*
  reported — no false positives for merely potential conflicts.
- **Deadlock vs. non-termination**: a deadlock is a state where every
  live thread is blocked on an operation that can never proceed; the
  report includes the circular-wait participants and the resources
  (mutexes / wait groups / channels) they wait on. A run that keeps
  making progress past `MaxSteps` is reported as *non-terminating*
  (e.g. livelock or an infinite loop), never as deadlock. Normal
  termination is reported as `completed`.
- **Budgets**: `MaxSchedules`, `MaxSteps`, `MaxThreads`, `Timeout`
  bound the exploration. When a budget cuts exploration short, the
  report has `Complete=false`, `Truncated=true` and a reason
  describing the uncovered remainder. The `Report` is assembled
  atomically and returned only at the end, so an interrupted run never
  yields contradictory partial conclusions.

## Applicability boundaries

- Only `syncx` operations are scheduling points. Interleavings *inside*
  plain Go code (e.g. two goroutines writing a plain shared variable)
  are not explored; shared state must live in `syncx.Var` and
  synchronization must use `syncx` primitives.
- The program must be deterministic given a schedule: do not use
  `time`, `math/rand` with global sources, map iteration order, real
  `sync`/`chan` operations, or the `go` keyword (use `syncx.Go`).
  Operations from unmanaged goroutines are rejected with a panic.
- `Timeout` (and the stuck-thread watchdog `StepTimeout`) depend on
  wall clock and may reduce *coverage* non-deterministically; every
  reported defect remains exactly reproducible via its schedule.
- WaitGroup waiters are modeled as depending on any other live thread
  (over-approximation) when no mutex/channel cycle explains the
  blocking; the deadlock verdict itself (no enabled operation in a
  closed system) is exact.
- User code that never reaches a `syncx` operation cannot be preempted;
  the watchdog classifies the run as non-terminating and the goroutine
  may be left running (documented in `Report.Notes`).
- Race reports deduplicate by resource id (creation call site +
  sequence). Resources created at the same call site in different
  schedule-dependent orders may produce separate reports.

## Tests

`explorer/` contains automated tests for: data race detection, absence
of false positives (mutex/channel/waitgroup ordering), assertion
violation and panic localization with replay, unreachable-assertion
coverage reporting, deadlock detection with wait cycles, livelock vs.
deadlock distinction, deterministic replay (including rejection of
invalid schedules), budget exhaustion (schedules/steps/threads/timeout)
and run-to-run determinism of both strategies.

```sh
go test ./...
```
