// Package syncx provides controlled concurrency primitives. Programs
// under test are written against syncx so that the explorer can
// enumerate, record and replay their schedules deterministically.
package syncx

import (
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/guoyyit/concurrency-explorer-go/explorer"
)

// Go spawns fn as a controlled thread.
func Go(env *explorer.Env, fn func()) {
	env.Submit(explorer.Op{Kind: explorer.OpSpawn, Val: fn, CallSite: callSite(1)})
}

// Assert reports an assertion violation (with a reproducible schedule)
// when cond is false.
func Assert(env *explorer.Env, cond bool, msg string) {
	env.Submit(explorer.Op{Kind: explorer.OpAssert, Val: cond, Msg: msg, CallSite: callSite(1)})
}

// callSite returns "file:line" of the caller's caller (skip frames up).
func callSite(skip int) string {
	_, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return "unknown"
	}
	return fmt.Sprintf("%s:%d", filepath.Base(file), line)
}
