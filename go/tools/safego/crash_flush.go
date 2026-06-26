// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package safego

import (
	"context"
	"sync/atomic"
	"time"
)

// crashFlushTimeout bounds how long flushBeforeCrash waits for the registered
// flusher. It is a var (not a const) so tests can shrink it. A deliberate crash
// must not be delayed appreciably, so keep this short.
var crashFlushTimeout = time.Second

// crashFlusher holds the best-effort flush invoked just before GoCrashOnPanic
// re-raises a panic. Stored as a pointer so registration is lock-free and a
// later call replaces an earlier one.
var crashFlusher atomic.Pointer[func(context.Context)]

// RegisterCrashFlusher installs a best-effort flush that runs once, just before
// GoCrashOnPanic re-raises a panic and the process dies. The telemetry layer
// registers a flusher here so a deliberate crash gets a chance to export the
// crashing operation's trace span, the panic log record, and recent metrics
// before exit — telemetry that is otherwise buffered and lost when the process
// dies. It is inversion-of-control so this low-level package needs no dependency
// on the telemetry packages.
//
// The flusher is invoked under a short, detached deadline and is guarded so that
// a hung or panicking flusher can neither delay the crash beyond crashFlushTimeout
// nor mask the original panic. At most one flusher is active; passing nil clears it.
//
// This covers only safego's GoCrashOnPanic. Other deliberate fatal-exit paths
// should route through the same facility once it grows a process-lifecycle home.
func RegisterCrashFlusher(fn func(context.Context)) {
	if fn == nil {
		crashFlusher.Store(nil)
		return
	}
	crashFlusher.Store(&fn)
}

// flushBeforeCrash runs the registered flusher (if any) on a best-effort basis.
// It detaches from ctx's cancellation — a crash often happens while ctx is
// already cancelled — but preserves its values so the flusher still sees the
// trace context of the crashing operation. The flusher runs in its own goroutine
// so a flusher that ignores the deadline cannot stall the crash past the timeout;
// any panic inside the flusher is swallowed so it cannot replace the original.
func flushBeforeCrash(ctx context.Context) {
	p := crashFlusher.Load()
	if p == nil {
		return
	}
	flush := *p

	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), crashFlushTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer func() {
			_ = recover() // a flusher panic must not preempt the original crash
			close(done)
		}()
		flush(flushCtx)
	}()

	select {
	case <-done:
	case <-flushCtx.Done():
	}
}
