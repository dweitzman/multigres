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
	"sync"
)

// WaitGroup is a panic-aware [sync.WaitGroup]. Like sync.WaitGroup.Go it pairs
// goroutine launch with completion tracking, but it requires each goroutine to
// declare its panic behavior, exactly as the package-level GoContinueOnPanic /
// GoCrashOnPanic do.
//
// Use this instead of sync.WaitGroup.Go: a sync.WaitGroup.Go whose function
// panics crashes the whole process — it recovers and then deliberately
// re-panics (so as not to race Done against the fatal panic). That is rarely the
// behavior a background worker wants, and it is invisible to the bare-`go`
// lint. WaitGroup makes the choice explicit and, by default (GoContinueOnPanic),
// contains the panic so one worker's failure does not take the process down.
//
// SCOPE: this type contains a child's panic here and lets Wait return normally —
// the right semantic for INDEPENDENT work (one item failing must not abort the
// others). It is the wrong tool for ALL-OR-NOTHING fan-out, where a child's
// panic (or error) should propagate to the joining goroutine.
//
// TODO(safego): for the all-or-nothing case — chiefly the errgroup.Group.Go
// sites (topoclient store, manager state_manager) where a partial result is
// invalid — evaluate adopting sourcegraph/conc (conc.WaitGroup / conc/pool).
// conc captures a child panic with its stack and re-raises it at Wait, and its
// pools convert a panic into a returned error. That propagate-to-join semantic
// is complementary to this type's contain-and-continue semantic; pick per site.
type WaitGroup struct {
	wg sync.WaitGroup
}

// GoContinueOnPanic runs fn in a new goroutine tracked by the group. A panic is
// recovered, logged with its stack under source, and counted; the goroutine then
// completes normally (unblocking Wait) and the process continues. See
// [RunContinueOnPanic].
func (w *WaitGroup) GoContinueOnPanic(ctx context.Context, source string, fn func()) {
	w.wg.Go(func() { RunContinueOnPanic(ctx, source, fn) })
}

// GoCrashOnPanic runs fn in a new goroutine tracked by the group. A panic is
// logged and counted, the crash flusher runs, and the panic is re-raised so the
// process crashes. Use only when continuing past the panic could violate
// correctness and a restart is the intended recovery. See [RunCrashOnPanic].
func (w *WaitGroup) GoCrashOnPanic(ctx context.Context, source string, fn func()) {
	w.wg.Go(func() { RunCrashOnPanic(ctx, source, fn) })
}

// Wait blocks until all goroutines launched via the group have completed.
func (w *WaitGroup) Wait() { w.wg.Wait() }
