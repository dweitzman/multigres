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

// Package safego launches goroutines whose panic behavior is explicit.
//
// A bare `go fn()` whose fn panics crashes the whole process: an unrecovered
// panic in any goroutine terminates the program, and there is no caller to hand
// the error to. safego forces every goroutine to declare what should happen on
// panic, so the decision is made deliberately and is visible at the call site:
//
//   - GoContinueOnPanic recovers the panic, logs it with its stack, counts it,
//     and lets the goroutine exit while the process keeps running. Use for
//     best-effort or retryable work where one failed run is survivable (the
//     common case — e.g. a monitor tick, a fire-and-forget notification).
//
//   - GoCrashOnPanic recovers, logs, counts, then re-raises the panic so the
//     process crashes. Use only when continuing past the panic could violate
//     correctness — shared or durable state may be left corrupt — and a clean
//     restart from durable state is the intended recovery.
//
// For the gRPC request path, do not use these: a request has a caller to return
// an error to, so recover→error (never crash) is correct. The servenv recovery
// interceptor uses [Recovered] for that.
package safego

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
)

// panicMode labels how a recovered panic was handled, for logs and metrics.
type panicMode string

const (
	modeContinue panicMode = "continue"
	modeCrash    panicMode = "crash"
	// modeRPC marks a panic recovered at the gRPC boundary and converted to an
	// error returned to the caller (see Recovered).
	modeRPC panicMode = "rpc"
)

// GoContinueOnPanic runs fn in a new goroutine. If fn panics, the panic is
// recovered, logged with its stack under source, and counted; the goroutine
// exits but the process continues.
func GoContinueOnPanic(ctx context.Context, source string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				record(ctx, source, modeContinue, r)
			}
		}()
		fn()
	}()
}

// GoCrashOnPanic runs fn in a new goroutine. If fn panics, the panic is logged
// with its stack under source and counted, then re-raised so the process
// crashes. Use only when continuing past the panic could violate correctness
// and a restart is the intended recovery.
func GoCrashOnPanic(ctx context.Context, source string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				record(ctx, source, modeCrash, r)
				panic(r)
			}
		}()
		fn()
	}()
}

// Recovered logs the recovered panic value r with its stack under source and
// counts it, returning a redacted error describing the panic. It is for callers
// that convert a recovered panic into an error instead of crashing — chiefly the
// gRPC recovery interceptor. It does not re-panic; the caller decides what to do
// with the returned error. r must be the non-nil value from recover(); callers
// invoke this only when recover() returned non-nil.
//
// The returned error deliberately omits the panic value and stack so it is safe
// to surface across a trust boundary; that detail is in the logs.
func Recovered(ctx context.Context, source string, r any) error {
	record(ctx, source, modeRPC, r)
	return fmt.Errorf("recovered from panic in %s", source)
}

// record performs the shared panic handling: log the value + stack under source
// and increment the panic counter keyed by source and mode.
func record(ctx context.Context, source string, mode panicMode, r any) {
	slog.ErrorContext(ctx, "recovered from panic in goroutine",
		"source", source,
		"mode", string(mode),
		"panic", r,
		"stack", string(debug.Stack()),
	)
	incPanic(ctx, source, mode)
}
