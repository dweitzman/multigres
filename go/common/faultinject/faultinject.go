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

// Package faultinject injects delays and errors at named points in the code,
// so tests can hold the system at a fragile moment or stop it there.
//
// Faults are off unless configured. Today they come from the file named by
// EnvFile, re-read whenever it changes, so a test can turn faults on after
// the cluster is up. Engine.Set configures them programmatically.
package faultinject

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// EnvFile names the environment variable holding the path of a faults file.
// Test-only: never set it in production.
//
// Each line is a point followed by its actions, for example:
//
//	promote.before-pg-promote delay=10s
//	promote.after-pg-promote fail
//
// Blank lines and lines starting with # are ignored. A missing file means no
// faults.
const EnvFile = "MULTIGRES_FAULTS_FILE"

// Point names a place where a fault can be injected.
type Point string

const (
	// PromoteBeforePgPromote is in Promote after the pooler has recorded the
	// proposed rule, before pg_promote: it believes it leads, postgres is a
	// standby.
	PromoteBeforePgPromote Point = "promote.before-pg-promote"
	// PromoteAfterPgPromote is in Promote after pg_promote, before the rule is
	// written: postgres is a primary, its WAL still names the old leader.
	PromoteAfterPgPromote Point = "promote.after-pg-promote"
)

var knownPoints = map[Point]bool{
	PromoteBeforePgPromote: true,
	PromoteAfterPgPromote:  true,
}

// Action is what happens when a point is reached: wait Delay, then fail if Fail.
type Action struct {
	Delay time.Duration
	Fail  bool
}

// Engine decides which faults fire. The zero value injects nothing.
type Engine struct {
	faults atomic.Pointer[map[Point]Action]

	// file, if set, is re-read whenever its size or modification time changes.
	file     string
	mu       sync.Mutex
	fileStat string
}

var std = &Engine{file: os.Getenv(EnvFile)}

// Check runs the fault configured for p, if any, on the process-wide engine.
func Check(ctx context.Context, p Point) error {
	return std.Check(ctx, p)
}

// Check waits and returns an error as configured for p. With no faults
// configured it returns nil at once.
func (e *Engine) Check(ctx context.Context, p Point) error {
	if e.file != "" {
		e.reload(ctx)
	}
	faults := e.faults.Load()
	if faults == nil {
		return nil
	}
	action, ok := (*faults)[p]
	if !ok {
		return nil
	}
	slog.WarnContext(ctx, "injecting fault", "point", p, "delay", action.Delay, "fail", action.Fail)
	if action.Delay > 0 {
		timer := time.NewTimer(action.Delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	if action.Fail {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL, "injected fault at %s", p)
	}
	return nil
}

// Set replaces the configured faults. A nil or empty map clears them.
func (e *Engine) Set(faults map[Point]Action) {
	if len(faults) == 0 {
		e.faults.Store(nil)
		return
	}
	e.faults.Store(&faults)
}

// reload re-reads the faults file if it changed. A file that fails to parse
// leaves the previous faults in place.
func (e *Engine) reload(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()
	info, err := os.Stat(e.file)
	stat := ""
	if err == nil {
		stat = fmt.Sprintf("%d/%d", info.Size(), info.ModTime().UnixNano())
	}
	if stat == e.fileStat {
		return
	}
	e.fileStat = stat
	if stat == "" {
		e.Set(nil)
		return
	}
	data, err := os.ReadFile(e.file)
	if err != nil {
		slog.ErrorContext(ctx, "reading faults file", "path", e.file, "error", err)
		return
	}
	faults, err := Parse(string(data))
	if err != nil {
		slog.ErrorContext(ctx, "parsing faults file", "path", e.file, "error", err)
		return
	}
	slog.WarnContext(ctx, "fault injection configured", "path", e.file, "faults", faults)
	e.Set(faults)
}

// Parse reads faults in the EnvFile format.
func Parse(s string) (map[Point]Action, error) {
	faults := map[Point]Action{}
	scanner := bufio.NewScanner(strings.NewReader(s))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		p := Point(fields[0])
		if !knownPoints[p] {
			return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "unknown fault point %q", p)
		}
		if len(fields) == 1 {
			return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "fault point %q has no action", p)
		}
		var action Action
		for _, f := range fields[1:] {
			switch {
			case f == "fail":
				action.Fail = true
			case strings.HasPrefix(f, "delay="):
				d, err := time.ParseDuration(strings.TrimPrefix(f, "delay="))
				if err != nil {
					return nil, mterrors.Wrapf(err, "fault point %q", p)
				}
				action.Delay = d
			default:
				return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "fault point %q: unknown action %q", p, f)
			}
		}
		faults[p] = action
	}
	return faults, scanner.Err()
}
