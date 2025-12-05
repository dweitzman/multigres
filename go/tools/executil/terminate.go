// Copyright 2025 Supabase, Inc.
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

package executil

import (
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"time"
)

// TerminateProcess sends SIGTERM to a process and waits for graceful exit.
//
// Returns true if the process exited before ctx expired.
// Returns false if ctx expired - process may still be running, call KillProcess().
//
// Returns true immediately if process is nil or was already dead.
func TerminateProcess(ctx context.Context, process *os.Process) bool {
	if process == nil {
		return true
	}
	return TerminatePID(ctx, process.Pid)
}

// KillProcess sends SIGKILL to a process and waits for it to exit.
//
// Returns (nil, true) if the process exited before ctx expired.
// Returns (ctx.Err(), false) if the wait timed out (unexpected for SIGKILL).
//
// Returns (nil, true) immediately if process is nil or was already dead.
func KillProcess(ctx context.Context, process *os.Process) (error, bool) {
	if process == nil {
		return nil, true
	}
	return KillPID(ctx, process.Pid)
}

// TerminatePID sends SIGTERM to a process by PID and waits for graceful exit.
//
// Returns true if the process exited before ctx expired.
// Returns false if ctx expired - process may still be running, call KillPID().
//
// Returns true immediately if the process was already dead.
func TerminatePID(ctx context.Context, pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return true // Process doesn't exist
	}

	// Send SIGTERM
	if err := process.Signal(syscall.SIGTERM); err != nil {
		if isProcessGone(err) {
			return true
		}
		// SIGTERM failed for unexpected reason - process state unknown
		return false
	}

	// Wait for process to exit or context timeout
	return waitForProcessExit(ctx, process)
}

// KillPID sends SIGKILL to a process by PID and waits for it to exit.
//
// Returns (nil, true) if the process exited before ctx expired.
// Returns (ctx.Err(), false) if the wait timed out (unexpected for SIGKILL).
//
// Returns (nil, true) immediately if the process was already dead.
func KillPID(ctx context.Context, pid int) (error, bool) {
	process, err := os.FindProcess(pid)
	if err != nil {
		//nolint:nilerr // err means process doesn't exist, which is success for kill
		return nil, true
	}

	// Send SIGKILL
	if err := process.Kill(); err != nil {
		if isProcessGone(err) {
			return nil, true
		}
		return err, false
	}

	// Wait for process to exit
	if waitForProcessExit(ctx, process) {
		return nil, true
	}
	return ctx.Err(), false
}

// StopProcess gracefully stops a process: SIGTERM first, then SIGKILL if needed.
//
// gracePeriod is how long to wait for SIGTERM to take effect.
// killTimeout is how long to wait after SIGKILL (should be short, SIGKILL is immediate).
//
// Returns (nil, true) if the process stopped.
// Returns (ctx.Err(), false) if the process couldn't be stopped (SIGKILL timeout - very rare).
//
// This is the recommended way to stop a process - always try graceful termination first.
func StopProcess(gracePeriod, killTimeout time.Duration, process *os.Process) (error, bool) {
	if process == nil {
		return nil, true
	}
	return StopPID(gracePeriod, killTimeout, process.Pid)
}

// StopPID gracefully stops a process by PID: SIGTERM first, then SIGKILL if needed.
//
// gracePeriod is how long to wait for SIGTERM to take effect.
// killTimeout is how long to wait after SIGKILL (should be short, SIGKILL is immediate).
//
// Returns (nil, true) if the process stopped.
// Returns (ctx.Err(), false) if the process couldn't be stopped (SIGKILL timeout - very rare).
//
// This is the recommended way to stop a process - always try graceful termination first.
func StopPID(gracePeriod, killTimeout time.Duration, pid int) (error, bool) {
	//nolint:gocritic // Fresh context for controlled shutdown timing
	termCtx, termCancel := context.WithTimeout(context.Background(), gracePeriod)
	exited := TerminatePID(termCtx, pid)
	termCancel()

	if exited {
		return nil, true
	}

	//nolint:gocritic // Fresh context for kill timeout
	killCtx, killCancel := context.WithTimeout(context.Background(), killTimeout)
	err, killed := KillPID(killCtx, pid)
	killCancel()

	return err, killed
}

// isProcessGone returns true if the error indicates the process doesn't exist.
func isProcessGone(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrProcessDone) {
		return true
	}
	if errors.Is(err, syscall.ESRCH) {
		return true
	}
	// Fallback to string matching for edge cases
	errMsg := err.Error()
	return strings.Contains(errMsg, "no such process") ||
		strings.Contains(errMsg, "process already finished")
}

// waitForProcessExit polls until the process exits or context is done.
// Returns true if process exited, false if context was cancelled/timed out.
func waitForProcessExit(ctx context.Context, process *os.Process) bool {
	// Check immediately before starting the ticker
	if err := process.Signal(syscall.Signal(0)); err != nil {
		return true
	}

	pollInterval := 50 * time.Millisecond
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if err := process.Signal(syscall.Signal(0)); err != nil {
				return true
			}
		}
	}
}
