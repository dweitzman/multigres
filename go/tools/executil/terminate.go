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
// If ctx expires before the process exits, sends SIGKILL and waits for exit.
// Returns nil if process is nil or was already dead when SIGTERM was attempted.
func TerminateProcess(ctx context.Context, process *os.Process) error {
	if process == nil {
		return nil
	}
	return TerminatePID(ctx, process.Pid)
}

// TerminatePID sends SIGTERM to a process by PID and waits for graceful exit.
// If ctx expires before the process exits, sends SIGKILL and waits for exit.
// Returns nil if the process was already dead when SIGTERM was attempted.
func TerminatePID(ctx context.Context, pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return nil
	}

	// Send SIGTERM
	if err := process.Signal(syscall.SIGTERM); err != nil {
		if isProcessGone(err) {
			return nil
		}
		// SIGTERM failed for unexpected reason, try SIGKILL immediately
		return killProcess(process)
	}

	// Wait for process to exit gracefully
	if waitForProcessExit(ctx, process) {
		return nil
	}

	// Context expired - escalate to SIGKILL
	if err := killProcess(process); err != nil {
		return err
	}

	// Wait for process to fully exit after SIGKILL (with a reasonable timeout)
	killCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitForProcessExit(killCtx, process)

	return nil
}

// killProcess sends SIGKILL to a process, returning nil if the process is already gone.
func killProcess(process *os.Process) error {
	if err := process.Kill(); err != nil {
		if isProcessGone(err) {
			return nil
		}
		return err
	}
	return nil
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
