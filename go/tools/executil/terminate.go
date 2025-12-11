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
	"syscall"
	"time"

	"github.com/multigres/multigres/go/tools/retry"
)

// TerminateProcess sends SIGTERM, waits for context to expire, then SIGKILL if needed.
// Returns nil if process terminates (including if already dead).
//
// Usage:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	executil.TerminateProcess(ctx, process)
func TerminateProcess(ctx context.Context, process *os.Process) error {
	if process == nil {
		return nil
	}
	return TerminatePID(ctx, process.Pid)
}

// TerminatePID sends SIGTERM to a PID, waits for context to expire, then SIGKILL if needed.
// Returns nil if process terminates (including if it doesn't exist).
//
// The context deadline/timeout controls the grace period between SIGTERM and SIGKILL.
// If the context has no deadline, waits indefinitely for the process to exit after SIGTERM.
//
// Note: This function attempts to reap the process to avoid zombies, but this only works
// if the target process is a child of the current process. For non-child processes,
// the parent process is responsible for reaping.
func TerminatePID(ctx context.Context, pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		//nolint:nilerr // Process not found is success for termination
		return nil
	}

	// Send SIGTERM
	if err := process.Signal(syscall.SIGTERM); err != nil {
		if isProcessNotExist(err) {
			return nil
		}
		// SIGTERM failed for other reason, try SIGKILL directly
		if err := killProcess(process); err != nil {
			return err
		}
		reapProcess(process)
		return nil
	}

	// Wait for process to exit or context to expire
	if waitForExit(ctx, process) {
		reapProcess(process)
		return nil
	}

	// Context expired (grace period over), send SIGKILL
	if err := killProcess(process); err != nil {
		return err
	}
	reapProcess(process)
	return nil
}

// killProcess sends SIGKILL to a process.
// Returns nil if the process is killed or doesn't exist.
func killProcess(process *os.Process) error {
	if err := process.Kill(); err != nil {
		if isProcessNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}

// reapProcess attempts to reap a terminated process to avoid zombies.
// This only succeeds if the process is a child of the current process.
// For non-child processes, this is a no-op (the parent must reap).
func reapProcess(process *os.Process) {
	// Wait() will fail for non-child processes, which is fine
	_, _ = process.Wait()
}

// isProcessNotExist returns true if the error indicates the process doesn't exist.
func isProcessNotExist(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH)
}

// waitForExit waits for a process to exit.
// Returns true if the process exited before the context expired.
//
// For child processes, this uses Wait() which properly detects exit and reaps the zombie.
// For non-child processes, Wait() fails and we fall back to Signal(0) polling,
// which cannot distinguish between running processes and zombies.
func waitForExit(ctx context.Context, process *os.Process) bool {
	// Try Wait() in a goroutine - this works for child processes and properly reaps them.
	waitDone := make(chan bool, 1)
	go func() {
		_, err := process.Wait()
		// Wait() succeeds for child processes, fails for non-child
		waitDone <- (err == nil)
	}()

	select {
	case success := <-waitDone:
		if success {
			return true // Child process exited and was reaped
		}
		// Not a child process - fall back to Signal(0) polling
		return waitForExitPolling(ctx, process)
	case <-ctx.Done():
		return false // Context expired
	}
}

// waitForExitPolling polls with Signal(0) to detect process exit.
// This is used for non-child processes where Wait() doesn't work.
// Note: This cannot distinguish between running processes and zombies.
func waitForExitPolling(ctx context.Context, process *os.Process) bool {
	r := retry.New(10*time.Millisecond, 100*time.Millisecond)
	for _, err := range r.Attempts(ctx) {
		if err != nil {
			// Context expired
			return false
		}
		// Signal(0) checks if process exists without sending signal
		if err := process.Signal(syscall.Signal(0)); err != nil {
			return true // Process exited
		}
	}
	return false
}
