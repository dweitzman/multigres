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

package utils

import (
	"context"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

const (
	// DefaultWaitDelay is the default time to wait between SIGTERM and SIGKILL
	// when terminating a process in tests.
	DefaultWaitDelay = 200 * time.Millisecond
)

// CommandContext creates a command with graceful termination automatically configured.
// It sets up:
// - Cancel function that sends SIGTERM
// - WaitDelay for the grace period before SIGKILL
// - MULTIGRES_TESTDATA_DIR environment variable (if testDataDir is non-empty)
//
// When the provided context is cancelled, the command will:
// 1. Receive SIGTERM via the Cancel function
// 2. Wait for DefaultWaitDelay
// 3. Receive SIGKILL if still running (handled automatically by Go)
//
// Usage:
//
//	ctx, cancel := context.WithCancel(t.Context())
//	defer cancel()
//	cmd := utils.CommandContext(t, ctx, "", "myapp", "arg1", "arg2")
//	cmd.Env = append(cmd.Env, "MY_VAR=value") // Optional: add more env vars
//	require.NoError(t, cmd.Start())
//	// ... test runs ...
//	// When defer executes, cancel() triggers graceful shutdown
//	_ = cmd.Wait()
//
// Usage with test data directory:
//
//	ctx, cancel := context.WithCancel(t.Context())
//	defer cancel()
//	cmd := utils.CommandContext(t, ctx, tempDir, "myapp")
//	require.NoError(t, cmd.Start())
//	// MULTIGRES_TESTDATA_DIR is automatically set to tempDir
func CommandContext(t testing.TB, ctx context.Context, testDataDir string, name string, arg ...string) *exec.Cmd {
	t.Helper()

	cmd := exec.CommandContext(ctx, name, arg...)

	// Set up graceful termination with SIGTERM
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return cmd.Process.Signal(syscall.SIGTERM)
		}
		return nil
	}
	cmd.WaitDelay = DefaultWaitDelay

	// Always initialize cmd.Env with current environment to allow callers to safely append
	cmd.Env = os.Environ()

	// Add test data directory to environment if provided
	if testDataDir != "" {
		cmd.Env = append(cmd.Env, "MULTIGRES_TESTDATA_DIR="+testDataDir)
	}

	return cmd
}
