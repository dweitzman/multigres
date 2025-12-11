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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTerminatePID_GracefulExit(t *testing.T) {
	// Start a process that exits on SIGTERM
	cmd := Command("sleep", "60")
	err := cmd.Start(context.Background())
	require.NoError(t, err)

	pid := cmd.Process().Pid

	// Terminate with grace period
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	start := time.Now()
	err = TerminatePID(ctx, pid)
	elapsed := time.Since(start)

	require.NoError(t, err)
	// Should exit quickly on SIGTERM (sleep respects it)
	assert.Less(t, elapsed, 500*time.Millisecond)
}

func TestTerminatePID_RequiresKill(t *testing.T) {
	// Start a process that ignores SIGTERM using bash with a loop
	// (sh -c with simple sleep doesn't properly handle trap)
	cmd := Command("bash", "-c", "trap '' TERM; while true; do sleep 1; done")
	err := cmd.Start(context.Background())
	require.NoError(t, err)

	pid := cmd.Process().Pid

	// Give the process time to set up the trap
	time.Sleep(50 * time.Millisecond)

	// Terminate with short grace period
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = TerminatePID(ctx, pid)
	elapsed := time.Since(start)

	require.NoError(t, err)
	// Should take approximately the grace period before SIGKILL
	assert.Greater(t, elapsed, 50*time.Millisecond)
	assert.Less(t, elapsed, 500*time.Millisecond)
}

func TestTerminatePID_NonExistent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Use an invalid PID (process should not exist)
	err := TerminatePID(ctx, 999999)
	require.NoError(t, err) // Should not error for non-existent process
}

func TestTerminateProcess_Nil(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := TerminateProcess(ctx, nil)
	require.NoError(t, err)
}

func TestTerminateProcess_GracefulExit(t *testing.T) {
	// Start a process that exits on SIGTERM
	cmd := Command("sleep", "60")
	err := cmd.Start(context.Background())
	require.NoError(t, err)

	process := cmd.Process()

	// Terminate with grace period
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = TerminateProcess(ctx, process)
	require.NoError(t, err)
}
