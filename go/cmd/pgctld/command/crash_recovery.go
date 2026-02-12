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

package command

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// needsCrashRecovery checks if the given cluster state indicates crash recovery is needed.
// For PostgreSQL, check database cluster state.
// States that indicate need for crash recovery:
// - "in production" - was running when killed
// - "shutting down" - was shutting down when killed
// - "in crash recovery" - already in crash recovery
// Clean states: "shut down", "shut down in recovery"
func needsCrashRecovery(state pgctldpb.DatabaseClusterState) bool {
	return state == pgctldpb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_PRODUCTION ||
		state == pgctldpb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUTTING_DOWN ||
		state == pgctldpb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_CRASH_RECOVERY
}

// extractClusterState parses pg_controldata output and extracts the database cluster state.
// It looks for the "Database cluster state:" line and returns the corresponding enum value.
func extractClusterState(pgControldataOutput string) (pgctldpb.DatabaseClusterState, error) {
	const prefix = "Database cluster state:"

	for line := range strings.SplitSeq(pgControldataOutput, "\n") {
		line = strings.TrimSpace(line)
		stateStr, found := strings.CutPrefix(line, prefix)
		if found {
			// Extract the state value after the prefix
			stateStr = strings.TrimSpace(stateStr)
			return stringToClusterState(stateStr), nil
		}
	}

	return pgctldpb.DatabaseClusterState_DATABASE_CLUSTER_STATE_UNKNOWN, errors.New("Database cluster state not found in pg_controldata output")
}

// stringToClusterState converts a pg_controldata cluster state string to the enum value.
func stringToClusterState(state string) pgctldpb.DatabaseClusterState {
	switch state {
	case "shut down":
		return pgctldpb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUT_DOWN
	case "shut down in recovery":
		return pgctldpb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUT_DOWN_IN_RECOVERY
	case "in production":
		return pgctldpb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_PRODUCTION
	case "shutting down":
		return pgctldpb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUTTING_DOWN
	case "in crash recovery":
		return pgctldpb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_CRASH_RECOVERY
	default:
		return pgctldpb.DatabaseClusterState_DATABASE_CLUSTER_STATE_UNKNOWN
	}
}

// runPgControldata executes pg_controldata and returns the output.
func (s *PgCtldService) runPgControldata(ctx context.Context) (string, error) {
	pgControldataPath, err := exec.LookPath("pg_controldata")
	if err != nil {
		return "", fmt.Errorf("pg_controldata not found in PATH: %w", err)
	}

	cmd := exec.CommandContext(ctx, pgControldataPath, s.config.PostgresDataDir)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("pg_controldata failed: %w (stderr: %s)", err, stderr.String())
	}

	return stdout.String(), nil
}

// runCrashRecovery runs postgres in single-user mode to complete crash recovery.
// It writes to stdin with a 50ms delay to ensure stability.
func (s *PgCtldService) runCrashRecovery(ctx context.Context) (string, error) {
	postgresPath, err := exec.LookPath("postgres")
	if err != nil {
		return "", fmt.Errorf("postgres not found in PATH: %w", err)
	}

	// Run postgres --single to complete crash recovery
	// Format: postgres --single -D <datadir> <database>
	cmd := exec.CommandContext(ctx, postgresPath, "--single", "-D", s.config.PostgresDataDir, "postgres")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Get stdin pipe
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return "", fmt.Errorf("failed to get stdin pipe: %w", err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("failed to start postgres --single: %w", err)
	}

	// Wait 50ms before writing to stdin for stability
	time.Sleep(50 * time.Millisecond)

	// Send newline to stdin to trigger crash recovery
	if _, err := stdin.Write([]byte("\n")); err != nil {
		return "", fmt.Errorf("failed to write to stdin: %w", err)
	}

	// Close stdin to signal completion
	stdin.Close()

	// Wait for command to complete
	if err := cmd.Wait(); err != nil {
		return "", fmt.Errorf("postgres --single failed: %w (stderr: %s)", err, stderr.String())
	}

	return stdout.String(), nil
}
