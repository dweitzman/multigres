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

//go:build !short

package command

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMain sets up orphan detection for all tests in this package
func TestMain(m *testing.M) {
	// Enable orphan detection by setting MULTIGRES_TEST_PARENT_PID
	// This allows the watchdog script to monitor the test process and cleanup if it dies
	os.Setenv("MULTIGRES_TEST_PARENT_PID", strconv.Itoa(os.Getpid()))

	// Add endtoend directory to PATH so run_command_if_parent_dies.sh can be found
	endtoendDir := filepath.Join("..", "..", "..", "test", "endtoend")
	if absPath, err := filepath.Abs(endtoendDir); err == nil {
		os.Setenv("PATH", absPath+":"+os.Getenv("PATH"))
	}

	// Run tests
	os.Exit(m.Run()) //nolint:forbidigo // TestMain needs os.Exit
}

// setupTestPostgres creates a real PostgreSQL data directory for testing
func setupTestPostgres(t *testing.T) (poolerDir string, config *pgctld.PostgresCtlConfig, cleanup func()) {
	t.Helper()

	// Use a short path to avoid Unix socket path length limits (103 bytes on Mac)
	tmpDir, err := os.MkdirTemp("/tmp", "pg_test_")
	require.NoError(t, err)

	poolerDir = tmpDir
	pgDataDir := filepath.Join(poolerDir, "pg_data")
	socketDir := filepath.Join(poolerDir, "pg_sockets") // Must match pgctld.PostgresSocketDir()
	confPath := filepath.Join(pgDataDir, "postgresql.conf")

	// Initialize PostgreSQL data directory
	initCmd := exec.Command("initdb", "-D", pgDataDir, "--no-sync")
	initCmd.Env = append(os.Environ(), "PGDATA="+pgDataDir)
	output, err := initCmd.CombinedOutput()
	require.NoError(t, err, "initdb failed: %s", string(output))

	// Create socket directory
	require.NoError(t, os.MkdirAll(socketDir, 0o755))

	// Write minimal postgresql.conf for faster startup
	testPort := utils.GetFreePort(t)
	conf := `
fsync = off
synchronous_commit = off
full_page_writes = off
max_connections = 10
shared_buffers = 128kB
`
	require.NoError(t, os.WriteFile(confPath, []byte(conf), 0o644))

	// Create PostgresCtlConfig for use with StartPostgreSQLWithResult
	postgresConfig, err := pgctld.NewPostgresCtlConfig(
		testPort,
		"postgres",
		"postgres",
		30, // timeout
		pgDataDir,
		confPath,
		poolerDir,
		"", // listen_addresses (empty = Unix socket only)
		socketDir,
	)
	require.NoError(t, err)

	cleanup = func() {
		// Stop postgres using the production StopPostgreSQLWithConfig function
		// This works even if postgres is already stopped
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		_ = StopPostgreSQLWithConfig(logger, postgresConfig, "fast")

		// Clean up temp directory
		os.RemoveAll(tmpDir)
	}

	return poolerDir, postgresConfig, cleanup
}

// startPostgres starts PostgreSQL using the production StartPostgreSQLWithResult function
// This automatically sets up orphan detection if environment variables are set
func startPostgres(t *testing.T, config *pgctld.PostgresCtlConfig) int {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	result, err := StartPostgreSQLWithResult(logger, config)
	if err != nil {
		// Read PostgreSQL log for debugging
		logPath := filepath.Join(config.PostgresDataDir, "postgresql.log")
		if logContent, readErr := os.ReadFile(logPath); readErr == nil {
			t.Logf("PostgreSQL log:\n%s", string(logContent))
		}
		require.NoError(t, err, "StartPostgreSQLWithResult failed")
	}

	require.NotZero(t, result.PID, "expected valid PID from StartPostgreSQLWithResult")
	return result.PID
}

// stopPostgres stops PostgreSQL using the production StopPostgreSQLWithConfig function
func stopPostgres(t *testing.T, config *pgctld.PostgresCtlConfig) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	err := StopPostgreSQLWithConfig(logger, config, "fast")
	if err != nil {
		t.Logf("Warning: stopPostgres failed (may already be stopped): %v", err)
	}
}

// killPostgres sends SIGKILL to simulate a crash
func killPostgres(t *testing.T, pid int) {
	t.Helper()

	process, err := os.FindProcess(pid)
	require.NoError(t, err)

	err = process.Signal(syscall.SIGKILL)
	require.NoError(t, err)

	// Wait for process to actually die
	require.Eventually(t, func() bool {
		return !isProcessRunning(pid)
	}, 5*time.Second, 10*time.Millisecond, "postgres process should die after SIGKILL")
}

func TestNeedsCrashRecovery_CleanShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	poolerDir, config, cleanup := setupTestPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	// Start postgres, then stop it cleanly
	startPostgres(t, config)
	stopPostgres(t, config)

	// Check if crash recovery is needed
	needsRecovery, state, err := needsCrashRecovery(ctx, logger, poolerDir)
	require.NoError(t, err)
	require.False(t, needsRecovery, "clean shutdown should not require crash recovery")
	require.Equal(t, pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUT_DOWN, state)
}

func TestNeedsCrashRecovery_CrashedPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	poolerDir, config, cleanup := setupTestPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	// Start postgres, then SIGKILL it to simulate crash
	pid := startPostgres(t, config)
	killPostgres(t, pid)

	// Check if crash recovery is needed
	needsRecovery, state, err := needsCrashRecovery(ctx, logger, poolerDir)
	require.NoError(t, err)
	require.True(t, needsRecovery, "crashed postgres should require crash recovery")
	require.Equal(t, pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_PRODUCTION, state)
}

func TestCrashRecoveryRPC_FullCycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	poolerDir, config, cleanup := setupTestPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Create service
	service := &PgCtldService{
		poolerDir: poolerDir,
		logger:    logger,
	}

	ctx := context.Background()

	// Start postgres, then SIGKILL it
	pid := startPostgres(t, config)
	killPostgres(t, pid)

	// Call CrashRecovery RPC
	resp, err := service.CrashRecovery(ctx, &pb.CrashRecoveryRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.RecoveryPerformed, "recovery should have been performed")
	require.Equal(t, pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_PRODUCTION, resp.StateBefore)
	require.Equal(t, pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUT_DOWN, resp.StateAfter)

	// Call again - should be idempotent
	resp2, err := service.CrashRecovery(ctx, &pb.CrashRecoveryRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp2)
	require.False(t, resp2.RecoveryPerformed, "recovery should not be performed again")
	require.Equal(t, pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUT_DOWN, resp2.StateBefore)
}
