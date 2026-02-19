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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/pgctld"
)

func TestStartPostgreSQLWithConfig_CrashRecoveryNeeded(t *testing.T) {
	baseDir, cleanup := testutil.TempDir(t, "pgctld_crash_recovery_test")
	defer cleanup()

	// Create initialized data directory
	testutil.CreateDataDir(t, baseDir, true)

	// Setup mock binaries
	binDir := filepath.Join(baseDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Mock pg_controldata to return "in production" state (needs recovery)
	pgControldataScript := `#!/bin/sh
cat <<EOF
pg_control version number:            1301
Catalog version number:               202209061
Database system identifier:           7123456789012345678
Database cluster state:               in production
pg_control last modified:             Mon Jan 15 10:30:45 2024
Latest checkpoint location:           0/1234567
EOF
`
	testutil.MockBinary(t, binDir, "pg_controldata", pgControldataScript)

	// Add to PATH for test
	originalPath := os.Getenv("PATH")
	os.Setenv("PATH", binDir+":"+originalPath)
	defer os.Setenv("PATH", originalPath)

	// Create config
	config, err := pgctld.NewPostgresCtlConfig(
		5432,
		"postgres",
		"postgres",
		30,
		pgctld.PostgresDataDir(baseDir),
		pgctld.PostgresConfigFile(baseDir),
		baseDir,
		"localhost",
		pgctld.PostgresSocketDir(baseDir),
	)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Attempt to start - should fail with typed error
	err = startPostgreSQLWithConfig(logger, config)

	// Verify we get the typed error with correct cluster state
	require.Error(t, err)

	var crashRecoveryErr *CrashRecoveryNeededError
	ok := errors.As(err, &crashRecoveryErr)
	require.True(t, ok, "Expected CrashRecoveryNeededError, got %T", err)

	// Type-safe check: verify the cluster state is correct
	assert.Equal(t, pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_PRODUCTION, crashRecoveryErr.ClusterState)
}

func TestStartPostgreSQLWithConfig_CleanDatabase(t *testing.T) {
	baseDir, cleanup := testutil.TempDir(t, "pgctld_clean_start_test")
	defer cleanup()

	// Create initialized data directory
	testutil.CreateDataDir(t, baseDir, true)

	// Setup mock binaries
	binDir := filepath.Join(baseDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Mock pg_controldata to return "shut down" state (clean)
	pgControldataScript := `#!/bin/sh
cat <<EOF
pg_control version number:            1301
Catalog version number:               202209061
Database system identifier:           7123456789012345678
Database cluster state:               shut down
pg_control last modified:             Mon Jan 15 10:30:45 2024
Latest checkpoint location:           0/1234567
EOF
`
	testutil.MockBinary(t, binDir, "pg_controldata", pgControldataScript)

	// Add to PATH for test
	originalPath := os.Getenv("PATH")
	os.Setenv("PATH", binDir+":"+originalPath)
	defer os.Setenv("PATH", originalPath)

	// Create config
	config, err := pgctld.NewPostgresCtlConfig(
		5432,
		"postgres",
		"postgres",
		30,
		pgctld.PostgresDataDir(baseDir),
		pgctld.PostgresConfigFile(baseDir),
		baseDir,
		"localhost",
		pgctld.PostgresSocketDir(baseDir),
	)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Attempt to start - should succeed past crash recovery check
	// (pg_ctl will fail but that's expected in mock env)
	err = startPostgreSQLWithConfig(logger, config)
	// We expect pg_ctl to fail in mock environment, but crash recovery check should pass
	// The error should NOT be a CrashRecoveryNeededError
	if err != nil {
		var crashRecoveryErr *CrashRecoveryNeededError
		ok := errors.As(err, &crashRecoveryErr)
		assert.False(t, ok, "Should not get CrashRecoveryNeededError for clean database, got: %v", err)
	}
}

func TestStartPostgreSQLWithConfig_ShuttingDownState(t *testing.T) {
	baseDir, cleanup := testutil.TempDir(t, "pgctld_shutting_down_test")
	defer cleanup()

	// Create initialized data directory
	testutil.CreateDataDir(t, baseDir, true)

	// Setup mock binaries
	binDir := filepath.Join(baseDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Mock pg_controldata to return "shutting down" state (needs recovery)
	pgControldataScript := `#!/bin/sh
cat <<EOF
pg_control version number:            1301
Catalog version number:               202209061
Database system identifier:           7123456789012345678
Database cluster state:               shutting down
pg_control last modified:             Mon Jan 15 10:30:45 2024
Latest checkpoint location:           0/1234567
EOF
`
	testutil.MockBinary(t, binDir, "pg_controldata", pgControldataScript)

	// Add to PATH for test
	originalPath := os.Getenv("PATH")
	os.Setenv("PATH", binDir+":"+originalPath)
	defer os.Setenv("PATH", originalPath)

	// Create config
	config, err := pgctld.NewPostgresCtlConfig(
		5432,
		"postgres",
		"postgres",
		30,
		pgctld.PostgresDataDir(baseDir),
		pgctld.PostgresConfigFile(baseDir),
		baseDir,
		"localhost",
		pgctld.PostgresSocketDir(baseDir),
	)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Attempt to start - should fail with typed error
	err = startPostgreSQLWithConfig(logger, config)

	// Verify we get the typed error
	require.Error(t, err)

	var crashRecoveryErr *CrashRecoveryNeededError
	ok := errors.As(err, &crashRecoveryErr)
	require.True(t, ok, "Expected CrashRecoveryNeededError, got %T", err)

	// Verify the cluster state is correct
	assert.Equal(t, pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUTTING_DOWN, crashRecoveryErr.ClusterState)
}

func TestStartPostgreSQLWithConfig_InCrashRecoveryState(t *testing.T) {
	baseDir, cleanup := testutil.TempDir(t, "pgctld_in_recovery_test")
	defer cleanup()

	// Create initialized data directory
	testutil.CreateDataDir(t, baseDir, true)

	// Setup mock binaries
	binDir := filepath.Join(baseDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Mock pg_controldata to return "in crash recovery" state (needs recovery)
	pgControldataScript := `#!/bin/sh
cat <<EOF
pg_control version number:            1301
Catalog version number:               202209061
Database system identifier:           7123456789012345678
Database cluster state:               in crash recovery
pg_control last modified:             Mon Jan 15 10:30:45 2024
Latest checkpoint location:           0/1234567
EOF
`
	testutil.MockBinary(t, binDir, "pg_controldata", pgControldataScript)

	// Add to PATH for test
	originalPath := os.Getenv("PATH")
	os.Setenv("PATH", binDir+":"+originalPath)
	defer os.Setenv("PATH", originalPath)

	// Create config
	config, err := pgctld.NewPostgresCtlConfig(
		5432,
		"postgres",
		"postgres",
		30,
		pgctld.PostgresDataDir(baseDir),
		pgctld.PostgresConfigFile(baseDir),
		baseDir,
		"localhost",
		pgctld.PostgresSocketDir(baseDir),
	)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Attempt to start - should fail with typed error
	err = startPostgreSQLWithConfig(logger, config)

	// Verify we get the typed error
	require.Error(t, err)

	var crashRecoveryErr *CrashRecoveryNeededError
	ok := errors.As(err, &crashRecoveryErr)
	require.True(t, ok, "Expected CrashRecoveryNeededError, got %T", err)

	// Verify the cluster state is correct
	assert.Equal(t, pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_CRASH_RECOVERY, crashRecoveryErr.ClusterState)
}

func TestNeedsCrashRecovery(t *testing.T) {
	tests := []struct {
		name                string
		pgControldataOutput string
		expectNeedsRecovery bool
		expectedState       pb.DatabaseClusterState
		expectError         bool
	}{
		{
			name: "in production - needs recovery",
			pgControldataOutput: `pg_control version number:            1301
Database cluster state:               in production
`,
			expectNeedsRecovery: true,
			expectedState:       pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_PRODUCTION,
			expectError:         false,
		},
		{
			name: "shutting down - needs recovery",
			pgControldataOutput: `pg_control version number:            1301
Database cluster state:               shutting down
`,
			expectNeedsRecovery: true,
			expectedState:       pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUTTING_DOWN,
			expectError:         false,
		},
		{
			name: "in crash recovery - needs recovery",
			pgControldataOutput: `pg_control version number:            1301
Database cluster state:               in crash recovery
`,
			expectNeedsRecovery: true,
			expectedState:       pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_CRASH_RECOVERY,
			expectError:         false,
		},
		{
			name: "shut down - clean",
			pgControldataOutput: `pg_control version number:            1301
Database cluster state:               shut down
`,
			expectNeedsRecovery: false,
			expectedState:       pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUT_DOWN,
			expectError:         false,
		},
		{
			name: "shut down in recovery - clean",
			pgControldataOutput: `pg_control version number:            1301
Database cluster state:               shut down in recovery
`,
			expectNeedsRecovery: false,
			expectedState:       pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUT_DOWN_IN_RECOVERY,
			expectError:         false,
		},
		{
			name: "unknown state - error",
			pgControldataOutput: `pg_control version number:            1301
Database cluster state:               something weird
`,
			expectNeedsRecovery: false,
			expectedState:       pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_UNKNOWN,
			expectError:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_needs_recovery_test")
			defer cleanup()

			// Create data directory
			testutil.CreateDataDir(t, baseDir, true)

			// Setup mock binaries
			binDir := filepath.Join(baseDir, "bin")
			require.NoError(t, os.MkdirAll(binDir, 0o755))

			// Mock pg_controldata with the test output
			pgControldataScript := "#!/bin/sh\ncat <<EOF\n" + tt.pgControldataOutput + "\nEOF\n"
			testutil.MockBinary(t, binDir, "pg_controldata", pgControldataScript)

			// Add to PATH for test
			originalPath := os.Getenv("PATH")
			os.Setenv("PATH", binDir+":"+originalPath)
			defer os.Setenv("PATH", originalPath)

			logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

			needsRecovery, state, err := needsCrashRecovery(context.Background(), logger, baseDir)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expectNeedsRecovery, needsRecovery)
			assert.Equal(t, tt.expectedState, state)
		})
	}
}

func TestExtractClusterState(t *testing.T) {
	tests := []struct {
		name     string
		output   string
		expected string
	}{
		{
			name: "in production",
			output: `pg_control version number:            1301
Catalog version number:               202209061
Database system identifier:           7123456789012345678
Database cluster state:               in production
pg_control last modified:             Mon Jan 15 10:30:45 2024`,
			expected: "in production",
		},
		{
			name: "shut down",
			output: `pg_control version number:            1301
Database cluster state:               shut down
`,
			expected: "shut down",
		},
		{
			name: "shutting down with extra spaces",
			output: `Database cluster state:                shutting down
`,
			expected: "shutting down",
		},
		{
			name:     "no cluster state line",
			output:   `pg_control version number: 1301`,
			expected: "unknown",
		},
		{
			name:     "empty output",
			output:   "",
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractClusterState(tt.output)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestStringToClusterState(t *testing.T) {
	tests := []struct {
		name     string
		state    string
		expected pb.DatabaseClusterState
	}{
		{
			name:     "shut down",
			state:    "shut down",
			expected: pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUT_DOWN,
		},
		{
			name:     "shut down in recovery",
			state:    "shut down in recovery",
			expected: pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUT_DOWN_IN_RECOVERY,
		},
		{
			name:     "in production",
			state:    "in production",
			expected: pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_PRODUCTION,
		},
		{
			name:     "shutting down",
			state:    "shutting down",
			expected: pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUTTING_DOWN,
		},
		{
			name:     "in crash recovery",
			state:    "in crash recovery",
			expected: pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_CRASH_RECOVERY,
		},
		{
			name:     "unknown state",
			state:    "something weird",
			expected: pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_UNKNOWN,
		},
		{
			name:     "empty string",
			state:    "",
			expected: pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_UNKNOWN,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stringToClusterState(tt.state)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCrashRecoveryNeededError_ErrorMessage(t *testing.T) {
	err := &CrashRecoveryNeededError{
		ClusterState: pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_PRODUCTION,
	}

	// Verify exact error message format
	expected := "database requires crash recovery (cluster state: DATABASE_CLUSTER_STATE_IN_PRODUCTION). Run crash recovery first, then retry start"
	assert.Equal(t, expected, err.Error())
}

func TestReadPostmasterPID_ValidFile(t *testing.T) {
	tmpDir := t.TempDir()
	postmasterPath := filepath.Join(tmpDir, "postmaster.pid")

	// Write valid postmaster.pid with typical format
	// Format: PID\ndata_dir\nstart_time\nport\nsocket_dir\nlisten_addresses\n
	pid := 12345
	content := fmt.Sprintf("%d\n/data\n1234567890\n5432\n/tmp\n*\n", pid)
	require.NoError(t, os.WriteFile(postmasterPath, []byte(content), 0o644))

	readPID, err := readPostmasterPID(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, pid, readPID)
}

func TestReadPostmasterPID_MissingFile(t *testing.T) {
	tmpDir := t.TempDir()

	_, err := readPostmasterPID(tmpDir)
	require.Error(t, err)
}

func TestReadPostmasterPID_InvalidContent(t *testing.T) {
	tmpDir := t.TempDir()
	postmasterPath := filepath.Join(tmpDir, "postmaster.pid")

	// Write invalid PID (not a number)
	require.NoError(t, os.WriteFile(postmasterPath, []byte("not-a-number\n"), 0o644))

	_, err := readPostmasterPID(tmpDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid PID")
}

func TestReadPostmasterPID_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	postmasterPath := filepath.Join(tmpDir, "postmaster.pid")

	// Write empty file
	require.NoError(t, os.WriteFile(postmasterPath, []byte(""), 0o644))

	_, err := readPostmasterPID(tmpDir)
	require.Error(t, err)
	// Empty file results in invalid PID error (empty string can't be parsed as int)
	assert.Contains(t, err.Error(), "invalid PID")
}
