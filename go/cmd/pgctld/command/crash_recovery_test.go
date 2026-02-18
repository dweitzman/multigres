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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

func TestReadPostmasterPID_ValidFile(t *testing.T) {
	tmpDir := t.TempDir()
	postmasterPath := filepath.Join(tmpDir, "postmaster.pid")

	// Write valid postmaster.pid with typical format
	pid := 12345
	content := fmt.Sprintf("%d\n/data\n1234567890\n5432\n/tmp\n*\n", pid)
	require.NoError(t, os.WriteFile(postmasterPath, []byte(content), 0o644))

	readPID, err := readPostmasterPID(tmpDir)
	require.NoError(t, err)
	require.Equal(t, pid, readPID)
}

func TestReadPostmasterPID_MissingFile(t *testing.T) {
	tmpDir := t.TempDir()

	_, err := readPostmasterPID(tmpDir)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err), "should return os.IsNotExist error")
}

func TestReadPostmasterPID_InvalidContent(t *testing.T) {
	tmpDir := t.TempDir()
	postmasterPath := filepath.Join(tmpDir, "postmaster.pid")

	// Write invalid PID (not a number)
	require.NoError(t, os.WriteFile(postmasterPath, []byte("not-a-number\n"), 0o644))

	_, err := readPostmasterPID(tmpDir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid PID")
}

func TestStringToClusterState(t *testing.T) {
	tests := []struct {
		input    string
		expected pb.DatabaseClusterState
	}{
		{"shut down", pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUT_DOWN},
		{"shut down in recovery", pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUT_DOWN_IN_RECOVERY},
		{"in production", pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_PRODUCTION},
		{"shutting down", pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUTTING_DOWN},
		{"in crash recovery", pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_CRASH_RECOVERY},
		{"unknown", pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_UNKNOWN},
		{"unexpected value", pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_UNKNOWN},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := stringToClusterState(tt.input)
			require.Equal(t, tt.expected, result)
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
			output: `pg_control version number:            1300
Catalog version number:               202107181
Database system identifier:           1234567890123456789
Database cluster state:               in production
pg_control last modified:             Mon Jan 15 10:30:45 2024`,
			expected: "in production",
		},
		{
			name: "shut down",
			output: `pg_control version number:            1300
Catalog version number:               202107181
Database system identifier:           1234567890123456789
Database cluster state:               shut down
pg_control last modified:             Mon Jan 15 10:30:45 2024`,
			expected: "shut down",
		},
		{
			name: "shut down in recovery",
			output: `pg_control version number:            1300
Database cluster state:               shut down in recovery
Latest checkpoint location:           0/123456`,
			expected: "shut down in recovery",
		},
		{
			name: "shutting down",
			output: `pg_control version number:            1300
Database cluster state:               shutting down
Latest checkpoint location:           0/123456`,
			expected: "shutting down",
		},
		{
			name: "in crash recovery",
			output: `pg_control version number:            1300
Database cluster state:               in crash recovery
Latest checkpoint location:           0/123456`,
			expected: "in crash recovery",
		},
		{
			name:     "missing line",
			output:   `pg_control version number:            1300\nCatalog version number:               202107181`,
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
			require.Equal(t, tt.expected, result)
		})
	}
}
