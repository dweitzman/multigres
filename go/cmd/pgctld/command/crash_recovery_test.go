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
	"testing"

	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

func TestExtractClusterState(t *testing.T) {
	tests := []struct {
		name          string
		output        string
		expectedState pgctldpb.DatabaseClusterState
		expectError   bool
	}{
		{
			name: "in production",
			output: `pg_control version number:            1300
Catalog version number:               202107181
Database cluster state:               in production
Latest checkpoint's TimeLineID:       1`,
			expectedState: pgctldpb.DatabaseClusterState_IN_PRODUCTION,
			expectError:   false,
		},
		{
			name: "shut down",
			output: `pg_control version number:            1300
Catalog version number:               202107181
Database cluster state:               shut down
Latest checkpoint's TimeLineID:       1`,
			expectedState: pgctldpb.DatabaseClusterState_SHUT_DOWN,
			expectError:   false,
		},
		{
			name: "shut down in recovery",
			output: `pg_control version number:            1300
Catalog version number:               202107181
Database cluster state:               shut down in recovery
Latest checkpoint's TimeLineID:       1`,
			expectedState: pgctldpb.DatabaseClusterState_SHUT_DOWN_IN_RECOVERY,
			expectError:   false,
		},
		{
			name: "in crash recovery",
			output: `pg_control version number:            1300
Catalog version number:               202107181
Database cluster state:               in crash recovery
Latest checkpoint's TimeLineID:       1`,
			expectedState: pgctldpb.DatabaseClusterState_IN_CRASH_RECOVERY,
			expectError:   false,
		},
		{
			name: "in archive recovery",
			output: `pg_control version number:            1300
Catalog version number:               202107181
Database cluster state:               in archive recovery
Latest checkpoint's TimeLineID:       1`,
			expectedState: pgctldpb.DatabaseClusterState_IN_ARCHIVE_RECOVERY,
			expectError:   false,
		},
		{
			name: "shutting down",
			output: `pg_control version number:            1300
Catalog version number:               202107181
Database cluster state:               shutting down
Latest checkpoint's TimeLineID:       1`,
			expectedState: pgctldpb.DatabaseClusterState_SHUTTING_DOWN,
			expectError:   false,
		},
		{
			name: "missing field",
			output: `pg_control version number:            1300
Catalog version number:               202107181
Latest checkpoint's TimeLineID:       1`,
			expectedState: pgctldpb.DatabaseClusterState_STATE_UNKNOWN,
			expectError:   true,
		},
		{
			name: "unknown state",
			output: `pg_control version number:            1300
Catalog version number:               202107181
Database cluster state:               unknown state value
Latest checkpoint's TimeLineID:       1`,
			expectedState: pgctldpb.DatabaseClusterState_STATE_UNKNOWN,
			expectError:   false,
		},
		{
			name:          "empty output",
			output:        "",
			expectedState: pgctldpb.DatabaseClusterState_STATE_UNKNOWN,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, err := extractClusterState(tt.output)
			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if state != tt.expectedState {
				t.Errorf("expected state %v, got %v", tt.expectedState, state)
			}
		})
	}
}

func TestStringToClusterState(t *testing.T) {
	tests := []struct {
		input    string
		expected pgctldpb.DatabaseClusterState
	}{
		{"in production", pgctldpb.DatabaseClusterState_IN_PRODUCTION},
		{"shut down", pgctldpb.DatabaseClusterState_SHUT_DOWN},
		{"shut down in recovery", pgctldpb.DatabaseClusterState_SHUT_DOWN_IN_RECOVERY},
		{"shutting down", pgctldpb.DatabaseClusterState_SHUTTING_DOWN},
		{"in crash recovery", pgctldpb.DatabaseClusterState_IN_CRASH_RECOVERY},
		{"in archive recovery", pgctldpb.DatabaseClusterState_IN_ARCHIVE_RECOVERY},
		{"unknown", pgctldpb.DatabaseClusterState_STATE_UNKNOWN},
		{"", pgctldpb.DatabaseClusterState_STATE_UNKNOWN},
		{"garbage", pgctldpb.DatabaseClusterState_STATE_UNKNOWN},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := stringToClusterState(tt.input)
			if got != tt.expected {
				t.Errorf("stringToClusterState(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestNeedsCrashRecovery(t *testing.T) {
	tests := []struct {
		name           string
		clusterState   pgctldpb.DatabaseClusterState
		expectRecovery bool
	}{
		{
			name:           "in crash recovery",
			clusterState:   pgctldpb.DatabaseClusterState_IN_CRASH_RECOVERY,
			expectRecovery: true,
		},
		{
			name:           "shut down - no recovery needed",
			clusterState:   pgctldpb.DatabaseClusterState_SHUT_DOWN,
			expectRecovery: false,
		},
		{
			name:           "shut down in recovery - no recovery needed",
			clusterState:   pgctldpb.DatabaseClusterState_SHUT_DOWN_IN_RECOVERY,
			expectRecovery: false,
		},
		{
			name:           "in production - no recovery needed",
			clusterState:   pgctldpb.DatabaseClusterState_IN_PRODUCTION,
			expectRecovery: false,
		},
		{
			name:           "in archive recovery - no recovery needed",
			clusterState:   pgctldpb.DatabaseClusterState_IN_ARCHIVE_RECOVERY,
			expectRecovery: false,
		},
		{
			name:           "shutting down - no recovery needed",
			clusterState:   pgctldpb.DatabaseClusterState_SHUTTING_DOWN,
			expectRecovery: false,
		},
		{
			name:           "unknown state - no recovery needed",
			clusterState:   pgctldpb.DatabaseClusterState_STATE_UNKNOWN,
			expectRecovery: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			needsRecovery := needsCrashRecovery(tt.clusterState)
			if needsRecovery != tt.expectRecovery {
				t.Errorf("needsCrashRecovery(%v) = %v, want %v", tt.clusterState, needsRecovery, tt.expectRecovery)
			}
		})
	}
}
