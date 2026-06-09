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

package manager

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
)

func TestParseApplicationName(t *testing.T) {
	tests := []struct {
		name        string
		appName     string
		expected    *clustermetadatapb.ID
		expectError bool
	}{
		{
			name:    "Valid application name",
			appName: "us-west_replica-1",
			expected: &clustermetadatapb.ID{
				Cell: "us-west",
				Name: "replica-1",
			},
		},
		{
			name:    "Application name with underscores in name part",
			appName: "cell1_standby_server_1",
			expected: &clustermetadatapb.ID{
				Cell: "cell1",
				Name: "standby_server_1",
			},
		},
		{
			name:    "Simple cell and name",
			appName: "zone1_primary",
			expected: &clustermetadatapb.ID{
				Cell: "zone1",
				Name: "primary",
			},
		},
		{
			name:        "Missing underscore separator",
			appName:     "invalidname",
			expectError: true,
		},
		{
			name:        "Empty string",
			appName:     "",
			expectError: true,
		},
		{
			name:        "Only underscore",
			appName:     "_",
			expectError: true,
		},
		{
			name:        "Cell with empty name",
			appName:     "cell_",
			expectError: true,
		},
		{
			name:        "Empty cell with name",
			appName:     "_name",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := consensus.ParseApplicationName(tt.appName)

			if tt.expectError {
				require.Error(t, err, "Should return error for invalid format")
				assert.Nil(t, result, "Result should be nil when parsing fails")
			} else {
				require.NoError(t, err, "Should not return error for valid format")
				require.NotNil(t, result, "Result should not be nil")
				assert.Equal(t, tt.expected.Cell, result.Cell, "Cell should match")
				assert.Equal(t, tt.expected.Name, result.Name, "Name should match")
			}
		})
	}
}

func TestParseSynchronousStandbyNames(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		expected    *SyncStandbyConfig
		expectError bool
	}{
		{
			name:  "FIRST with single standby",
			value: `FIRST 1 ("cell1_replica1")`,
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync: 1,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "cell1", Name: "replica1"},
				},
			},
		},
		{
			name:  "FIRST with multiple standbys",
			value: `FIRST 2 ("us-west_replica1", "us-west_replica2", "us-east_replica1")`,
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync: 2,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "us-west", Name: "replica1"},
					{Cell: "us-west", Name: "replica2"},
					{Cell: "us-east", Name: "replica1"},
				},
			},
		},
		{
			name:  "ANY with multiple standbys",
			value: `ANY 1 ("zone1_standby1", "zone2_standby1")`,
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				NumSync: 1,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "zone1", Name: "standby1"},
					{Cell: "zone2", Name: "standby1"},
				},
			},
		},
		{
			name:        "Wildcard configuration not supported",
			value:       "*",
			expectError: true,
		},
		{
			name:        "Wildcard in FIRST not supported",
			value:       "FIRST 1 (*)",
			expectError: true,
		},
		{
			name:        "Wildcard in ANY not supported",
			value:       "ANY 2 (*)",
			expectError: true,
		},
		{
			name:  "FIRST with spaces around members",
			value: `FIRST 1 ( "cell_name1" , "cell_name2" )`,
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync: 1,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "cell", Name: "name1"},
					{Cell: "cell", Name: "name2"},
				},
			},
		},
		{
			name:  "ANY with higher num_sync",
			value: `ANY 3 ("a_b", "c_d", "e_f", "g_h")`,
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				NumSync: 3,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "a", Name: "b"},
					{Cell: "c", Name: "d"},
					{Cell: "e", Name: "f"},
					{Cell: "g", Name: "h"},
				},
			},
		},
		{
			name:        "Empty string",
			value:       "",
			expectError: true,
		},
		{
			name:        "Invalid format - missing parentheses",
			value:       "FIRST 1 cell_replica1",
			expectError: true,
		},
		{
			name:        "Invalid format - missing method",
			value:       "1 (cell_replica1)",
			expectError: true,
		},
		{
			name:        "Invalid format - missing num_sync",
			value:       `FIRST ("cell_replica1")`,
			expectError: true,
		},
		{
			name:        "Invalid format - empty member list",
			value:       "FIRST 1 ()",
			expectError: true,
		},
		{
			name:        "Invalid format - invalid num_sync",
			value:       `FIRST abc ("cell_replica1")`,
			expectError: true,
		},
		{
			name:        "Invalid method",
			value:       `QUORUM 1 ("cell_replica1")`,
			expectError: true,
		},
		{
			name:        "Invalid application name format in member",
			value:       `FIRST 1 ("invalidname")`,
			expectError: true,
		},
		{
			name:  "Members without quotes",
			value: "FIRST 1 (cell_replica1)",
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync: 1,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "cell", Name: "replica1"},
				},
			},
		},
		{
			name:  "Mixed quoted and unquoted",
			value: `FIRST 2 ("cell1_name1", cell2_name2)`,
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync: 2,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "cell1", Name: "name1"},
					{Cell: "cell2", Name: "name2"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseSynchronousStandbyNames(tt.value)

			if tt.expectError {
				require.Error(t, err, "Should return error for invalid format")
				assert.Nil(t, result, "Result should be nil when parsing fails")
			} else {
				require.NoError(t, err, "Should not return error for valid format")
				require.NotNil(t, result, "Result should not be nil")
				assert.Equal(t, tt.expected.Method, result.Method, "Method should match")
				assert.Equal(t, tt.expected.NumSync, result.NumSync, "NumSync should match")

				if tt.expected.StandbyIDs == nil {
					assert.Nil(t, result.StandbyIDs, "StandbyIDs should be nil")
				} else {
					require.Len(t, result.StandbyIDs, len(tt.expected.StandbyIDs), "StandbyIDs length should match")
					for i, expectedID := range tt.expected.StandbyIDs {
						assert.Equal(t, expectedID.Cell, result.StandbyIDs[i].Cell, "Cell should match at index %d", i)
						assert.Equal(t, expectedID.Name, result.StandbyIDs[i].Name, "Name should match at index %d", i)
					}
				}
			}
		})
	}
}
