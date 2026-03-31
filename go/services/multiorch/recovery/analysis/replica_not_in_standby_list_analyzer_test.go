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

package analysis

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestReplicaNotInStandbyListAnalyzer_Analyze(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()
	rpcClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(rpcClient, slog.Default())
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "cell1",
		Name:      "test-coord",
	}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(nil, poolerStore, rpcClient, ts, coord, slog.Default())

	analyzer := &ReplicaNotInStandbyListAnalyzer{factory: factory}
	shardKey := commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	require.Equal(t, types.CheckName("ReplicaNotInStandbyList"), analyzer.Name())

	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
	replica1ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"}

	// makeReplicaState creates a REPLICA pooler state with primary_conninfo set.
	makeReplicaState := func(id *clustermetadatapb.ID) *PoolerState {
		return &PoolerState{
			ID:             id,
			IsPrimary:      false,
			IsInitialized:  true,
			LastCheckValid: true,
			Type:           clustermetadatapb.PoolerType_REPLICA,
			Health: &multiorchdatapb.PoolerHealthState{
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
						Host: "primary.example.com",
					},
				},
			},
		}
	}

	// makePrimaryState creates a reachable primary with the given standby list.
	makePrimaryState := func(standbyIDs []*clustermetadatapb.ID) *PoolerState {
		return &PoolerState{
			ID:             primaryID,
			IsPrimary:      true,
			IsInitialized:  true,
			LastCheckValid: true,
			Health: &multiorchdatapb.PoolerHealthState{
				IsPostgresRunning: true,
				PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
					SyncReplicationConfig: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
						StandbyIds: standbyIDs,
					},
				},
			},
		}
	}

	tests := []struct {
		name          string
		poolers       []*PoolerState
		expectCount   int
		expectedCode  types.ProblemCode
		expectedScope types.ProblemScope
		expectedPrio  types.Priority
	}{
		{
			name: "detects replica not in standby list",
			poolers: []*PoolerState{
				makePrimaryState(nil), // Empty standby list
				makeReplicaState(replica1ID),
			},
			expectCount:   1,
			expectedCode:  types.ProblemReplicaNotInStandbyList,
			expectedScope: types.ScopePooler,
			expectedPrio:  types.PriorityNormal,
		},
		{
			name: "ignores replica already in standby list",
			poolers: []*PoolerState{
				makePrimaryState([]*clustermetadatapb.ID{replica1ID}),
				makeReplicaState(replica1ID),
			},
			expectCount: 0,
		},
		{
			name: "ignores primary nodes",
			poolers: []*PoolerState{
				{
					ID:             primaryID,
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					Type:           clustermetadatapb.PoolerType_PRIMARY,
					Health: &multiorchdatapb.PoolerHealthState{
						IsPostgresRunning: true,
					},
				},
			},
			expectCount: 0,
		},
		{
			name: "ignores uninitialized replica",
			poolers: []*PoolerState{
				makePrimaryState(nil),
				{
					ID:            replica1ID,
					IsPrimary:     false,
					IsInitialized: false, // Not initialized
					Type:          clustermetadatapb.PoolerType_REPLICA,
					Health: &multiorchdatapb.PoolerHealthState{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{Host: "primary.example.com"},
						},
					},
				},
			},
			expectCount: 0,
		},
		{
			name: "ignores replica when primary is unreachable",
			poolers: []*PoolerState{
				{
					ID:             primaryID,
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: false, // Unreachable
					Health:         &multiorchdatapb.PoolerHealthState{},
				},
				makeReplicaState(replica1ID),
			},
			expectCount: 0,
		},
		{
			name: "ignores replica with no replication configured",
			poolers: []*PoolerState{
				makePrimaryState(nil),
				{
					ID:            replica1ID,
					IsPrimary:     false,
					IsInitialized: true,
					Type:          clustermetadatapb.PoolerType_REPLICA,
					Health: &multiorchdatapb.PoolerHealthState{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{Host: ""}, // No conninfo
						},
					},
				},
			},
			expectCount: 0,
		},
		{
			name: "ignores UNKNOWN pooler type",
			poolers: []*PoolerState{
				makePrimaryState(nil),
				{
					ID:            replica1ID,
					IsPrimary:     false,
					IsInitialized: true,
					Type:          clustermetadatapb.PoolerType_UNKNOWN, // Unknown type
					Health: &multiorchdatapb.PoolerHealthState{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{Host: "primary.example.com"},
						},
					},
				},
			},
			expectCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shard := &ShardAnalysis{ShardKey: shardKey, Poolers: tt.poolers}
			problems, err := analyzer.Analyze(shard)
			require.NoError(t, err)

			if tt.expectCount > 0 {
				require.Len(t, problems, tt.expectCount)
				require.Equal(t, tt.expectedCode, problems[0].Code)
				require.Equal(t, tt.expectedScope, problems[0].Scope)
				require.Equal(t, tt.expectedPrio, problems[0].Priority)
				require.NotNil(t, problems[0].RecoveryAction)
			} else {
				require.Empty(t, problems)
			}
		})
	}

	t.Run("detects multiple replicas not in standby list", func(t *testing.T) {
		replica2ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica2"}
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				makePrimaryState(nil), // Neither replica in standby list
				makeReplicaState(replica1ID),
				makeReplicaState(replica2ID),
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Len(t, problems, 2, "should detect both replicas missing from standby list")
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		nilFactoryAnalyzer := &ReplicaNotInStandbyListAnalyzer{factory: nil}
		shard := &ShardAnalysis{ShardKey: shardKey}

		_, err := nilFactoryAnalyzer.Analyze(shard)
		require.Error(t, err)
		require.Contains(t, err.Error(), "factory not initialized")
	})
}
