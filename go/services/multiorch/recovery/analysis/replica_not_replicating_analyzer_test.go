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

func TestReplicaNotReplicatingAnalyzer_Analyze(t *testing.T) {
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

	analyzer := &ReplicaNotReplicatingAnalyzer{factory: factory}
	shardKey := commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"}

	// reachablePrimary is a helper for tests that need a reachable primary in the shard.
	reachablePrimary := &PoolerState{
		ID:             primaryID,
		IsPrimary:      true,
		IsInitialized:  true,
		LastCheckValid: true,
		Health:         &multiorchdatapb.PoolerHealthState{IsPostgresRunning: true},
	}

	t.Run("detects replica with no primary_conninfo", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				reachablePrimary,
				{
					ID:            replicaID,
					IsPrimary:     false,
					IsInitialized: true,
					Health: &multiorchdatapb.PoolerHealthState{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
								Host: "", // No primary_conninfo configured
							},
						},
					},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemReplicaNotReplicating, problems[0].Code)
		require.Equal(t, types.ScopePooler, problems[0].Scope)
		require.Equal(t, types.PriorityHigh, problems[0].Priority)
		require.Equal(t, replicaID, problems[0].PoolerID)
		require.NotNil(t, problems[0].RecoveryAction)
	})

	t.Run("detects replica with replication stopped (WAL replay paused)", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				reachablePrimary,
				{
					ID:            replicaID,
					IsPrimary:     false,
					IsInitialized: true,
					Health: &multiorchdatapb.PoolerHealthState{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
								Host: "primary.example.com",
							},
							IsWalReplayPaused: true, // Replication stopped
						},
					},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemReplicaNotReplicating, problems[0].Code)
	})

	t.Run("ignores replica with healthy replication", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				reachablePrimary,
				{
					ID:            replicaID,
					IsPrimary:     false,
					IsInitialized: true,
					Health: &multiorchdatapb.PoolerHealthState{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
								Host: "primary.example.com",
							},
							IsWalReplayPaused: false,
						},
					},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores primary nodes", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             primaryID,
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					Health:         &multiorchdatapb.PoolerHealthState{IsPostgresRunning: true},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores uninitialized replica", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				reachablePrimary,
				{
					ID:            replicaID,
					IsPrimary:     false,
					IsInitialized: false, // Not initialized
					Health:        &multiorchdatapb.PoolerHealthState{},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores replica when primary is unreachable (PrimaryIsDead handles this)", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             primaryID,
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: false, // Primary unreachable
					Health:         &multiorchdatapb.PoolerHealthState{},
				},
				{
					ID:            replicaID,
					IsPrimary:     false,
					IsInitialized: true,
					Health: &multiorchdatapb.PoolerHealthState{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{Host: ""},
						},
					},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Empty(t, problems, "PrimaryIsDead handles replica issues when primary is unreachable")
	})

	t.Run("detects multiple replicas not replicating", func(t *testing.T) {
		replica2ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica2"}
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				reachablePrimary,
				{
					ID:            replicaID,
					IsPrimary:     false,
					IsInitialized: true,
					Health: &multiorchdatapb.PoolerHealthState{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{Host: ""},
						},
					},
				},
				{
					ID:            replica2ID,
					IsPrimary:     false,
					IsInitialized: true,
					Health: &multiorchdatapb.PoolerHealthState{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo:   &multipoolermanagerdatapb.PrimaryConnInfo{Host: "primary.example.com"},
							IsWalReplayPaused: true,
						},
					},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Len(t, problems, 2, "should detect both replicas with issues")
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("ReplicaNotReplicating"), analyzer.Name())
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		nilFactoryAnalyzer := &ReplicaNotReplicatingAnalyzer{factory: nil}
		shard := &ShardAnalysis{ShardKey: shardKey}

		_, err := nilFactoryAnalyzer.Analyze(shard)
		require.Error(t, err)
		require.Contains(t, err.Error(), "factory not initialized")
	})
}
