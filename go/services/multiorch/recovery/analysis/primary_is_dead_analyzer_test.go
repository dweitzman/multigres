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

func TestPrimaryIsDeadAnalyzer_Analyze(t *testing.T) {
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

	analyzer := &PrimaryIsDeadAnalyzer{factory: factory}
	shardKey := commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"}

	t.Run("detects dead primary (primary exists but is unreachable)", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             primaryID,
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: false, // Unreachable
					Health:         &multiorchdatapb.PoolerHealthState{IsPostgresRunning: false},
				},
				{
					ID:            replicaID,
					IsPrimary:     false,
					IsInitialized: true, // Initialized replica exists
					Health:        &multiorchdatapb.PoolerHealthState{},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemPrimaryIsDead, problems[0].Code)
		require.Equal(t, types.ScopeShard, problems[0].Scope)
		require.Equal(t, types.PriorityEmergency, problems[0].Priority)
		require.Equal(t, primaryID, problems[0].PoolerID, "should identify the dead primary")
		require.NotNil(t, problems[0].RecoveryAction)
	})

	t.Run("ignores healthy primary (reachable with postgres running)", func(t *testing.T) {
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
				{
					ID:            replicaID,
					IsPrimary:     false,
					IsInitialized: true,
					Health:        &multiorchdatapb.PoolerHealthState{},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores shard with no primary", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:            replicaID,
					IsPrimary:     false,
					IsInitialized: true,
					Health:        &multiorchdatapb.PoolerHealthState{},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Empty(t, problems) // ShardNeedsBootstrap handles this case
	})

	t.Run("ignores dead primary when no initialized replica exists", func(t *testing.T) {
		// If no replica has been initialized, we have no perspective from which to
		// assess primary health — ShardNeedsBootstrap handles uninitialized shards.
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             primaryID,
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: false, // Dead
					Health:         &multiorchdatapb.PoolerHealthState{},
				},
				{
					ID:            replicaID,
					IsPrimary:     false,
					IsInitialized: false, // No initialized replica
					Health:        &multiorchdatapb.PoolerHealthState{},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores uninitialized replica when checking for initialized replicas", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             primaryID,
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: false,
					Health:         &multiorchdatapb.PoolerHealthState{},
				},
				{
					ID:            replicaID,
					IsPrimary:     false,
					IsInitialized: false, // Uninitialized - ShardNeedsBootstrap handles this
					Health:        &multiorchdatapb.PoolerHealthState{},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("PrimaryIsDead"), analyzer.Name())
	})

	t.Run("ignores when primary pooler down but replicas connected (postgres still running)", func(t *testing.T) {
		// When the primary pooler is unreachable (!LastCheckValid) but all replicas are
		// still receiving WAL from it, the postgres process is still running — only the
		// pooler process crashed. Don't failover; operator should restart the pooler.
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             primaryID,
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: false, // Pooler unreachable
					Health: &multiorchdatapb.PoolerHealthState{
						IsPostgresRunning: false, // Unknown since pooler is down
						MultiPooler: &clustermetadatapb.MultiPooler{
							Hostname: "primary-host",
							PortMap:  map[string]int32{"postgres": 5432},
						},
					},
				},
				{
					ID:             replicaID,
					IsPrimary:      false,
					IsInitialized:  true,
					LastCheckValid: true, // Replica is reachable
					Health: &multiorchdatapb.PoolerHealthState{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
								Host: "primary-host",
								Port: 5432,
							},
							LastReceiveLsn: "0/1234", // Actively receiving WAL
						},
					},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Empty(t, problems, "should not trigger failover when pooler is down but replicas are still connected")
	})

	t.Run("triggers failover when primary pooler up but postgres down", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             primaryID,
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true, // Pooler is reachable
					Health: &multiorchdatapb.PoolerHealthState{
						IsPostgresRunning: false, // But postgres is down
					},
				},
				{
					ID:            replicaID,
					IsPrimary:     false,
					IsInitialized: true,
					Health:        &multiorchdatapb.PoolerHealthState{},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Len(t, problems, 1, "should trigger failover when pooler is up but postgres is down")
		require.Equal(t, types.ProblemPrimaryIsDead, problems[0].Code)
	})

	t.Run("triggers failover when both pooler and replicas disconnected", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             primaryID,
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: false, // Pooler down
					Health: &multiorchdatapb.PoolerHealthState{
						MultiPooler: &clustermetadatapb.MultiPooler{
							Hostname: "primary-host",
							PortMap:  map[string]int32{"postgres": 5432},
						},
					},
				},
				{
					ID:             replicaID,
					IsPrimary:      false,
					IsInitialized:  true,
					LastCheckValid: true,
					Health: &multiorchdatapb.PoolerHealthState{
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
								Host: "different-host", // Replica not connected to this primary
								Port: 5432,
							},
							LastReceiveLsn: "",
						},
					},
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Len(t, problems, 1, "should trigger failover when pooler down and replicas disconnected")
		require.Equal(t, types.ProblemPrimaryIsDead, problems[0].Code)
	})
}
