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
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestShardNeedsBootstrapAnalyzer_Analyze(t *testing.T) {
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

	analyzer := &ShardNeedsBootstrapAnalyzer{factory: factory}
	shardKey := commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	poolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}
	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}

	t.Run("detects uninitialized shard needing bootstrap", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:               poolerID,
					IsPrimary:        false,
					LastCheckValid:   true,
					IsInitialized:    false,
					HasDataDirectory: false,
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemShardNeedsBootstrap, problems[0].Code)
		require.Equal(t, types.ScopeShard, problems[0].Scope)
		require.Equal(t, types.PriorityShardBootstrap, problems[0].Priority)
		require.Nil(t, problems[0].PoolerID, "shard-scoped problem should have nil PoolerID")
		require.NotNil(t, problems[0].RecoveryAction)
	})

	t.Run("ignores initialized pooler", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             poolerID,
					IsPrimary:      false,
					LastCheckValid: true,
					IsInitialized:  true, // Already initialized
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores uninitialized pooler when primary exists", func(t *testing.T) {
		// If a primary is already running, no bootstrap is needed — the replica will
		// be provisioned through normal replication setup.
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:               primaryID,
					IsPrimary:        true,
					IsInitialized:    true,
					LastCheckValid:   true,
					HasDataDirectory: true,
				},
				{
					ID:               poolerID,
					IsPrimary:        false,
					IsInitialized:    false,
					HasDataDirectory: false,
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Empty(t, problems, "primary exists, so no bootstrap needed")
	})

	t.Run("detects bootstrap needed for replica without data directory", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:               poolerID,
					IsPrimary:        false,
					LastCheckValid:   true,
					IsInitialized:    false,
					HasDataDirectory: false, // No data directory — needs bootstrap
					Type:             clustermetadatapb.PoolerType_REPLICA,
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemShardNeedsBootstrap, problems[0].Code)
	})

	t.Run("skips replica that has a data directory (may be temporarily down)", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:               poolerID,
					IsPrimary:        false,
					LastCheckValid:   true,
					IsInitialized:    false, // May be temporarily down
					HasDataDirectory: true,  // Has data - not a bootstrap case
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Empty(t, problems, "replica with data directory is not a bootstrap case")
	})

	t.Run("ignores unreachable pooler (can't trust its state)", func(t *testing.T) {
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:               poolerID,
					IsPrimary:        false,
					LastCheckValid:   false, // Unreachable - can't trust state
					IsInitialized:    false,
					HasDataDirectory: false,
				},
			},
		}

		problems, err := analyzer.Analyze(shard)
		require.NoError(t, err)
		require.Empty(t, problems, "should not bootstrap based on unreachable node state")
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("ShardNeedsBootstrap"), analyzer.Name())
	})
}
