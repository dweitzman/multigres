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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestSetPrimaryAnalyzer_Analyze(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()
	rpcClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(rpcClient, slog.Default())
	coordID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell1", Name: "test-coord"}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(nil, poolerStore, rpcClient, ts, coord, slog.Default())

	analyzer := &SetPrimaryAnalyzer{factory: factory}

	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"}
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	// reachableLeader is the highest-rule leader: a healthy primary naming itself
	// at term 1. The analyzer needs this present before flagging anyone.
	reachableLeader := func() *PoolerAnalysis {
		return &PoolerAnalysis{
			PoolerID:        primaryID,
			ShardKey:        shardKey,
			IsInitialized:   true,
			ConsensusStatus: consensusNamingLeader(primaryID, primaryID, 1),
		}
	}

	t.Run("detects replica not following the leader", func(t *testing.T) {
		// Orphan replica: no ReplicationPrimary recorded at all.
		sa := &ShardAnalysis{
			ShardKey:                      shardKey,
			HighestTermDiscoveredLeaderID: primaryID,
			HighestTermReachableLeader:    reachableLeader(),
			LeaderReachable:               true,
			Analyses: []*PoolerAnalysis{
				reachableLeader(),
				{
					PoolerID:        replicaID,
					ShardKey:        shardKey,
					IsInitialized:   true,
					ConsensusStatus: &clustermetadatapb.ConsensusStatus{Id: replicaID},
				},
			},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemNeedsSetPrimary, problems[0].Code)
		require.Equal(t, replicaID.Name, problems[0].PoolerID.Name)
		require.Equal(t, types.ScopePooler, problems[0].Scope)
		require.Equal(t, types.PriorityHigh, problems[0].Priority)
		require.NotNil(t, problems[0].RecoveryAction)
	})

	t.Run("detects a stale leader (self-named, not the highest rule)", func(t *testing.T) {
		// A stale leader still names itself as its own ReplicationPrimary, so it
		// is not following the current leader and must be demoted via SetPrimary.
		sa := &ShardAnalysis{
			ShardKey:                      shardKey,
			HighestTermDiscoveredLeaderID: primaryID,
			HighestTermReachableLeader:    reachableLeader(),
			LeaderReachable:               true,
			Analyses: []*PoolerAnalysis{
				reachableLeader(),
				{
					PoolerID:        replicaID,
					ShardKey:        shardKey,
					IsInitialized:   true,
					ConsensusStatus: consensusNamingLeader(replicaID, replicaID, 0),
				},
			},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemNeedsSetPrimary, problems[0].Code)
		require.Equal(t, replicaID.Name, problems[0].PoolerID.Name)
	})

	// Skip when there's no usable leader yet. HighestTermReachableLeader is nil
	// whenever the leader's rule hasn't been observed, so we have no rule to put
	// in SetPrimaryRequest. Waiting one cycle is cheap.
	t.Run("skips when no usable leader is known", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:                      shardKey,
			HighestTermDiscoveredLeaderID: primaryID,
			HighestTermReachableLeader:    nil,
			LeaderReachable:               true,
			Analyses: []*PoolerAnalysis{{
				PoolerID:        replicaID,
				ShardKey:        shardKey,
				IsInitialized:   true,
				ConsensusStatus: &clustermetadatapb.ConsensusStatus{Id: replicaID},
			}},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores replica already following the leader", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:                      shardKey,
			HighestTermDiscoveredLeaderID: primaryID,
			HighestTermReachableLeader:    reachableLeader(),
			LeaderReachable:               true,
			Analyses: []*PoolerAnalysis{
				reachableLeader(),
				{
					PoolerID:        replicaID,
					ShardKey:        shardKey,
					IsInitialized:   true,
					ConsensusStatus: consensusNamingLeader(replicaID, primaryID, 1),
				},
			},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores the current leader itself", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:                      shardKey,
			HighestTermDiscoveredLeaderID: primaryID,
			HighestTermReachableLeader:    reachableLeader(),
			LeaderReachable:               true,
			Analyses:                      []*PoolerAnalysis{reachableLeader()},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores uninitialized replica", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:                      shardKey,
			HighestTermDiscoveredLeaderID: primaryID,
			HighestTermReachableLeader:    reachableLeader(),
			LeaderReachable:               true,
			Analyses: []*PoolerAnalysis{{
				PoolerID:        replicaID,
				ShardKey:        shardKey,
				IsInitialized:   false,
				ConsensusStatus: &clustermetadatapb.ConsensusStatus{Id: replicaID},
			}},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("SetPrimary"), analyzer.Name())
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		nilFactoryAnalyzer := &SetPrimaryAnalyzer{factory: nil}
		sa := &ShardAnalysis{
			ShardKey:                      shardKey,
			HighestTermDiscoveredLeaderID: primaryID,
			HighestTermReachableLeader:    reachableLeader(),
			LeaderReachable:               true,
			Analyses: []*PoolerAnalysis{{
				PoolerID:        replicaID,
				ShardKey:        shardKey,
				IsInitialized:   true,
				ConsensusStatus: &clustermetadatapb.ConsensusStatus{Id: replicaID},
			}},
		}

		problems, err := nilFactoryAnalyzer.Analyze(sa)
		require.Error(t, err)
		require.Nil(t, problems)
		require.Contains(t, err.Error(), "factory not initialized")
	})
}
