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

package analysis

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestStaleReplicationPrimaryAnalyzer_Analyze(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()
	rpcClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(rpcClient, slog.Default())
	coordID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell1", Name: "test-coord"}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(nil, poolerStore, rpcClient, ts, coord, slog.Default())
	analyzer := &StaleReplicationPrimaryAnalyzer{factory: factory}

	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "leader"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"}
	otherID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "other"}

	// repPrimary builds a ConsensusStatus whose ReplicationPrimary.Rule
	// names id at the given coordinator term. Used to seed
	// SelfLeaderObservation in fixtures.
	repPrimary := func(id *clustermetadatapb.ID, term int64) *clustermetadatapb.ConsensusStatus {
		return &clustermetadatapb.ConsensusStatus{
			ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
					LeaderId:   id,
				},
			},
		}
	}

	leaderPA := &PoolerAnalysis{
		PoolerID:        leaderID,
		ShardKey:        shardKey,
		IsLeader:        true,
		IsInitialized:   true,
		ConsensusStatus: repPrimary(leaderID, 2),
	}
	clusterObs := &clustermetadatapb.LeaderObservation{
		LeaderId:         leaderID,
		LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
	}

	baseShard := func(replicaPA *PoolerAnalysis) *ShardAnalysis {
		return &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderReachable:   true,
			LeaderObservation: clusterObs,
			Leader:            leaderPA,
			Analyses:          []*PoolerAnalysis{leaderPA, replicaPA},
		}
	}

	t.Run("emits ProblemStaleLeader when pooler self-claims at older rule", func(t *testing.T) {
		stale := &PoolerAnalysis{
			PoolerID:        replicaID,
			ShardKey:        shardKey,
			IsLeader:        false,
			IsInitialized:   true,
			ConsensusStatus: repPrimary(replicaID, 1), // self-claim at term 1
		}
		problems, err := analyzer.Analyze(baseShard(stale))
		require.NoError(t, err)
		require.Len(t, problems, 1)
		assert.Equal(t, types.ProblemStaleLeader, problems[0].Code)
		assert.Equal(t, replicaID, problems[0].PoolerID)
		assert.Equal(t, types.PriorityEmergency, problems[0].Priority)
	})

	t.Run("emits ProblemReplicaNotReplicating when ReplicationPrimary names a non-leader", func(t *testing.T) {
		wrong := &PoolerAnalysis{
			PoolerID:        replicaID,
			ShardKey:        shardKey,
			IsLeader:        false,
			IsInitialized:   true,
			ConsensusStatus: repPrimary(otherID, 1), // points at "other", not leader
		}
		problems, err := analyzer.Analyze(baseShard(wrong))
		require.NoError(t, err)
		require.Len(t, problems, 1)
		assert.Equal(t, types.ProblemReplicaNotReplicating, problems[0].Code)
		assert.Equal(t, replicaID, problems[0].PoolerID)
		assert.Equal(t, types.PriorityHigh, problems[0].Priority)
	})

	t.Run("emits ProblemReplicaNotReplicating when ReplicationPrimary is unset", func(t *testing.T) {
		fresh := &PoolerAnalysis{
			PoolerID:      replicaID,
			ShardKey:      shardKey,
			IsLeader:      false,
			IsInitialized: true,
			// ConsensusStatus left nil — no ReplicationPrimary observation
		}
		problems, err := analyzer.Analyze(baseShard(fresh))
		require.NoError(t, err)
		require.Len(t, problems, 1)
		assert.Equal(t, types.ProblemReplicaNotReplicating, problems[0].Code)
	})

	t.Run("no problem when pooler's ReplicationPrimary matches cluster leader", func(t *testing.T) {
		healthy := &PoolerAnalysis{
			PoolerID:        replicaID,
			ShardKey:        shardKey,
			IsLeader:        false,
			IsInitialized:   true,
			ConsensusStatus: repPrimary(leaderID, 2), // same as cluster
		}
		problems, err := analyzer.Analyze(baseShard(healthy))
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("no problem when pooler's ReplicationPrimary matches cluster leader at older rule", func(t *testing.T) {
		// Older rule for the same LeaderId is not stale — same leader.
		// Catches the case where the pooler hasn't received the latest
		// rule yet but is pointed at the right node.
		lagging := &PoolerAnalysis{
			PoolerID:        replicaID,
			ShardKey:        shardKey,
			IsLeader:        false,
			IsInitialized:   true,
			ConsensusStatus: repPrimary(leaderID, 1), // same LeaderId, older rule
		}
		problems, err := analyzer.Analyze(baseShard(lagging))
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("skips when no cluster leader is observed", func(t *testing.T) {
		// Bootstrap window: no LeaderObservation populated. Nothing to compare against.
		stale := &PoolerAnalysis{
			PoolerID:        replicaID,
			ShardKey:        shardKey,
			IsLeader:        false,
			IsInitialized:   true,
			ConsensusStatus: repPrimary(replicaID, 1),
		}
		sa := baseShard(stale)
		sa.LeaderObservation = nil
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("skips replica when no leader pooler is reachable", func(t *testing.T) {
		// Without sa.Leader the recovery action can't build a SetTermPrimary
		// request, so firing the problem now would just generate a failed
		// RPC. PrimaryIsDead handles the unreachable-leader case.
		wrong := &PoolerAnalysis{
			PoolerID:        replicaID,
			ShardKey:        shardKey,
			IsLeader:        false,
			IsInitialized:   true,
			ConsensusStatus: repPrimary(otherID, 1),
		}
		sa := baseShard(wrong)
		sa.LeaderReachable = false
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("self-claiming pooler still emits ProblemStaleLeader even without reachable leader", func(t *testing.T) {
		// Stale primaries are dangerous (may accept writes) — emit
		// even when the cluster leader is currently unreachable, so the
		// problem is visible. The action will still need a reachable
		// leader to send SetTermPrimary, but flagging the condition is
		// the right diagnostic.
		stale := &PoolerAnalysis{
			PoolerID:        replicaID,
			ShardKey:        shardKey,
			IsLeader:        false,
			IsInitialized:   true,
			ConsensusStatus: repPrimary(replicaID, 1),
		}
		sa := baseShard(stale)
		sa.LeaderReachable = false
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		assert.Equal(t, types.ProblemStaleLeader, problems[0].Code)
	})

	t.Run("skips uninitialized poolers", func(t *testing.T) {
		fresh := &PoolerAnalysis{
			PoolerID:      replicaID,
			ShardKey:      shardKey,
			IsLeader:      false,
			IsInitialized: false,
		}
		problems, err := analyzer.Analyze(baseShard(fresh))
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("multiple stale primaries sorted with descending priorities", func(t *testing.T) {
		stale1 := &PoolerAnalysis{
			PoolerID:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-1"},
			ShardKey:        shardKey,
			IsLeader:        false,
			IsInitialized:   true,
			ConsensusStatus: repPrimary(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-1"}, 1),
		}
		stale1.ConsensusStatus.CurrentPosition = &clustermetadatapb.PoolerPosition{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				LeaderId:   stale1.PoolerID,
			},
		}
		stale2 := &PoolerAnalysis{
			PoolerID:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-2"},
			ShardKey:        shardKey,
			IsLeader:        false,
			IsInitialized:   true,
			ConsensusStatus: repPrimary(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-2"}, 1),
		}
		// Higher self-claim rule on stale2 so it's "less stale" relative to stale1.
		stale2.ConsensusStatus.CurrentPosition = &clustermetadatapb.PoolerPosition{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1, LeaderSubterm: 5},
				LeaderId:   stale2.PoolerID,
			},
		}
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderReachable:   true,
			LeaderObservation: clusterObs,
			Leader:            leaderPA,
			Analyses:          []*PoolerAnalysis{leaderPA, stale1, stale2},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 2)
		assert.Equal(t, types.ProblemStaleLeader, problems[0].Code)
		assert.Equal(t, types.ProblemStaleLeader, problems[1].Code)
		assert.Equal(t, types.PriorityEmergency, problems[0].Priority)
		assert.Equal(t, types.PriorityEmergency-1, problems[1].Priority)
	})

	t.Run("metadata accessors return expected values", func(t *testing.T) {
		assert.Equal(t, types.CheckName("StaleReplicationPrimary"), analyzer.Name())
		assert.Equal(t, types.ProblemStaleLeader, analyzer.ProblemCode())
		require.NotNil(t, analyzer.RecoveryAction())
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		nilFactoryAnalyzer := &StaleReplicationPrimaryAnalyzer{factory: nil}
		problems, err := nilFactoryAnalyzer.Analyze(&ShardAnalysis{ShardKey: shardKey})
		require.Error(t, err)
		assert.Nil(t, problems)
		assert.Contains(t, err.Error(), "factory not initialized")
	})
}
