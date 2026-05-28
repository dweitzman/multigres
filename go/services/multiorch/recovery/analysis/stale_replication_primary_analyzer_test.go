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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

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

	// Runaway-recruit subcases. The analyzer should short-circuit to a
	// single ProblemUnresolvedRevocation when the leader's term is below
	// an accepted revocation that has been outstanding long enough.
	oldCoordID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell1", Name: "old-coord"}
	leaderAtTerm := func(term int64) *PoolerAnalysis {
		return &PoolerAnalysis{
			PoolerID:      leaderID,
			ShardKey:      shardKey,
			IsLeader:      true,
			IsInitialized: true,
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
						LeaderId:   leaderID,
					},
				},
			},
		}
	}
	poolerWithRevocation := func(id *clustermetadatapb.ID, revokedTerm int64, initiatedAt time.Time, recordedTerm int64) *PoolerAnalysis {
		pa := &PoolerAnalysis{
			PoolerID:        id,
			ShardKey:        shardKey,
			IsLeader:        false,
			IsInitialized:   true,
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{},
		}
		if revokedTerm > 0 {
			pa.ConsensusStatus.TermRevocation = &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       revokedTerm,
				AcceptedCoordinatorId:  oldCoordID,
				CoordinatorInitiatedAt: timestamppb.New(initiatedAt),
			}
		}
		if recordedTerm > 0 {
			pa.ConsensusStatus.CurrentPosition = &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: recordedTerm},
					LeaderId:   leaderID,
				},
			}
		}
		return pa
	}

	t.Run("runaway recruit: leader below stale revocation triggers AppointLeader", func(t *testing.T) {
		oldEnough := time.Now().Add(-(unresolvedRevocationThreshold + 5*time.Second))
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderReachable:   true,
			LeaderObservation: clusterObs, // leader at term 2
			Leader:            leaderAtTerm(2),
			Analyses: []*PoolerAnalysis{
				leaderAtTerm(2),
				// Replica accepted revocation at term 3 — leader is below.
				poolerWithRevocation(replicaID, 3, oldEnough, 2),
			},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1, "should short-circuit to a single shard-level problem")
		p := problems[0]
		assert.Equal(t, types.ProblemUnresolvedRevocation, p.Code)
		assert.Equal(t, types.ScopeShard, p.Scope)
		assert.Equal(t, types.PriorityEmergency, p.Priority)
		assert.Contains(t, p.Description, "term 2")
		assert.Contains(t, p.Description, "term 3")
		require.NotNil(t, p.RecoveryAction)
	})

	t.Run("runaway recruit: skipped when revocation is fresh", func(t *testing.T) {
		recent := time.Now().Add(-(unresolvedRevocationThreshold - 2*time.Second))
		stale := poolerWithRevocation(replicaID, 3, recent, 2)
		// Add a non-self-claiming stale-rep-primary so we can confirm
		// the per-pooler problem path still runs.
		stale.ConsensusStatus.ReplicationPrimary = &clustermetadatapb.ReplicationPrimary{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				LeaderId:   otherID,
			},
		}
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderReachable:   true,
			LeaderObservation: clusterObs,
			Leader:            leaderAtTerm(2),
			Analyses:          []*PoolerAnalysis{leaderAtTerm(2), stale},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		assert.Equal(t, types.ProblemReplicaNotReplicating, problems[0].Code,
			"fresh revocation should not preempt the normal stale-rep path")
	})

	t.Run("runaway recruit: skipped when leader is unreachable", func(t *testing.T) {
		oldEnough := time.Now().Add(-(unresolvedRevocationThreshold + 5*time.Second))
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderReachable:   false, // LeaderUnreachable handles this case
			LeaderObservation: clusterObs,
			Leader:            leaderAtTerm(2),
			Analyses: []*PoolerAnalysis{
				leaderAtTerm(2),
				poolerWithRevocation(replicaID, 3, oldEnough, 2),
			},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("runaway recruit: skipped when leader's term is at or above the revoked term", func(t *testing.T) {
		oldEnough := time.Now().Add(-(unresolvedRevocationThreshold + 5*time.Second))
		// Replica has a stale revocation but leader has caught up.
		// Replica also points at the cluster leader so it's otherwise
		// healthy — we want this test to isolate the runaway-recruit
		// branch from the per-pooler stale-rep-primary branch.
		healthyReplica := poolerWithRevocation(replicaID, 3, oldEnough, 3)
		healthyReplica.ConsensusStatus.ReplicationPrimary = &clustermetadatapb.ReplicationPrimary{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3},
				LeaderId:   leaderID,
			},
		}
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderReachable:   true,
			LeaderObservation: clusterObs,
			Leader:            leaderAtTerm(3),
			Analyses:          []*PoolerAnalysis{leaderAtTerm(3), healthyReplica},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems, "leader at/above revoked term — no runaway")
	})

	t.Run("runaway recruit: picks highest revoked term across multiple poolers", func(t *testing.T) {
		oldEnough := time.Now().Add(-(unresolvedRevocationThreshold + 5*time.Second))
		other2 := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica-2"}
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderReachable:   true,
			LeaderObservation: clusterObs,
			Leader:            leaderAtTerm(2),
			Analyses: []*PoolerAnalysis{
				leaderAtTerm(2),
				poolerWithRevocation(replicaID, 3, oldEnough, 2),
				poolerWithRevocation(other2, 5, oldEnough, 2),
			},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		assert.Contains(t, problems[0].Description, "term 5")
	})
}
