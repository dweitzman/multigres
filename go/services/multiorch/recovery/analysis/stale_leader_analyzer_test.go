// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analysis

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// selfClaimStatus builds a ConsensusStatus that names id as the leader
// at the given coordinator term in BOTH CurrentPosition.Rule (read by
// commonconsensus.IsLeader / LeaderTerm — used by the analyzer's
// problem description) and ReplicationPrimary.Rule (read by
// SelfLeaderObservation — used by the analyzer's stale-detection logic).
func selfClaimStatus(id *clustermetadatapb.ID, term int64) *clustermetadatapb.ConsensusStatus {
	rule := &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
		LeaderId:   id,
	}
	return &clustermetadatapb.ConsensusStatus{
		Id:                 id,
		CurrentPosition:    &clustermetadatapb.PoolerPosition{Rule: rule},
		ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{Rule: rule},
	}
}

// clusterObservation is the LeaderObservation form of "the cluster
// elected id at this term."
func clusterObservation(id *clustermetadatapb.ID, term int64) *clustermetadatapb.LeaderObservation {
	return &clustermetadatapb.LeaderObservation{
		LeaderId:         id,
		LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
	}
}

func TestStaleLeaderAnalyzer_Analyze(t *testing.T) {
	factory := &RecoveryActionFactory{poolerStore: store.NewPoolerStore(nil, slog.Default())}
	analyzer := &StaleLeaderAnalyzer{factory: factory}
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "default", Shard: "0"}

	newLeaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "new-leader"}
	staleID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-primary"}

	t.Run("detects stale primary whose self-claim disagrees with cluster", func(t *testing.T) {
		// Cluster says new-leader at term 6; stale-primary's own health
		// stream still names itself at term 5.
		newLeaderPA := &PoolerAnalysis{
			PoolerID:        newLeaderID,
			ShardKey:        shardKey,
			IsLeader:        true,
			IsInitialized:   true,
			ConsensusStatus: selfClaimStatus(newLeaderID, 6),
		}
		stalePA := &PoolerAnalysis{
			PoolerID:        staleID,
			ShardKey:        shardKey,
			IsLeader:        false,
			IsInitialized:   true,
			ConsensusStatus: selfClaimStatus(staleID, 5),
		}
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			Analyses:          []*PoolerAnalysis{newLeaderPA, stalePA},
			LeaderObservation: clusterObservation(newLeaderID, 6),
			Leader:            newLeaderPA,
		}

		problems, err := analyzer.Analyze(sa)

		require.NoError(t, err)
		require.Len(t, problems, 1)
		p := problems[0]
		assert.Equal(t, types.ProblemStaleLeader, p.Code)
		assert.Equal(t, types.ScopePooler, p.Scope)
		assert.Equal(t, types.PriorityEmergency, p.Priority)
		assert.Equal(t, staleID, p.PoolerID)
		assert.Contains(t, p.Description, "stale-primary")
		assert.Contains(t, p.Description, "stale_leader_term 5")
		assert.Contains(t, p.Description, "new-leader")
		assert.Contains(t, p.Description, "most_advanced_leader_term 6")
	})

	t.Run("no problem when no cluster leader observed yet", func(t *testing.T) {
		// Bootstrap window: no pooler has published an observation.
		stalePA := &PoolerAnalysis{
			PoolerID:        staleID,
			ShardKey:        shardKey,
			ConsensusStatus: selfClaimStatus(staleID, 5),
		}
		sa := &ShardAnalysis{
			ShardKey: shardKey,
			Analyses: []*PoolerAnalysis{stalePA},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("no problem when followers acknowledge the cluster leader", func(t *testing.T) {
		// Healthy steady state: every pooler's health stream names the
		// cluster-elected leader. No staleness anywhere.
		newLeaderPA := &PoolerAnalysis{
			PoolerID:        newLeaderID,
			ShardKey:        shardKey,
			IsLeader:        true,
			ConsensusStatus: selfClaimStatus(newLeaderID, 6),
		}
		followerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "follower"}
		followerPA := &PoolerAnalysis{
			PoolerID: followerID,
			ShardKey: shardKey,
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id: followerID,
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
					Rule: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
						LeaderId:   newLeaderID,
					},
				},
			},
		}
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			Analyses:          []*PoolerAnalysis{newLeaderPA, followerPA},
			LeaderObservation: clusterObservation(newLeaderID, 6),
			Leader:            newLeaderPA,
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("no problem when pooler has no self-observation", func(t *testing.T) {
		// New pooler that hasn't yet reported a ConsensusStatus — no claim
		// at all, so it can't be a stale primary.
		newLeaderPA := &PoolerAnalysis{
			PoolerID:        newLeaderID,
			ShardKey:        shardKey,
			IsLeader:        true,
			ConsensusStatus: selfClaimStatus(newLeaderID, 6),
		}
		freshID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "fresh"}
		freshPA := &PoolerAnalysis{
			PoolerID: freshID,
			ShardKey: shardKey,
			// ConsensusStatus left nil.
		}
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			Analyses:          []*PoolerAnalysis{newLeaderPA, freshPA},
			LeaderObservation: clusterObservation(newLeaderID, 6),
			Leader:            newLeaderPA,
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("multiple stale primaries sorted with descending priorities", func(t *testing.T) {
		// Two stuck-at-older-rule poolers at different terms. Most stale
		// (lower term) gets PriorityEmergency, next gets PriorityEmergency-1.
		stale1ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-1"}
		stale2ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-2"}
		newLeaderPA := &PoolerAnalysis{
			PoolerID:        newLeaderID,
			ShardKey:        shardKey,
			IsLeader:        true,
			ConsensusStatus: selfClaimStatus(newLeaderID, 10),
		}
		stale1PA := &PoolerAnalysis{
			PoolerID:        stale1ID,
			ShardKey:        shardKey,
			ConsensusStatus: selfClaimStatus(stale1ID, 5),
		}
		stale2PA := &PoolerAnalysis{
			PoolerID:        stale2ID,
			ShardKey:        shardKey,
			ConsensusStatus: selfClaimStatus(stale2ID, 7),
		}
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			Analyses:          []*PoolerAnalysis{newLeaderPA, stale1PA, stale2PA},
			LeaderObservation: clusterObservation(newLeaderID, 10),
			Leader:            newLeaderPA,
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 2)
		assert.Equal(t, stale1ID, problems[0].PoolerID)
		assert.Equal(t, types.PriorityEmergency, problems[0].Priority)
		assert.Equal(t, stale2ID, problems[1].PoolerID)
		assert.Equal(t, types.PriorityEmergency-1, problems[1].Priority)
	})

	t.Run("factory nil returns error", func(t *testing.T) {
		nilAnalyzer := &StaleLeaderAnalyzer{factory: nil}
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderObservation: clusterObservation(newLeaderID, 6),
		}
		problems, err := nilAnalyzer.Analyze(sa)
		require.Error(t, err)
		assert.Nil(t, problems)
		assert.Contains(t, err.Error(), "factory not initialized")
	})

	t.Run("analyzer name", func(t *testing.T) {
		assert.Equal(t, types.CheckName("StaleLeader"), analyzer.Name())
	})
}
