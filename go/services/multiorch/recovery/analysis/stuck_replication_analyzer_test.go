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

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestStuckReplicationAnalyzer_Analyze(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()
	rpcClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(rpcClient, slog.Default())
	coordID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell1", Name: "test-coord"}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(nil, poolerStore, rpcClient, ts, coord, slog.Default())
	analyzer := &StuckReplicationAnalyzer{factory: factory}

	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "leader"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"}

	leaderObs := &clustermetadatapb.LeaderObservation{
		LeaderId:         leaderID,
		LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
	}
	leaderPA := &PoolerAnalysis{PoolerID: leaderID, ShardKey: shardKey, IsLeader: true}

	// replicaPointingAtLeader returns a PoolerAnalysis whose
	// ReplicationPrimary names the cluster leader. WAL-receiver state
	// is controlled by the caller.
	replicaPointingAtLeader := func(notStreamingSince time.Time) *PoolerAnalysis {
		return &PoolerAnalysis{
			PoolerID:                     replicaID,
			ShardKey:                     shardKey,
			IsLeader:                     false,
			IsInitialized:                true,
			WalReceiverNotStreamingSince: notStreamingSince,
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
					Rule: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
						LeaderId:   leaderID,
					},
				},
			},
		}
	}

	baseShard := func(replicaPA *PoolerAnalysis) *ShardAnalysis {
		return &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderReachable:   true,
			LeaderObservation: leaderObs,
			Leader:            leaderPA,
			Analyses:          []*PoolerAnalysis{leaderPA, replicaPA},
		}
	}

	t.Run("no problem when wal_receiver is streaming", func(t *testing.T) {
		problems, err := analyzer.Analyze(baseShard(replicaPointingAtLeader(time.Time{})))
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("no problem when stuck duration is under threshold", func(t *testing.T) {
		stuck := time.Now().Add(-(stuckReplicationThreshold - 30*time.Second))
		problems, err := analyzer.Analyze(baseShard(replicaPointingAtLeader(stuck)))
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("emits ProblemReplicaStuck when stuck past threshold and pointed at leader", func(t *testing.T) {
		stuck := time.Now().Add(-(stuckReplicationThreshold + 5*time.Second))
		problems, err := analyzer.Analyze(baseShard(replicaPointingAtLeader(stuck)))
		require.NoError(t, err)
		require.Len(t, problems, 1)
		p := problems[0]
		assert.Equal(t, types.ProblemReplicaStuck, p.Code)
		assert.Equal(t, replicaID, p.PoolerID)
		assert.Equal(t, types.ScopePooler, p.Scope)
		assert.Nil(t, p.RecoveryAction, "observability-only: no recovery action")
		assert.Contains(t, p.Description, "leader")
		assert.Contains(t, p.Description, "non-streaming")
	})

	t.Run("ignores replica pointing at a different leader (StaleReplicationPrimary handles that)", func(t *testing.T) {
		stuck := time.Now().Add(-(stuckReplicationThreshold + 5*time.Second))
		stalePA := replicaPointingAtLeader(stuck)
		// Override the replica's self-claim to name some other pooler.
		otherID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "other"}
		stalePA.ConsensusStatus.ReplicationPrimary.Rule.LeaderId = otherID
		problems, err := analyzer.Analyze(baseShard(stalePA))
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("ignores poolers that haven't published the field", func(t *testing.T) {
		// Older pooler version: WalReceiverNotStreamingSince is the
		// zero value because the pooler didn't populate it.
		problems, err := analyzer.Analyze(baseShard(replicaPointingAtLeader(time.Time{})))
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("ignores the cluster leader itself", func(t *testing.T) {
		// Leaders don't have WAL receivers; if one somehow showed a
		// stuck-since timestamp, we still shouldn't flag it.
		stuck := time.Now().Add(-(stuckReplicationThreshold + 5*time.Second))
		leaderWithSince := &PoolerAnalysis{
			PoolerID:                     leaderID,
			ShardKey:                     shardKey,
			IsLeader:                     true,
			WalReceiverNotStreamingSince: stuck,
		}
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderObservation: leaderObs,
			Leader:            leaderWithSince,
			Analyses:          []*PoolerAnalysis{leaderWithSince},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("ignores when no cluster leader observation", func(t *testing.T) {
		stuck := time.Now().Add(-(stuckReplicationThreshold + 5*time.Second))
		sa := baseShard(replicaPointingAtLeader(stuck))
		sa.LeaderObservation = nil
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("metadata accessors return expected values", func(t *testing.T) {
		assert.Equal(t, types.CheckName("StuckReplication"), analyzer.Name())
		assert.Equal(t, types.ProblemReplicaStuck, analyzer.ProblemCode())
		assert.Nil(t, analyzer.RecoveryAction(), "observability-only: no recovery action")
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		nilFactoryAnalyzer := &StuckReplicationAnalyzer{factory: nil}
		problems, err := nilFactoryAnalyzer.Analyze(&ShardAnalysis{ShardKey: shardKey})
		require.Error(t, err)
		assert.Nil(t, problems)
		assert.Contains(t, err.Error(), "factory not initialized")
	})
}
