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

func TestReplicaNotReplicatingAnalyzer_Analyze(t *testing.T) {
	// Set up factory for tests
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

	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"}
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	// reachableLeader is the PoolerAnalysis for a healthy primary that
	// has already observed its own rule — the precondition the analyzer
	// needs (sa.Leader != nil && sa.LeaderReachable) before it will
	// generate a ReplicaNotReplicating problem.
	reachableLeader := &PoolerAnalysis{
		PoolerID: primaryID,
		ShardKey: shardKey,
		IsLeader: true,
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				},
			},
		},
	}

	t.Run("detects replica with no primary_conninfo", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:        shardKey,
			LeaderReachable: true,
			Leader:          reachableLeader,
			Analyses: []*PoolerAnalysis{{
				PoolerID:            replicaID,
				ShardKey:            shardKey,
				IsLeader:            false,
				IsInitialized:       true,
				PrimaryConnInfoHost: "", // No primary_conninfo configured
				ReplicationStopped:  false,
			}},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemReplicaNotReplicating, problems[0].Code)
		require.Equal(t, types.ScopePooler, problems[0].Scope)
		require.Equal(t, types.PriorityHigh, problems[0].Priority)
		require.NotNil(t, problems[0].RecoveryAction)
	})

	t.Run("detects replica with replication stopped", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:        shardKey,
			LeaderReachable: true,
			Leader:          reachableLeader,
			Analyses: []*PoolerAnalysis{{
				PoolerID:            replicaID,
				ShardKey:            shardKey,
				IsLeader:            false,
				IsInitialized:       true,
				PrimaryConnInfoHost: "primary.example.com",
				ReplicationStopped:  true, // Replication stopped
			}},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemReplicaNotReplicating, problems[0].Code)
	})

	// Skip the problem when there's no usable primary yet.
	// HighestTermReachableLeader is nil whenever the leader's rule hasn't
	// been observed (findHighestTermLeader filters those out), so we have no
	// rule to put in SetTermPrimaryRequest. Waiting one cycle is cheap; the next
	// health snapshot will repopulate the field.
	t.Run("skips when no usable primary is known", func(t *testing.T) {
		// sa.Leader == nil signals "no cluster-authoritative leader pooler
		// is in our known set yet" — the analyzer needs a leader to put
		// in SetTermPrimaryRequest.Rule, so it bows out.
		sa := &ShardAnalysis{
			ShardKey:        shardKey,
			LeaderReachable: false,
			Analyses: []*PoolerAnalysis{{
				PoolerID:            replicaID,
				ShardKey:            shardKey,
				IsLeader:            false,
				IsInitialized:       true,
				PrimaryConnInfoHost: "",
			}},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores replica with healthy replication", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:        shardKey,
			LeaderReachable: true,
			Leader:          reachableLeader,
			Analyses: []*PoolerAnalysis{{
				PoolerID:            replicaID,
				ShardKey:            shardKey,
				IsLeader:            false,
				IsInitialized:       true,
				PrimaryConnInfoHost: "primary.example.com",
				ReplicationStopped:  false,
			}},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores primary nodes", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey: shardKey,
			Analyses: []*PoolerAnalysis{{
				PoolerID:            primaryID,
				ShardKey:            shardKey,
				IsLeader:            true,
				IsInitialized:       true,
				PrimaryConnInfoHost: "", // Primaries don't have primary_conninfo
			}},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores uninitialized replica", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey: shardKey,
			Analyses: []*PoolerAnalysis{{
				PoolerID:            replicaID,
				ShardKey:            shardKey,
				IsLeader:            false,
				IsInitialized:       false, // Not initialized
				PrimaryConnInfoHost: "",
			}},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores replica when primary is unreachable", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:        shardKey,
			LeaderReachable: false, // Primary unreachable — PrimaryIsDead handles this
			Analyses: []*PoolerAnalysis{{
				PoolerID:            replicaID,
				ShardKey:            shardKey,
				IsLeader:            false,
				IsInitialized:       true,
				PrimaryConnInfoHost: "",
			}},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("detects replica whose ReplicationPrimary names a wrong (non-self) leader", func(t *testing.T) {
		// Replica's primary_conninfo is set, replication isn't stopped, but
		// its self-reported ReplicationPrimary names the old leader at term 1
		// while the cluster has advanced to term 2 under a different leader.
		// SetTermPrimary against the new leader will retarget it.
		newLeaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "new-leader"}
		staleLeaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "old-leader"}
		newLeaderPA := &PoolerAnalysis{
			PoolerID: newLeaderID,
			ShardKey: shardKey,
			IsLeader: true,
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
						LeaderId:   newLeaderID,
					},
				},
			},
		}
		sa := &ShardAnalysis{
			ShardKey:        shardKey,
			LeaderReachable: true,
			Leader:          newLeaderPA,
			LeaderObservation: &clustermetadatapb.LeaderObservation{
				LeaderId:         newLeaderID,
				LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
			},
			Analyses: []*PoolerAnalysis{{
				PoolerID:            replicaID,
				ShardKey:            shardKey,
				IsLeader:            false,
				IsInitialized:       true,
				PrimaryConnInfoHost: "old-leader.example.com",
				ReplicationStopped:  false,
				ConsensusStatus: &clustermetadatapb.ConsensusStatus{
					ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
						Rule: &clustermetadatapb.ShardRule{
							RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
							LeaderId:   staleLeaderID,
						},
					},
				},
			}},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemReplicaNotReplicating, problems[0].Code)
		require.Equal(t, replicaID.Name, problems[0].PoolerID.Name)
	})

	t.Run("ignores replica whose ReplicationPrimary matches cluster leader", func(t *testing.T) {
		// Healthy steady state: replica's ReplicationPrimary names the same
		// LeaderId as the cluster, even if the rule number is slightly older.
		// Per the staleness model, same LeaderId at any rule = not stale.
		sa := &ShardAnalysis{
			ShardKey:        shardKey,
			LeaderReachable: true,
			Leader:          reachableLeader,
			LeaderObservation: &clustermetadatapb.LeaderObservation{
				LeaderId:         primaryID,
				LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
			},
			Analyses: []*PoolerAnalysis{{
				PoolerID:            replicaID,
				ShardKey:            shardKey,
				IsLeader:            false,
				IsInitialized:       true,
				PrimaryConnInfoHost: "primary.example.com",
				ReplicationStopped:  false,
				ConsensusStatus: &clustermetadatapb.ConsensusStatus{
					ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
						Rule: &clustermetadatapb.ShardRule{
							RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
							LeaderId:   primaryID,
						},
					},
				},
			}},
		}

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("ReplicaNotReplicating"), analyzer.Name())
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		nilFactoryAnalyzer := &ReplicaNotReplicatingAnalyzer{factory: nil}
		sa := &ShardAnalysis{
			ShardKey:        shardKey,
			LeaderReachable: true,
			Leader:          reachableLeader,
			Analyses: []*PoolerAnalysis{{
				PoolerID:            replicaID,
				ShardKey:            shardKey,
				IsLeader:            false,
				IsInitialized:       true,
				PrimaryConnInfoHost: "",
			}},
		}

		problems, err := nilFactoryAnalyzer.Analyze(sa)
		require.Error(t, err)
		require.Nil(t, problems)
		require.Contains(t, err.Error(), "factory not initialized")
	})
}
