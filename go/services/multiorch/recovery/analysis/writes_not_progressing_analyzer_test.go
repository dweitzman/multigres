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
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestWritesNotProgressingAnalyzer_Analyze(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()
	rpcClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(rpcClient, slog.Default())
	cfg := config.NewTestConfig()
	coordID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell1", Name: "test-coord"}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(cfg, poolerStore, rpcClient, ts, coord, slog.Default())
	analyzer := &WritesNotProgressingAnalyzer{factory: factory}

	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "leader"}
	replica1ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica-1"}
	replica2ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica-2"}
	leaderObs := &clustermetadatapb.LeaderObservation{
		LeaderId:         leaderID,
		LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
	}
	leaderPA := &PoolerAnalysis{PoolerID: leaderID, ShardKey: shardKey, IsLeader: true, IsInitialized: true}

	// streamingReplica returns a replica PoolerAnalysis that the WAL
	// receiver state says is currently streaming (WalReceiverNotStreamingSince==zero)
	// with the given last-observed-LSN-advance timestamp.
	streamingReplica := func(id *clustermetadatapb.ID, lastAdvance time.Time) *PoolerAnalysis {
		return &PoolerAnalysis{
			PoolerID:                     id,
			ShardKey:                     shardKey,
			IsLeader:                     false,
			IsInitialized:                true,
			WalReceiverNotStreamingSince: time.Time{},
			LastLsnAdvance:               lastAdvance,
		}
	}

	t.Run("emits ProblemWritesNotProgressing when all streaming replicas have stale LSN advance", func(t *testing.T) {
		stale := time.Now().Add(-(writesNotProgressingThreshold + 5*time.Second))
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderObservation: leaderObs,
			Leader:            leaderPA,
			Analyses: []*PoolerAnalysis{
				leaderPA,
				streamingReplica(replica1ID, stale),
				streamingReplica(replica2ID, stale),
			},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		p := problems[0]
		assert.Equal(t, types.ProblemWritesNotProgressing, p.Code)
		assert.Equal(t, types.ScopeShard, p.Scope)
		assert.Equal(t, types.PriorityEmergency, p.Priority)
		assert.Contains(t, p.Description, "2 replica(s)")
		assert.Contains(t, p.Description, "leader")
		require.NotNil(t, p.RecoveryAction)
	})

	t.Run("short-circuits when any replica's LSN advanced recently", func(t *testing.T) {
		stale := time.Now().Add(-(writesNotProgressingThreshold + 5*time.Second))
		recent := time.Now().Add(-1 * time.Second)
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderObservation: leaderObs,
			Leader:            leaderPA,
			Analyses: []*PoolerAnalysis{
				leaderPA,
				streamingReplica(replica1ID, stale),
				streamingReplica(replica2ID, recent), // one keeps moving
			},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems, "any moving replica means the primary is writing")
	})

	t.Run("no problem when no streaming replicas exist", func(t *testing.T) {
		// All replicas have a non-zero WalReceiverNotStreamingSince —
		// they're not claiming to be streaming, so we have no witness
		// for primary forward-progress. StuckReplication / StaleReplication
		// handle the disconnected case.
		disconnected := time.Now().Add(-2 * time.Minute)
		stale := time.Now().Add(-(writesNotProgressingThreshold + 5*time.Second))
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderObservation: leaderObs,
			Leader:            leaderPA,
			Analyses: []*PoolerAnalysis{
				leaderPA,
				{
					PoolerID:                     replica1ID,
					ShardKey:                     shardKey,
					IsLeader:                     false,
					IsInitialized:                true,
					WalReceiverNotStreamingSince: disconnected,
					LastLsnAdvance:               stale,
				},
			},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("ignores replicas that have never observed an LSN advance", func(t *testing.T) {
		// LastLsnAdvance==zero on a streaming replica means we haven't
		// yet stamped one (newly discovered, or first snapshot).
		// Don't count toward staleness either way.
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderObservation: leaderObs,
			Leader:            leaderPA,
			Analyses: []*PoolerAnalysis{
				leaderPA,
				streamingReplica(replica1ID, time.Time{}),
			},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("no problem when only the leader exists", func(t *testing.T) {
		// Single-node shard — no witnesses. Other analyzers handle the
		// "leader is dead" case via direct reachability checks.
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderObservation: leaderObs,
			Leader:            leaderPA,
			Analyses:          []*PoolerAnalysis{leaderPA},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("skips uninitialized replicas", func(t *testing.T) {
		stale := time.Now().Add(-(writesNotProgressingThreshold + 5*time.Second))
		recent := time.Now().Add(-1 * time.Second)
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderObservation: leaderObs,
			Leader:            leaderPA,
			Analyses: []*PoolerAnalysis{
				leaderPA,
				// Uninitialized replica with stale advance — ignored
				{
					PoolerID:                     replica1ID,
					ShardKey:                     shardKey,
					IsLeader:                     false,
					IsInitialized:                false,
					WalReceiverNotStreamingSince: time.Time{},
					LastLsnAdvance:               stale,
				},
				// Initialized streaming replica with recent advance — primary alive
				streamingReplica(replica2ID, recent),
			},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		assert.Empty(t, problems)
	})

	t.Run("metadata accessors return expected values", func(t *testing.T) {
		assert.Equal(t, types.CheckName("WritesNotProgressing"), analyzer.Name())
		assert.Equal(t, types.ProblemWritesNotProgressing, analyzer.ProblemCode())
		require.NotNil(t, analyzer.RecoveryAction())
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		nilFactoryAnalyzer := &WritesNotProgressingAnalyzer{factory: nil}
		problems, err := nilFactoryAnalyzer.Analyze(&ShardAnalysis{ShardKey: shardKey})
		require.Error(t, err)
		assert.Nil(t, problems)
		assert.Contains(t, err.Error(), "factory not initialized")
	})
}
