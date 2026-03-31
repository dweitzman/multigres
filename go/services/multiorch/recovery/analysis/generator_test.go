// Copyright 2025 Supabase, Inc.
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
	"google.golang.org/protobuf/types/known/timestamppb"

	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestAnalysisGenerator_GenerateShardAnalyses_EmptyStore(t *testing.T) {
	generator := NewAnalysisGenerator(store.NewPoolerStore(nil, slog.Default()))

	analyses := generator.GenerateShardAnalyses()

	assert.Empty(t, analyses, "should return empty slice for empty store")
}

func TestAnalysisGenerator_GenerateShardAnalyses_SinglePrimary(t *testing.T) {
	ps := store.NewPoolerStore(nil, slog.Default())

	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-1",
	}

	ps.Set("multipooler-cell1-primary-1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         primaryID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
		PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
			Lsn:   "0/1234567",
			Ready: true,
		},
	})

	generator := NewAnalysisGenerator(ps)
	analyses := generator.GenerateShardAnalyses()

	require.Len(t, analyses, 1, "should generate one shard analysis")
	shard := analyses[0]
	assert.Equal(t, "testdb", shard.ShardKey.Database)
	assert.Equal(t, "testtg", shard.ShardKey.TableGroup)
	assert.Equal(t, "0", shard.ShardKey.Shard)
	require.Len(t, shard.Poolers, 1)
	assert.True(t, shard.Poolers[0].IsPrimary)
	assert.True(t, shard.Poolers[0].LastCheckValid)
	assert.Equal(t, "primary-1", shard.Poolers[0].ID.Name)
}

func TestAnalysisGenerator_GenerateShardAnalyses_MultiplePoolers(t *testing.T) {
	ps := store.NewPoolerStore(nil, slog.Default())

	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary-1"}
	replica1ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica-1"}
	replica2ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica-2"}

	ps.Set("multipooler-cell1-primary-1", &multiorchdatapb.PoolerHealthState{
		MultiPooler:      &clustermetadatapb.MultiPooler{Id: primaryID, Database: "testdb", TableGroup: "testtg", Shard: "0", Type: clustermetadatapb.PoolerType_PRIMARY},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
	})
	ps.Set("multipooler-cell1-replica-1", &multiorchdatapb.PoolerHealthState{
		MultiPooler:      &clustermetadatapb.MultiPooler{Id: replica1ID, Database: "testdb", TableGroup: "testtg", Shard: "0", Type: clustermetadatapb.PoolerType_REPLICA},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
	})
	ps.Set("multipooler-cell1-replica-2", &multiorchdatapb.PoolerHealthState{
		MultiPooler:      &clustermetadatapb.MultiPooler{Id: replica2ID, Database: "testdb", TableGroup: "testtg", Shard: "0", Type: clustermetadatapb.PoolerType_REPLICA},
		IsLastCheckValid: false, // Unreachable
		IsUpToDate:       false,
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
	})

	generator := NewAnalysisGenerator(ps)
	analyses := generator.GenerateShardAnalyses()

	require.Len(t, analyses, 1, "should generate one shard analysis for all three poolers")
	shard := analyses[0]
	assert.Len(t, shard.Poolers, 3, "shard should contain all three poolers")

	// Verify we can find primary using shard method
	primary := shard.FindPrimary()
	require.NotNil(t, primary)
	assert.Equal(t, "primary-1", primary.ID.Name)
}

func TestAnalysisGenerator_GenerateShardAnalyses_MultipleTableGroups(t *testing.T) {
	ps := store.NewPoolerStore(nil, slog.Default())

	ps.Set("multipooler-cell1-tg1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "tg1-primary"},
			Database:   "testdb",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
	})

	ps.Set("multipooler-cell1-tg2-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "tg2-primary"},
			Database:   "testdb",
			TableGroup: "tg2",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
	})

	generator := NewAnalysisGenerator(ps)
	analyses := generator.GenerateShardAnalyses()

	require.Len(t, analyses, 2, "should generate one shard analysis per table group")
	tableGroups := make(map[string]bool)
	for _, a := range analyses {
		tableGroups[a.ShardKey.TableGroup] = true
	}
	assert.True(t, tableGroups["tg1"])
	assert.True(t, tableGroups["tg2"])
}

func TestAnalysisGenerator_GenerateShardAnalyses_SkipsNilEntries(t *testing.T) {
	ps := store.NewPoolerStore(nil, slog.Default())

	ps.Set("nil-pooler", nil)
	ps.Set("valid-pooler", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "valid"},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "shard1",
		},
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		IsLastCheckValid: true,
	})

	gen := NewAnalysisGenerator(ps)
	analyses := gen.GenerateShardAnalyses()

	require.Len(t, analyses, 1, "should generate one shard, skipping nil entry")
	assert.Equal(t, "db1", analyses[0].ShardKey.Database)
	assert.Len(t, analyses[0].Poolers, 1, "shard should contain only the valid pooler")
}

func TestAnalysisGenerator_GenerateShardAnalysis_NotFound(t *testing.T) {
	ps := store.NewPoolerStore(nil, slog.Default())
	gen := NewAnalysisGenerator(ps)

	_, err := gen.GenerateShardAnalysis(commontypes.ShardKey{Database: "missing", TableGroup: "tg", Shard: "0"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard not found")
}

func TestAnalysisGenerator_GenerateShardAnalysis_Found(t *testing.T) {
	ps := store.NewPoolerStore(nil, slog.Default())

	shardKey := commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"}
	ps.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"},
			Database:   shardKey.Database,
			TableGroup: shardKey.TableGroup,
			Shard:      shardKey.Shard,
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		},
		PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
		IsLastCheckValid: true,
	})

	gen := NewAnalysisGenerator(ps)
	shard, err := gen.GenerateShardAnalysis(shardKey)

	require.NoError(t, err)
	require.NotNil(t, shard)
	assert.Equal(t, shardKey, shard.ShardKey)
	require.Len(t, shard.Poolers, 1)
}

func TestBuildPoolerState_TypeResolution(t *testing.T) {
	// Health check type should take precedence over topology type when not UNKNOWN.
	t.Run("health check type takes precedence over topology type", func(t *testing.T) {
		pooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p"},
				Type: clustermetadatapb.PoolerType_REPLICA, // Topology says REPLICA
			},
			PoolerType: clustermetadatapb.PoolerType_PRIMARY, // Health check says PRIMARY
		}
		ps := buildPoolerState(pooler)
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, ps.Type)
		assert.True(t, ps.IsPrimary)
	})

	t.Run("falls back to topology type when health check is UNKNOWN", func(t *testing.T) {
		pooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p"},
				Type: clustermetadatapb.PoolerType_PRIMARY, // Topology says PRIMARY
			},
			PoolerType: clustermetadatapb.PoolerType_UNKNOWN, // Health check unknown
		}
		ps := buildPoolerState(pooler)
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, ps.Type)
		assert.True(t, ps.IsPrimary)
	})
}

func TestBuildPoolerState_ConsensusTerms(t *testing.T) {
	primaryTermValue := int64(7)
	consensusTermValue := int64(15)

	pooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p"},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		ConsensusTerm: &multipoolermanagerdatapb.ConsensusTerm{
			PrimaryTerm: primaryTermValue,
		},
		ConsensusStatus: &consensusdatapb.StatusResponse{
			CurrentTerm: consensusTermValue,
		},
	}

	ps := buildPoolerState(pooler)
	assert.Equal(t, primaryTermValue, ps.PrimaryTerm)
	assert.Equal(t, consensusTermValue, ps.ConsensusTerm)
}

func TestBuildPoolerState_StaleAndInitializedFlags(t *testing.T) {
	t.Run("IsStale is true when not up to date", func(t *testing.T) {
		pooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p"},
				Type: clustermetadatapb.PoolerType_REPLICA,
			},
			IsUpToDate:        false, // Stale
			IsLastCheckValid:  true,
			IsPostgresRunning: true,
		}
		ps := buildPoolerState(pooler)
		assert.True(t, ps.IsStale)
	})

	t.Run("IsInitialized reflects store.IsInitialized logic", func(t *testing.T) {
		pooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p"},
				Type: clustermetadatapb.PoolerType_PRIMARY,
			},
			IsLastCheckValid: true,
			IsInitialized:    true, // store.IsInitialized reads this field directly
		}
		ps := buildPoolerState(pooler)
		assert.True(t, ps.IsInitialized)
	})
}

func TestAnalysisGenerator_PoolersGroupedCorrectlyByShard(t *testing.T) {
	// Two poolers in same shard should appear in the same ShardAnalysis.
	// Two poolers in different shards should appear in different ShardAnalyses.
	ps := store.NewPoolerStore(nil, slog.Default())

	ps.Set("shard0-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "c1", Name: "s0p"},
			Database: "db", TableGroup: "tg", Shard: "0",
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	})
	ps.Set("shard0-replica", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "c1", Name: "s0r"},
			Database: "db", TableGroup: "tg", Shard: "0",
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	})
	ps.Set("shard1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "c1", Name: "s1p"},
			Database: "db", TableGroup: "tg", Shard: "1",
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	})

	gen := NewAnalysisGenerator(ps)
	analyses := gen.GenerateShardAnalyses()

	require.Len(t, analyses, 2, "should have one analysis per shard")

	shardMap := make(map[string]*ShardAnalysis)
	for _, a := range analyses {
		shardMap[a.ShardKey.Shard] = a
	}

	require.Contains(t, shardMap, "0")
	require.Contains(t, shardMap, "1")
	assert.Len(t, shardMap["0"].Poolers, 2, "shard 0 has 2 poolers")
	assert.Len(t, shardMap["1"].Poolers, 1, "shard 1 has 1 pooler")
}
