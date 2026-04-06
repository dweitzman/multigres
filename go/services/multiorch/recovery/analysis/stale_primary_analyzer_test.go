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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// testNodePos builds a NodePosition with the given rule number for use in tests.
func testNodePos(coordTerm, subterm int64) *clustermetadatapb.NodePosition {
	return &clustermetadatapb.NodePosition{
		Rule: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{
				CoordinatorTerm: coordTerm,
				RuleSubterm:     subterm,
			},
		},
	}
}

// testPrimaryInfo builds a PrimaryInfo for use in tests.
func testPrimaryInfo(name, cell string, coordTerm, subterm int64) *store.PrimaryInfo {
	return &store.PrimaryInfo{
		ID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cell,
			Name:      name,
		},
		Position: testNodePos(coordTerm, subterm),
	}
}

func TestStalePrimaryAnalyzer_Analyze(t *testing.T) {
	factory := &RecoveryActionFactory{poolerStore: store.NewPoolerStore(nil, slog.Default())}

	t.Run("detects stale primary when this pooler has lower committed rule", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "stale-primary",
			},
			ShardKey:        commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:       true,
			IsInitialized:   true,
			PrimaryPosition: testNodePos(5, 0),
			OtherPrimariesInShard: []*store.PrimaryInfo{
				testPrimaryInfo("new-primary", "cell1", 6, 0),
			},
			HighestTermPrimary: testPrimaryInfo("new-primary", "cell1", 6, 0),
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.NotNil(t, problem)
		assert.Equal(t, types.ProblemStalePrimary, problem.Code)
		assert.Equal(t, types.ScopeShard, problem.Scope)
		assert.Equal(t, types.PriorityEmergency, problem.Priority)
		assert.Equal(t,
			"Stale primary detected: stale-primary (Rule[5.0]) is stale, most advanced primary new-primary (Rule[6.0])",
			problem.Description)
	})

	t.Run("detects other primary as stale when this pooler has higher committed rule", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "new-primary",
			},
			ShardKey:        commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:       true,
			IsInitialized:   true,
			PrimaryPosition: testNodePos(6, 0),
			OtherPrimariesInShard: []*store.PrimaryInfo{
				testPrimaryInfo("stale-primary", "cell1", 5, 0),
			},
			HighestTermPrimary: testPrimaryInfo("new-primary", "cell1", 6, 0),
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.NotNil(t, problem, "should detect other primary as stale")
		assert.Equal(t, types.ProblemStalePrimary, problem.Code)
		assert.Equal(t, "stale-primary", problem.PoolerID.Name, "should report the stale primary")
		assert.Equal(t,
			"Stale primary detected: stale-primary (Rule[5.0]) is stale, most advanced primary new-primary (Rule[6.0])",
			problem.Description)
	})

	t.Run("does not demote when committed rules are equal (consensus bug)", func(t *testing.T) {
		// When committed rules are equal, this indicates a consensus bug — two primaries
		// should not reach the same rule. Skip automatic demotion to avoid making it worse.
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary-a",
			},
			ShardKey:        commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:       true,
			IsInitialized:   true,
			PrimaryPosition: testNodePos(5, 0),
			OtherPrimariesInShard: []*store.PrimaryInfo{
				testPrimaryInfo("primary-b", "cell1", 5, 0),
			},
			HighestTermPrimary: nil, // Tie detected, so nil
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Nil(t, problem, "should NOT demote when committed rules are equal to prevent double demotion")
	})

	t.Run("ignores replicas", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica1",
			},
			ShardKey:      commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:     false,
			IsInitialized: true,
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("ignores when no other primary detected", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			ShardKey:              commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:             true,
			IsInitialized:         true,
			PrimaryPosition:       testNodePos(5, 0),
			OtherPrimariesInShard: nil,
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("ignores uninitialized primary", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			ShardKey:      commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:     true,
			IsInitialized: false,
			OtherPrimariesInShard: []*store.PrimaryInfo{
				testPrimaryInfo("other-primary", "cell1", 5, 0),
			},
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: nil}
		analysis := &store.ReplicationAnalysis{IsPrimary: true}

		_, err := analyzer.Analyze(analysis)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "factory not initialized")
	})

	t.Run("handles multiple other primaries", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "new-primary",
			},
			ShardKey:        commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:       true,
			IsInitialized:   true,
			PrimaryPosition: testNodePos(6, 0),
			OtherPrimariesInShard: []*store.PrimaryInfo{
				testPrimaryInfo("stale-primary-1", "cell1", 4, 0),
				testPrimaryInfo("stale-primary-2", "cell1", 5, 0),
			},
			HighestTermPrimary: testPrimaryInfo("new-primary", "cell1", 6, 0),
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.NotNil(t, problem)
		// Should report first stale primary (they'll be demoted one at a time)
		assert.Equal(t, "stale-primary-1", problem.PoolerID.Name)
	})

	t.Run("skips when this pooler has nil position (invalid state)", func(t *testing.T) {
		// Note: This tests the invariant check. In a properly initialized shard,
		// PRIMARY poolers should never have a nil PrimaryPosition.
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary-a",
			},
			ShardKey:        commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:       true,
			IsInitialized:   true,
			PrimaryPosition: nil, // Invalid: initialized PRIMARY should never have nil PrimaryPosition
			OtherPrimariesInShard: []*store.PrimaryInfo{
				testPrimaryInfo("primary-b", "cell1", 5, 0),
			},
			HighestTermPrimary: testPrimaryInfo("primary-b", "cell1", 5, 0),
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Nil(t, problem, "should skip when this pooler's PrimaryPosition is nil (invalid state)")
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		assert.Equal(t, types.CheckName("StalePrimary"), analyzer.Name())
	})
}
