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

func TestStalePrimaryAnalyzer_Analyze(t *testing.T) {
	factory := &RecoveryActionFactory{poolerStore: store.NewPoolerStore(nil, slog.Default())}
	shardKey := commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"}

	t.Run("detects stale primary when it has lower primary_term", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-primary"},
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					PrimaryTerm:    5,
				},
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "new-primary"},
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					PrimaryTerm:    6,
				},
			},
		}

		problems, err := analyzer.Analyze(shard)

		require.NoError(t, err)
		require.Len(t, problems, 1)
		assert.Equal(t, types.ProblemStalePrimary, problems[0].Code)
		assert.Equal(t, types.ScopeShard, problems[0].Scope)
		assert.Equal(t, types.PriorityEmergency, problems[0].Priority)
		assert.Equal(t, "stale-primary", problems[0].PoolerID.Name)
		assert.Contains(t, problems[0].Description, "stale-primary")
		assert.Contains(t, problems[0].Description, "primary_term=5")
		assert.Contains(t, problems[0].Description, "primary_term=6")
	})

	t.Run("detects stale primary regardless of iteration order", func(t *testing.T) {
		// The analyzer should detect the lower-term primary as stale regardless
		// of which primary appears first in the Poolers slice.
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "new-primary"},
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					PrimaryTerm:    6,
				},
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-primary"},
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					PrimaryTerm:    5,
				},
			},
		}

		problems, err := analyzer.Analyze(shard)

		require.NoError(t, err)
		require.Len(t, problems, 1, "should detect stale primary regardless of order")
		assert.Equal(t, "stale-primary", problems[0].PoolerID.Name, "should report the lower-term primary as stale")
	})

	t.Run("does not demote when primary_terms are equal (consensus bug)", func(t *testing.T) {
		// When primary_terms are equal, this indicates a consensus bug (PrimaryTerm should be
		// unique per primary). We skip automatic demotion to avoid making the situation worse.
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary-a"},
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					PrimaryTerm:    5,
				},
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary-b"},
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					PrimaryTerm:    5,
				},
			},
		}

		problems, err := analyzer.Analyze(shard)

		require.NoError(t, err)
		require.Empty(t, problems, "should NOT demote when primary_terms are equal to prevent double demotion")
	})

	t.Run("ignores replicas", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica1"},
					IsPrimary:      false,
					IsInitialized:  true,
					LastCheckValid: true,
				},
			},
		}

		problems, err := analyzer.Analyze(shard)

		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores when only one primary exists", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"},
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					PrimaryTerm:    5,
				},
			},
		}

		problems, err := analyzer.Analyze(shard)

		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores uninitialized primary", func(t *testing.T) {
		// An uninitialized primary is excluded from the eligible set. With only one
		// remaining eligible primary, there's no conflict to resolve.
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"},
					IsPrimary:      true,
					IsInitialized:  false, // Not initialized - excluded from conflict detection
					LastCheckValid: true,
					PrimaryTerm:    5,
				},
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "other-primary"},
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					PrimaryTerm:    6,
				},
			},
		}

		problems, err := analyzer.Analyze(shard)

		require.NoError(t, err)
		require.Empty(t, problems, "uninitialized primary is not eligible for conflict detection")
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: nil}
		shard := &ShardAnalysis{ShardKey: shardKey}

		_, err := analyzer.Analyze(shard)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "factory not initialized")
	})

	t.Run("detects all stale primaries when multiple exist", func(t *testing.T) {
		// When three primaries exist, the two with lower terms are both stale
		// and should both be reported so they can each be demoted.
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "new-primary"},
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					PrimaryTerm:    6,
				},
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-primary-1"},
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					PrimaryTerm:    4,
				},
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-primary-2"},
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					PrimaryTerm:    5,
				},
			},
		}

		problems, err := analyzer.Analyze(shard)

		require.NoError(t, err)
		require.Len(t, problems, 2, "should detect all stale primaries")
		staleNames := make(map[string]bool)
		for _, p := range problems {
			staleNames[p.PoolerID.Name] = true
		}
		assert.True(t, staleNames["stale-primary-1"])
		assert.True(t, staleNames["stale-primary-2"])
	})

	t.Run("skips primary with primary_term zero (invalid state)", func(t *testing.T) {
		// Initialized PRIMARY poolers should never have PrimaryTerm=0. If they do, they're
		// excluded from conflict detection to avoid acting on bad data.
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		shard := &ShardAnalysis{
			ShardKey: shardKey,
			Poolers: []*PoolerState{
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary-a"},
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					PrimaryTerm:    0, // Invalid
				},
				{
					ID:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary-b"},
					IsPrimary:      true,
					IsInitialized:  true,
					LastCheckValid: true,
					PrimaryTerm:    5,
				},
			},
		}

		problems, err := analyzer.Analyze(shard)

		require.NoError(t, err)
		require.Empty(t, problems, "primary-a is excluded due to PrimaryTerm=0, leaving only 1 eligible primary")
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		assert.Equal(t, types.CheckName("StalePrimary"), analyzer.Name())
	})
}
