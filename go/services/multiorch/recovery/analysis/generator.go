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
	"fmt"
	"time"

	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// DefaultReplicaLagThreshold is the threshold above which a replica is considered lagging.
const DefaultReplicaLagThreshold = 10 * time.Second

// PoolersByShard is a structured map for efficient lookups.
// Structure: [database][tablegroup][shard][pooler_id] -> PoolerHealthState
type PoolersByShard map[string]map[string]map[string]map[string]*multiorchdatapb.PoolerHealthState

// AnalysisGenerator creates ReplicationAnalysis from the pooler store.
type AnalysisGenerator struct {
	poolerStore    *store.PoolerStore
	poolersByShard PoolersByShard
}

// NewAnalysisGenerator creates a new analysis generator.
// It eagerly builds the poolersByShard map from the current store state.
func NewAnalysisGenerator(poolerStore *store.PoolerStore) *AnalysisGenerator {
	g := &AnalysisGenerator{
		poolerStore: poolerStore,
	}
	g.poolersByShard = g.buildPoolersByShard()
	return g
}

// GenerateShardAnalyses creates one ShardAnalysis per shard in the store.
// Each ShardAnalysis contains the full state of all poolers in that shard.
// Analyzers receive this and compute whatever cross-pooler aggregation they need.
func (g *AnalysisGenerator) GenerateShardAnalyses() []*ShardAnalysis {
	var shardAnalyses []*ShardAnalysis

	for database, tableGroups := range g.poolersByShard {
		for tableGroup, shards := range tableGroups {
			for shard, poolers := range shards {
				shardKey := commontypes.ShardKey{
					Database:   database,
					TableGroup: tableGroup,
					Shard:      shard,
				}
				shardAnalyses = append(shardAnalyses, g.buildShardAnalysis(shardKey, poolers))
			}
		}
	}

	return shardAnalyses
}

// GenerateShardAnalysis creates a ShardAnalysis for a specific shard.
// Used for targeted re-analysis (e.g., in recheckProblem) after re-polling poolers.
func (g *AnalysisGenerator) GenerateShardAnalysis(shardKey commontypes.ShardKey) (*ShardAnalysis, error) {
	poolers, ok := g.poolersByShard[shardKey.Database][shardKey.TableGroup][shardKey.Shard]
	if !ok {
		return nil, fmt.Errorf("shard not found: %s", shardKey)
	}
	return g.buildShardAnalysis(shardKey, poolers), nil
}

// buildShardAnalysis constructs a ShardAnalysis from a map of pooler health states.
func (g *AnalysisGenerator) buildShardAnalysis(
	shardKey commontypes.ShardKey,
	poolers map[string]*multiorchdatapb.PoolerHealthState,
) *ShardAnalysis {
	shard := &ShardAnalysis{
		ShardKey:   shardKey,
		AnalyzedAt: time.Now(),
	}
	for _, pooler := range poolers {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			continue
		}
		shard.Poolers = append(shard.Poolers, buildPoolerState(pooler))
	}
	return shard
}

// buildPoolersByShard creates a structured map by iterating the store once.
// Since ProtoStore.Range() returns clones, we don't need explicit DeepCopy.
func (g *AnalysisGenerator) buildPoolersByShard() PoolersByShard {
	poolersByShard := make(PoolersByShard)

	g.poolerStore.Range(func(poolerID string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // skip nil entries
		}

		database := pooler.MultiPooler.Database
		tableGroup := pooler.MultiPooler.TableGroup
		shard := pooler.MultiPooler.Shard

		// Initialize nested maps if needed
		if poolersByShard[database] == nil {
			poolersByShard[database] = make(map[string]map[string]map[string]*multiorchdatapb.PoolerHealthState)
		}
		if poolersByShard[database][tableGroup] == nil {
			poolersByShard[database][tableGroup] = make(map[string]map[string]*multiorchdatapb.PoolerHealthState)
		}
		if poolersByShard[database][tableGroup][shard] == nil {
			poolersByShard[database][tableGroup][shard] = make(map[string]*multiorchdatapb.PoolerHealthState)
		}

		// Store the pooler (already a clone from Range)
		poolersByShard[database][tableGroup][shard][poolerID] = pooler
		return true // continue
	})

	return poolersByShard
}

// buildPoolerState constructs a PoolerState from a raw health state.
// Resolves the pooler type (health check authoritative, topology as fallback) and
// pre-populates lightweight computed fields to avoid duplication across analyzers.
func buildPoolerState(pooler *multiorchdatapb.PoolerHealthState) *PoolerState {
	// Determine pooler type: health check is authoritative since nodes report their
	// actual running state. Fall back to topology type if health check is UNKNOWN.
	poolerType := pooler.PoolerType
	if poolerType == clustermetadatapb.PoolerType_UNKNOWN {
		poolerType = pooler.MultiPooler.Type
	}

	ps := &PoolerState{
		Health:           pooler,
		ID:               pooler.MultiPooler.Id,
		Type:             poolerType,
		IsPrimary:        poolerType == clustermetadatapb.PoolerType_PRIMARY,
		LastCheckValid:   pooler.IsLastCheckValid,
		IsStale:          !pooler.IsUpToDate,
		IsInitialized:    store.IsInitialized(pooler),
		HasDataDirectory: pooler.HasDataDirectory,
	}

	if pooler.ConsensusStatus != nil {
		ps.ConsensusTerm = pooler.ConsensusStatus.CurrentTerm
	}
	if pooler.ConsensusTerm != nil {
		ps.PrimaryTerm = pooler.ConsensusTerm.PrimaryTerm
	}

	return ps
}
