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

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// DefaultReplicaLagThreshold is the threshold above which a replica is considered lagging.
const DefaultReplicaLagThreshold = 10 * time.Second

// replicationHeartbeatStalenessMultiplier is applied to wal_receiver_status_interval
// to compute the heartbeat staleness threshold. The replica sends a status message
// to the primary every wal_receiver_status_interval; the primary echoes a keepalive
// reply. Three missed intervals means the primary has gone silent well before the
// wal_receiver_timeout (60s) would disconnect the WAL receiver.
const replicationHeartbeatStalenessMultiplier = 3

// defaultReplicationHeartbeatStalenessThreshold is the fallback threshold used
// when wal_receiver_status_interval is not available in the replica's health
// state. Equals replicationHeartbeatStalenessMultiplier × the default
// wal_receiver_status_interval (10s).
const defaultReplicationHeartbeatStalenessThreshold = 30 * time.Second

// PoolersByShard is a structured map for efficient lookups.
// Structure: [database][tablegroup][shard][pooler_id] -> PoolerHealthState
type PoolersByShard map[string]map[string]map[string]map[string]*multiorchdatapb.PoolerHealthState

// AnalysisGenerator creates ReplicationAnalysis from the pooler store.
type AnalysisGenerator struct {
	poolerStore    *store.PoolerStore
	poolersByShard PoolersByShard
	// policyLookup returns the bootstrap durability policy for a database name.
	// May be nil; when nil, ShardAnalysis.BootstrapDurabilityPolicy is left nil.
	policyLookup func(database string) *clustermetadatapb.DurabilityPolicy
	now          func() time.Time
}

// NewAnalysisGenerator creates a new analysis generator.
// It eagerly builds the poolersByShard map from the current store state.
// policyLookup is optional; pass nil if the bootstrap policy is unavailable.
func NewAnalysisGenerator(poolerStore *store.PoolerStore, policyLookup func(database string) *clustermetadatapb.DurabilityPolicy) *AnalysisGenerator {
	g := &AnalysisGenerator{
		poolerStore:  poolerStore,
		policyLookup: policyLookup,
		now:          time.Now,
	}
	g.poolersByShard = g.buildPoolersByShard()
	return g
}

// GenerateShardAnalyses groups per-pooler analyses into one ShardAnalysis per shard.
func (g *AnalysisGenerator) GenerateShardAnalyses() []*ShardAnalysis {
	type shardEntry struct {
		key     *clustermetadatapb.ShardKey
		poolers map[string]*multiorchdatapb.PoolerHealthState
	}
	byKey := make(map[string]*shardEntry)

	for database, tableGroups := range g.poolersByShard {
		for tableGroup, shards := range tableGroups {
			for shard, poolers := range shards {
				key := &clustermetadatapb.ShardKey{Database: database, TableGroup: tableGroup, Shard: shard}
				byKey[string(commontypes.FormatShardKey(key))] = &shardEntry{key: key, poolers: poolers}
			}
		}
	}

	result := make([]*ShardAnalysis, 0, len(byKey))
	for _, entry := range byKey {
		result = append(result, g.buildShardAnalysis(entry.key, entry.poolers))
	}
	return result
}

// GenerateShardAnalysis returns a ShardAnalysis for a specific shard.
// Returns an error if no poolers for that shard are found in the store.
func (g *AnalysisGenerator) GenerateShardAnalysis(shardKey *clustermetadatapb.ShardKey) (*ShardAnalysis, error) {
	poolers, ok := g.poolersByShard[shardKey.Database][shardKey.TableGroup][shardKey.Shard]
	if !ok || len(poolers) == 0 {
		return nil, fmt.Errorf("shard not found: %s", commontypes.FormatShardKey(shardKey))
	}
	return g.buildShardAnalysis(shardKey, poolers), nil
}

// buildShardAnalysis constructs a ShardAnalysis for a shard, including shard-level aggregates.
func (g *AnalysisGenerator) buildShardAnalysis(shardKey *clustermetadatapb.ShardKey, poolers map[string]*multiorchdatapb.PoolerHealthState) *ShardAnalysis {
	sa := &ShardAnalysis{ShardKey: shardKey}
	// Compute the cluster-wide authoritative LeaderObservation once and
	// thread it into per-pooler analysis. Roles are derived from "does
	// this pooler's id match the shard leader's?", not from PoolerType.
	poolerSlice := make([]*multiorchdatapb.PoolerHealthState, 0, len(poolers))
	for _, p := range poolers {
		poolerSlice = append(poolerSlice, p)
	}
	leaderObs, _ := store.ShardLeader(poolerSlice)
	for _, pooler := range poolers {
		sa.Analyses = append(sa.Analyses, g.generateAnalysisForPooler(pooler, shardKey, leaderObs))
	}
	g.computeShardLevelFields(sa, poolers)
	return sa
}

// buildPoolersByShard creates a structured map by iterating the store once.
// Since ProtoStore.Range() returns clones, we don't need explicit DeepCopy.
func (g *AnalysisGenerator) buildPoolersByShard() PoolersByShard {
	poolersByShard := make(PoolersByShard)

	g.poolerStore.Range(func(poolerID string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // skip nil entries
		}

		database := pooler.MultiPooler.GetShardKey().GetDatabase()
		tableGroup := pooler.MultiPooler.GetShardKey().GetTableGroup()
		shard := pooler.MultiPooler.GetShardKey().GetShard()

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

// GetPoolersInShard returns all pooler IDs in the same shard as the given pooler.
// Uses the cached poolersByShard for efficient lookup.
func (g *AnalysisGenerator) GetPoolersInShard(poolerIDStr string) ([]string, error) {
	// Get pooler from store to determine its shard
	pooler, ok := g.poolerStore.Get(poolerIDStr)
	if !ok {
		return nil, fmt.Errorf("pooler not found in store: %s", poolerIDStr)
	}

	if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
		return nil, fmt.Errorf("pooler or ID is nil: %s", poolerIDStr)
	}

	database := pooler.MultiPooler.GetShardKey().GetDatabase()
	tableGroup := pooler.MultiPooler.GetShardKey().GetTableGroup()
	shard := pooler.MultiPooler.GetShardKey().GetShard()

	// Use cached poolersByShard for efficient lookup
	poolers, ok := g.poolersByShard[database][tableGroup][shard]
	if !ok {
		return []string{}, nil
	}

	poolerIDs := make([]string, 0, len(poolers))
	for id := range poolers {
		poolerIDs = append(poolerIDs, id)
	}

	return poolerIDs, nil
}

// GenerateAnalysisForPooler generates and returns the ShardAnalysis for the shard containing
// the given pooler ID. Used primarily in tests to inspect shard-level fields like
// ReplicasConnectedToLeader without running the full analysis loop.
func (g *AnalysisGenerator) GenerateAnalysisForPooler(poolerIDStr string) (*ShardAnalysis, error) {
	pooler, ok := g.poolerStore.Get(poolerIDStr)
	if !ok {
		return nil, fmt.Errorf("pooler not found in store: %s", poolerIDStr)
	}
	if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
		return nil, fmt.Errorf("pooler or ID is nil: %s", poolerIDStr)
	}

	database := pooler.MultiPooler.GetShardKey().GetDatabase()
	tableGroup := pooler.MultiPooler.GetShardKey().GetTableGroup()
	shard := pooler.MultiPooler.GetShardKey().GetShard()

	poolers, ok := g.poolersByShard[database][tableGroup][shard]
	if !ok || len(poolers) == 0 {
		return nil, fmt.Errorf("shard not found for pooler: %s", poolerIDStr)
	}

	shardKey := &clustermetadatapb.ShardKey{Database: database, TableGroup: tableGroup, Shard: shard}
	return g.buildShardAnalysis(shardKey, poolers), nil
}

// generateAnalysisForPooler creates a ReplicationAnalysis for a single pooler.
// leaderObs is the cluster-wide authoritative LeaderObservation for the
// shard (may be nil if no observation exists yet) — used to mark this
// pooler as the leader without reading any PoolerType field.
func (g *AnalysisGenerator) generateAnalysisForPooler(
	pooler *multiorchdatapb.PoolerHealthState,
	shardKey *clustermetadatapb.ShardKey,
	leaderObs *clustermetadatapb.LeaderObservation,
) *PoolerAnalysis {
	analysis := &PoolerAnalysis{
		PoolerID:       pooler.MultiPooler.Id,
		ShardKey:       shardKey,
		IsLeader:       leaderObs.GetLeaderId() != nil && proto.Equal(leaderObs.GetLeaderId(), pooler.GetMultiPooler().GetId()),
		LastCheckValid: pooler.IsLastCheckValid,
		IsInitialized:  store.IsInitialized(pooler),
		CohortMembers:  pooler.GetStatus().GetCohortMembers(),
		AnalyzedAt:     time.Now(),
	}

	// Store consensus status.
	analysis.ConsensusStatus = pooler.GetConsensusStatus()
	analysis.AvailabilityStatus = pooler.GetAvailabilityStatus()

	// If this is a REPLICA, populate replica-specific fields
	if !analysis.IsLeader {
		if rs := pooler.GetStatus().GetReplicationStatus(); rs != nil {
			analysis.ReplicationStopped = rs.IsWalReplayPaused

			// Extract primary connection info
			if rs.PrimaryConnInfo != nil {
				analysis.PrimaryConnInfoHost = rs.PrimaryConnInfo.Host
			}

			if ts := rs.GetWalReceiverNotStreamingSince(); ts != nil {
				analysis.WalReceiverNotStreamingSince = ts.AsTime()
			}
		}
	}

	return analysis
}

// allReplicasConnectedToLeader checks if ALL postgres standbys in the shard are connected to the leader's postgres.
// A replica is considered connected if:
// 1. Its health check is valid (IsLastCheckValid)
// 2. It has PrimaryConnInfo configured pointing to the leader's postgres
// 3. It has received WAL (LastReceiveLsn is not empty)
//
// Returns true only if all replicas meet these criteria.
// Returns false if there are no replicas or any replica is disconnected.
func (g *AnalysisGenerator) allReplicasConnectedToLeader(
	primary *multiorchdatapb.PoolerHealthState,
	poolers map[string]*multiorchdatapb.PoolerHealthState,
) bool {
	primaryIDStr := topoclient.MultiPoolerIDString(primary.MultiPooler.Id)
	primaryHost := primary.MultiPooler.Hostname
	primaryPort := primary.MultiPooler.PortMap["postgres"]

	replicaCount := 0
	connectedCount := 0

	for poolerID, pooler := range poolers {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			continue
		}

		// Skip the leader itself
		if poolerID == primaryIDStr {
			continue
		}

		// Skip non-replicas. Note: a node whose postgres crashed into recovery mode
		// without going through the normal resign flow could report PoolerType_REPLICA
		// while still being the consensus leader. Such a node would be incorrectly
		// counted as a follower here, overstating connected-follower count.
		replicaType := pooler.GetStatus().GetPoolerType()
		if replicaType == clustermetadatapb.PoolerType_UNKNOWN {
			replicaType = pooler.MultiPooler.Type
		}
		if replicaType != clustermetadatapb.PoolerType_REPLICA {
			continue
		}

		replicaCount++

		// Check if replica is connected to the leader's postgres
		if !g.isFollowerConnectedToLeader(pooler, primaryHost, primaryPort) {
			continue
		}

		connectedCount++
	}

	// All replicas must be connected (and there must be at least one replica)
	return replicaCount > 0 && connectedCount == replicaCount
}

// isFollowerConnectedToLeader checks if a single replica is actively connected to the leader's postgres.
// It verifies both that the connection is configured correctly and that the WAL receiver is
// actively exchanging keepalives with the leader's postgres via pg_stat_wal_receiver.
func (g *AnalysisGenerator) isFollowerConnectedToLeader(
	replica *multiorchdatapb.PoolerHealthState,
	primaryHost string,
	primaryPort int32,
) bool {
	// Replica must be reachable
	if !replica.IsLastCheckValid {
		return false
	}

	// Replica must have replication status
	rs := replica.GetStatus().GetReplicationStatus()
	if rs == nil {
		return false
	}

	// Replica must have PrimaryConnInfo pointing to the primary
	connInfo := rs.PrimaryConnInfo
	if connInfo == nil || connInfo.Host == "" {
		return false
	}

	// Verify the replica is pointing to the correct primary. Note: if this is
	// not the case, there is a more fundamental problem (e.g., misconfiguration
	// or split-brain). This is not correctly indicated by a simple "false"
	// return value, but we still want to return false here to avoid falsely
	// triggering failover analyzers that rely on this method.
	if connInfo.Host != primaryHost || connInfo.Port != primaryPort {
		return false
	}

	// Replica must have received WAL (indicates connection was established)
	if rs.LastReceiveLsn == "" {
		return false
	}

	// WAL receiver must be in streaming state
	if rs.WalReceiverStatus != "streaming" {
		return false
	}

	// If last_msg_receive_time is available, verify the leader's postgres is still
	// sending keepalives. The threshold is
	// replicationHeartbeatStalenessMultiplier × wal_receiver_status_interval,
	// falling back to defaultReplicationHeartbeatStalenessThreshold when the
	// interval is unknown.
	//
	// If the last heartbeat is older than WAL receiver timeout, the connection
	// is effectively dead even if the replica hasn't noticed yet, so we check
	// that as well.
	if ts := rs.LastMsgReceiveTime; ts != nil {
		threshold := defaultReplicationHeartbeatStalenessThreshold
		delay := g.now().Sub(ts.AsTime())
		if d := rs.WalReceiverTimeout; d != nil && delay > d.AsDuration() {
			return false
		}
		if d := rs.WalReceiverStatusInterval; d != nil && d.AsDuration() > 0 {
			threshold = replicationHeartbeatStalenessMultiplier * d.AsDuration()
		}
		if delay > threshold {
			return false
		}
	}

	return true
}

// computeShardLevelFields populates shard-level aggregates on sa after all per-pooler
// analyses have been built. These fields describe the shard as a whole rather than
// any individual pooler, so they are computed once here rather than per-pooler.
func (g *AnalysisGenerator) computeShardLevelFields(sa *ShardAnalysis, poolers map[string]*multiorchdatapb.PoolerHealthState) {
	// Bootstrap durability policy lookup.
	if g.policyLookup != nil {
		sa.BootstrapDurabilityPolicy = g.policyLookup(sa.ShardKey.Database)
	}

	// Count reachable, initialized poolers for bootstrap analysis.
	for _, pa := range sa.Analyses {
		if pa.LastCheckValid && pa.IsInitialized {
			sa.NumInitialized++
		}
	}

	// Identify the cluster-wide consensus leader for the shard. The
	// observation is sourced from every pooler's etcd CurrentLeadership
	// and health-stream ConsensusStatus via consensus.MostAuthoritativeObservation —
	// strictly higher-rule-numbered observations win, so stale lower-rule
	// claims from demoted-but-not-yet-restarted poolers resolve silently.
	poolerSlice := make([]*multiorchdatapb.PoolerHealthState, 0, len(poolers))
	for _, p := range poolers {
		poolerSlice = append(poolerSlice, p)
	}
	leaderObs, leaderPooler := store.ShardLeader(poolerSlice)
	sa.LeaderObservation = leaderObs
	// Wire sa.Leader to the matching PoolerAnalysis (already populated
	// above by generateAnalysisForPooler with IsLeader set from leaderObs).
	if leaderObs.GetLeaderId() != nil {
		for _, pa := range sa.Analyses {
			if pa.IsLeader {
				sa.Leader = pa
				break
			}
		}
	}
	if leaderPooler != nil {
		sa.LeaderPoolerReachable = leaderPooler.IsLastCheckValid
		sa.LeaderPostgresReady = leaderPooler.GetStatus().GetPostgresReady()
		sa.LeaderPostgresRunning = leaderPooler.GetStatus().GetPostgresRunning()
		// LeaderHasResigned: AvailabilityStatus is populated from StatusResponse
		// on every health stream snapshot, so LeaderNeedsReplacement correctly
		// detects REQUESTING_DEMOTION signals without a separate RPC.
		sa.LeaderHasResigned = types.LeaderNeedsReplacement(leaderPooler)
		// LeaderReachable requires the leader to be serving as PRIMARY and
		// not have resigned. A resigned leader has voluntarily stepped down;
		// treating it as reachable would prevent LeaderIsDead detection even
		// when postgres is still running on the demoted node.
		sa.LeaderReachable = leaderPooler.IsLastCheckValid &&
			leaderPooler.GetStatus().GetPostgresReady() &&
			!sa.LeaderHasResigned &&
			leaderPooler.GetStatus().GetPoolerType() == clustermetadatapb.PoolerType_PRIMARY
		if leaderPooler.LastPostgresReadyTime != nil {
			sa.LeaderLastPostgresReadyTime = leaderPooler.LastPostgresReadyTime.AsTime()
		}

		// Populate the standby list from the leader (used by IsInStandbyList).
		if ps := leaderPooler.GetStatus().GetPrimaryStatus(); ps != nil && ps.SyncReplicationConfig != nil {
			sa.LeaderStandbyIDs = ps.SyncReplicationConfig.StandbyIds
		}

		// Detect pg_promote transition: multipooler explicitly signals promotion is running.
		if leaderPooler.GetStatus().GetPostgresStatus() == multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING {
			sa.PromotingPrimaryID = leaderPooler.MultiPooler.Id
		}
	}

	// HasInitializedReplica: any non-primary, reachable, initialized pooler.
	for _, pa := range sa.Analyses {
		if !pa.IsLeader && pa.LastCheckValid && pa.IsInitialized {
			sa.HasInitializedReplica = true
			break
		}
	}

	// Determine if all followers are still connected to the leader's postgres.
	// Use the cluster-authoritative leader pooler (which may be unreachable)
	// so we can detect the "pooler down but postgres still running" scenario
	// that ReplicasConnectedToLeader is designed to catch.
	if leaderPooler != nil {
		sa.ReplicasConnectedToLeader = g.allReplicasConnectedToLeader(leaderPooler, poolers)
	}
}
