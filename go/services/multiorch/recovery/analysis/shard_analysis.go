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
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// ShardAnalysis groups all per-pooler analyses for a single shard.
// It is the input type for the Analyzer interface.
type ShardAnalysis struct {
	ShardKey *clustermetadatapb.ShardKey
	Analyses []*PoolerAnalysis

	// NumInitialized is the count of reachable, initialized poolers in this shard.
	// Pre-computed by the generator for use in analyzers.
	NumInitialized int

	// BootstrapDurabilityPolicy is the durability policy configured for this shard's database.
	// May be nil if not yet configured or not available.
	BootstrapDurabilityPolicy *clustermetadatapb.DurabilityPolicy

	// Shard-level aggregates computed once by the generator.

	// HighestTermReachableLeader is the shard's single leader — the highest-term
	// consensus leader — exposed as a PoolerAnalysis only when its pooler is
	// reachable. Nil when no leader is known or the leader is currently unreachable
	// (LeaderIsDead handles the unreachable case). Analyzers that re-point replicas
	// use this as the leader whose rule should be delivered.
	HighestTermReachableLeader *PoolerAnalysis

	// HighestTermDiscoveredLeaderID is the pooler ID of the highest-term leader known to exist
	// in this shard's topology, regardless of whether it is currently reachable.
	// Nil if no leader has been recorded in topology yet.
	HighestTermDiscoveredLeaderID *clustermetadatapb.ID

	// LeaderReachable is true if the topology leader's pooler is reachable AND
	// its Postgres is running. False when TopologyLeaderID is nil.
	LeaderReachable bool

	// LeaderPoolerReachable is true if the topology leader's pooler health check
	// succeeded, independently of whether Postgres is running.
	// False when TopologyLeaderID is nil.
	LeaderPoolerReachable bool

	// LeaderStandbyIDs is the synchronous_standby_names list from the topology leader.
	// Nil when TopologyLeaderID is nil or the leader has no sync replication config.
	// Use IsInStandbyList to check membership.
	LeaderStandbyIDs []*clustermetadatapb.ID

	// HasInitializedReplica is true if at least one non-leader, reachable, initialized pooler exists
	// in the shard. This is a postgres-layer check (is there a standby that has joined the cluster?),
	// not a consensus-layer check — it does not require the pooler to be a cohort member. Used by
	// LeaderIsDeadAnalyzer to avoid false positives when no postgres standby can observe the leader.
	HasInitializedReplica bool

	// ReplicasConnectedToLeader is true only if ALL postgres standbys in the shard are still
	// connected to the leader's Postgres via WAL streaming (pg_stat_wal_receiver). Used to avoid
	// failover when only the leader pooler process is down but Postgres is still running.
	ReplicasConnectedToLeader bool

	// LeaderPostgresReady is true if the topology leader's Postgres is accepting connections
	// (pg_isready succeeds). Distinct from LeaderReachable: the pooler may be reachable
	// but Postgres may not yet be ready (e.g. still starting up).
	LeaderPostgresReady bool

	// LeaderPostgresRunning is true if the topology leader's Postgres process exists,
	// even if it is not accepting connections. False when the process is dead (SIGKILL).
	LeaderPostgresRunning bool

	// LeaderLastPostgresReadyTime is the last time the topology leader's Postgres
	// responded healthy (IsPostgresReady was true). Zero if never seen ready.
	// Used to time-bound failover suppression when followers are still connected.
	LeaderLastPostgresReadyTime time.Time

	// LeaderHasResigned is true when the topology leader has voluntarily requested
	// replacement via the REQUESTING_DEMOTION signal (set during Recruit's
	// primary-demotion path or graceful shutdown of a leader). LeaderResignedAnalyzer
	// keys off this to trigger immediate failover, separately from the LeaderIsDead
	// reachability-based path.
	LeaderHasResigned bool

	// PromotingPrimaryID is the ID of the topology primary that is currently running
	// pg_promote() but has not yet transitioned to accepting connections. Nil when no
	// promotion is in progress.
	// Used by LeaderIsDeadAnalyzer to suppress spurious failover detection during the
	// brief window (~5–10s) when the newly promoted node's postgres is not yet ready.
	PromotingPrimaryID *clustermetadatapb.ID
}

// fitToLead reports whether a pooler is currently fit to serve as the shard
// leader: it is the consensus leader, has a recent successful health check, its
// postgres is ready and actually running as a primary (not a demoted standby),
// and it has not signaled that it needs replacement. This is the single predicate
// behind "should we replace the leader?" — when the discovered leader is not
// fitToLead, failover detection takes over.
func fitToLead(pa *PoolerAnalysis) bool {
	if pa == nil {
		return false
	}
	return commonconsensus.IsLeader(pa.ConsensusStatus) &&
		pa.LastCheckValid &&
		pa.PostgresReady &&
		pa.RunningAsPrimary &&
		!types.LeaderResignSignaled(pa.AvailabilityStatus, pa.ConsensusStatus)
}

// IsInStandbyList reports whether the given pooler ID appears in the leader's
// synchronous standby list. Returns false when no standby list is available.
func (sa *ShardAnalysis) IsInStandbyList(id *clustermetadatapb.ID) bool {
	for _, standbyID := range sa.LeaderStandbyIDs {
		if standbyID.Cell == id.Cell && standbyID.Name == id.Name {
			return true
		}
	}
	return false
}

// Replicas returns the PoolerAnalysis entries for all follower poolers.
func (sa *ShardAnalysis) Replicas() []*PoolerAnalysis {
	var replicas []*PoolerAnalysis
	for _, pa := range sa.Analyses {
		if !commonconsensus.IsLeader(pa.ConsensusStatus) {
			replicas = append(replicas, pa)
		}
	}
	return replicas
}

// PoolerAnalysis represents the analyzed state of a single pooler
// and its replication topology. This is the in-memory equivalent of
// VTOrc's replication_analysis table.
type PoolerAnalysis struct {
	// Identity
	PoolerID *clustermetadatapb.ID
	ShardKey *clustermetadatapb.ShardKey

	// Represents if the poolerID is reachable and it's returning a
	// valid status response
	LastCheckValid   bool
	IsInitialized    bool // Whether this pooler is fully initialized and ready to join the cohort
	HasDataDirectory bool // Whether this pooler has a PostgreSQL data directory (PG_VERSION exists)
	AnalyzedAt       time.Time

	// Raw postgres health signals from the most recent snapshot. PostgresReady is
	// whether postgres accepts connections (pg_isready). RunningAsPrimary is the
	// recovery-mode signal (pg_is_in_recovery() == false) — a node demoted to a
	// standby reports false here even while consensus still names it leader. These
	// are postgres-layer facts, not the topology PoolerType routing label.
	PostgresReady    bool
	RunningAsPrimary bool

	// ReplicationStatus is the standby's raw replication status from its most
	// recent snapshot (nil for the leader, or a standby with no WAL receiver).
	// Analyzers read it directly to judge replication fitness rather than relying
	// on pre-computed flags.
	ReplicationStatus *multipoolermanagerdatapb.StandbyReplicationStatus

	// ConsensusStatus from the pooler's most recent StatusResponse snapshot.
	// Leadership and term are derived from it on demand (commonconsensus.IsLeader /
	// LeaderTerm) rather than cached as separate fields.
	ConsensusStatus *clustermetadatapb.ConsensusStatus

	// AvailabilityStatus carries the pooler's self-reported willingness signals
	// (cohort eligibility, leader-resignation request). May be nil for older
	// poolers that don't publish it.
	AvailabilityStatus *clustermetadatapb.AvailabilityStatus
}

// analyzeAllPoolers runs fn against each pooler analysis in sa, collecting all problems.
// Both the shard analysis and the per-pooler analysis are passed so callbacks can
// access shard-level fields (e.g. LeaderReachable) alongside pooler-specific state.
// Errors are accumulated — the first error encountered is returned alongside any problems collected.
func analyzeAllPoolers(sa *ShardAnalysis, fn func(*ShardAnalysis, *PoolerAnalysis) (*types.Problem, error)) ([]types.Problem, error) {
	var problems []types.Problem
	var firstErr error
	for _, poolerAnalysis := range sa.Analyses {
		p, err := fn(sa, poolerAnalysis)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if p != nil {
			problems = append(problems, *p)
		}
	}
	return problems, firstErr
}
