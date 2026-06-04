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
	"cmp"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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

	// LeaderObservation is the cluster-wide authoritative leader claim
	// derived from every pooler's etcd CurrentLeadership and health-stream
	// ConsensusStatus via consensus.MostAuthoritativeObservation. Nil
	// when no observation exists from any source yet. The leader's pooler
	// ID is LeaderObservation.GetLeaderId() — use that anywhere the old
	// HighestTermDiscoveredLeaderID was read.
	LeaderObservation *clustermetadatapb.LeaderObservation

	// Leader is the PoolerAnalysis for the cluster-authoritative leader,
	// or nil if the named pooler is not in this shard's known set (e.g.
	// observed via a peer's health stream but record not yet discovered).
	// All Leader* convenience booleans below are populated from this.
	Leader *PoolerAnalysis

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
	// LeaderUnreachableAnalyzer to avoid false positives when no postgres standby can observe the leader.
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

	// LeaderHasResigned is true when the topology leader has voluntarily
	// signalled it should be replaced — either COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE
	// (graceful shutdown) or LEADERSHIP_SIGNAL_REQUESTING_DEMOTION (Recruit's
	// primary-demotion path after a postgres crash). See
	// types.LeaderNeedsReplacement for the aggregation. LeaderResignedAnalyzer
	// keys off this to trigger immediate failover, separately from the
	// LeaderUnreachableAnalyzer reachability-based path.
	LeaderHasResigned bool

	// PromotingPrimaryID is the ID of the topology primary that is currently running
	// pg_promote() but has not yet transitioned to accepting connections. Nil when no
	// promotion is in progress.
	// Used by LeaderUnreachableAnalyzer to suppress spurious failover detection during the
	// brief window (~5–10s) when the newly promoted node's postgres is not yet ready.
	PromotingPrimaryID *clustermetadatapb.ID
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
		if !pa.IsLeader {
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

	IsLeader bool
	// Represents if the poolerID is reachable and it's returning a
	// valid status response
	LastCheckValid bool
	IsInitialized  bool // Whether this pooler is fully initialized and ready to join the cohort
	// CohortMembers are the strongly-typed IDs from the most recent
	// multigres.leadership_history record. Nil or empty both indicate no cohort
	// has been established. When IsInitialized=true, an empty list means the
	// 0-member bootstrap record is present — Phase 2 is needed.
	CohortMembers []*clustermetadatapb.ID
	AnalyzedAt    time.Time

	// Replica-specific fields
	ReplicationStopped  bool
	PrimaryConnInfoHost string

	// WalReceiverNotStreamingSince is the pooler-published timestamp of
	// when its WAL receiver last exited "streaming" state (and stayed
	// out). Zero when currently streaming, when no primary_conninfo is
	// configured, or when the pooler hasn't published the field yet.
	// Consumers apply a duration threshold to absorb bootstrap windows
	// and transient reconnects.
	WalReceiverNotStreamingSince time.Time

	// LastLsnAdvance is the orch-side timestamp of when this pooler's
	// CurrentPosition.Lsn was last observed to move forward. Zero
	// until the first advance observation. Used by
	// WritesNotProgressingAnalyzer to detect "replica claims to be
	// streaming but is not actually receiving WAL" conditions.
	LastLsnAdvance time.Time

	// ConsensusStatus from the pooler's most recent StatusResponse snapshot.
	// Used to derive the primary term via commonconsensus.PrimaryTerm(ConsensusStatus).
	ConsensusStatus *clustermetadatapb.ConsensusStatus

	// AvailabilityStatus carries the pooler's self-reported willingness signals
	// (cohort eligibility, leader-resignation request). May be nil for older
	// poolers that don't publish it.
	AvailabilityStatus *clustermetadatapb.AvailabilityStatus
}

// SelfLeaderObservation returns this pooler's own most-recent
// health-stream view of who the consensus leader is, derived from its
// ConsensusStatus.ReplicationPrimary.Rule. Returns nil if the pooler
// hasn't reported a rule yet.
//
// Distinct from ShardAnalysis.LeaderObservation, which is the
// cluster-wide max over all poolers' observations. Used by analyzers
// to detect "stale primaries" — poolers whose self-view still names
// themselves at an older rule than the cluster authoritative view.
func (pa *PoolerAnalysis) SelfLeaderObservation() *clustermetadatapb.LeaderObservation {
	rule := pa.ConsensusStatus.GetReplicationPrimary().GetRule()
	if rule == nil {
		return nil
	}
	return &clustermetadatapb.LeaderObservation{
		LeaderId:         rule.GetLeaderId(),
		LeaderRuleNumber: rule.GetRuleNumber(),
	}
}

// compareLeaderTimeline compares two leader PoolerAnalysis entries by the
// coordinator term of each pooler's current rule (via commonconsensus.LeaderTerm).
// Returns negative if a is less advanced than b, 0 if equal, positive if a is
// more advanced. LSN is intentionally excluded: for leaders, the coordinator
// term must be unique per promotion, so equal terms indicate a consensus bug
// rather than a resolvable tie.
func compareLeaderTimeline(a, b *PoolerAnalysis) int {
	return cmp.Compare(
		commonconsensus.LeaderTerm(a.ConsensusStatus),
		commonconsensus.LeaderTerm(b.ConsensusStatus),
	)
}
