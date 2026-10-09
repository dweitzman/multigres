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
	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// ruleParticipation answers "does this cohort member currently back the
// shard's highest-known rule?" — one of four conclusions, shared by both
// classifyFollowerToLeader (followers) and leaderParticipation (the leader
// itself), since both are ultimately answering the same question about
// different roles with different evidence.
type ruleParticipation int

const (
	// participationUnknown: we can't conclude anything. No fresh observation, or
	// (for a follower) its highest-known rule doesn't name this leader, or (past
	// the grace) it isn't even configured to follow it.
	participationUnknown ruleParticipation = iota
	// participationAdapting: it learned of this rule only within the connect
	// grace, so not-yet-confirmed is inconclusive — give it time.
	participationAdapting
	// participationActive: conclusive evidence it backs this rule — a follower
	// actively streaming from the leader, or the leader's own report confirming
	// itself (commonconsensus.IsActiveLeader).
	participationActive
	// participationLapsed: conclusive evidence it does NOT back this rule
	// (revoked past it, or — for a follower — configured-and-waited yet not
	// streaming).
	participationLapsed
)

// classifyFollowerReachability sorts cohort followers (the leader is judged
// separately by leaderParticipation) by their participation in the leader's
// rule, collecting the two conclusive sets: `vouching` (active → proves the
// leader alive) and `cutOff` (lapsed → conclusively not backing it).
// Adapting/unknown followers land in neither — their silence is not evidence.
func (a *LeaderNeedsReplacementAnalyzer) classifyFollowerReachability(sa *ShardAnalysis, cohort []*clustermetadatapb.ID, leaderID *clustermetadatapb.ID) (vouching, cutOff []*clustermetadatapb.ID) {
	if sa.Leader == nil {
		// No leader identity to check followers against — no evidence either way.
		return nil, nil
	}
	primaryHost := sa.Leader.Health().GetMultipooler().GetHostname()
	primaryPort := sa.Leader.Health().GetMultipooler().GetPortMap()["postgres"]
	leaderKey := topoclient.ComponentIDString(leaderID)

	byID := make(map[topoclient.ComponentID]*store.Pooler, len(sa.Analyses))
	for _, pa := range sa.Analyses {
		if pa != nil {
			byID[topoclient.ComponentIDString(poolerID(pa))] = pa
		}
	}

	for _, member := range cohort {
		if topoclient.ComponentIDString(member) == leaderKey {
			continue // the leader's own participation is judged by leaderParticipation
		}
		pa, ok := byID[topoclient.ComponentIDString(member)]
		if !ok {
			continue
		}
		switch a.classifyFollowerToLeader(sa, pa, leaderID, primaryHost, primaryPort) {
		case participationActive:
			vouching = append(vouching, member)
		case participationLapsed:
			cutOff = append(cutOff, member)
		case participationAdapting, participationUnknown:
			// no conclusive evidence either way
		}
	}
	return vouching, cutOff
}

// classifyFollowerToLeader answers "does this follower back the leader's rule?".
// Revocation is checked before streaming: a follower's own TermRevocation is
// authoritative bookkeeping, so a follower that reports itself revoked is
// lapsed even if it appears to be streaming — that combination shouldn't
// happen (recruit should have stopped its WAL receiver), so it's logged as a
// suspicious anomaly rather than trusted as proof of life.
//
// RecruitBlockedUntil is deliberately not consulted: a blocked recruit is still
// pointed at the leader and can report it unreachable — recruitability is a
// feasibility concern (recruitableCohort), not detection.
func (a *LeaderNeedsReplacementAnalyzer) classifyFollowerToLeader(sa *ShardAnalysis, pa *store.Pooler, leaderID *clustermetadatapb.ID, primaryHost string, primaryPort int32) ruleParticipation {
	if !observationFresh(pa, sa.Now, sa.Policy.FollowerStreamFreshness) {
		return participationUnknown
	}
	streaming := followerStreamingFromLeader(sa, pa, primaryHost, primaryPort)
	if commonconsensus.IsSelfRevoked(pa.Health().GetConsensusStatus()) && revocationStrandsFollower(sa, pa) {
		if streaming {
			a.factory.Logger().Error("follower reports self-revoked yet still streaming WAL under the revoked rule",
				"pooler_id", topoclient.ComponentIDString(poolerID(pa)), "shard_key", sa.ShardKey.String())
		}
		return participationLapsed
	}
	if streaming {
		return participationActive
	}
	// Not streaming. Does it know it should be following THIS leader? Its highest-known
	// rule may come from its replication primary, not only its own WAL position.
	rule := commonconsensus.PossiblyUndecidedRule(
		commonconsensus.HighestKnownRule([]*clustermetadatapb.ConsensusStatus{pa.Health().GetConsensusStatus()}))
	if !commonconsensus.RuleNamesLeader(rule, leaderID) {
		return participationUnknown
	}
	if created := rule.GetCreationTime(); created == nil || sa.Now.Sub(created.AsTime()) <= sa.Policy.ConnectReplicasToNewLeaderGrace {
		return participationAdapting
	}
	if !replicaConfiguredForLeader(pa, primaryHost, primaryPort) {
		return participationUnknown // knows the leader, had time, but isn't pointed at it
	}
	return participationLapsed
}

// TODO: followerStreamingFromLeader/classifyFollowerToLeader should be named
// "replica" instead of "follower", since they also work for observers (non-cohort members).

// followerStreamingFromLeader reports whether a single follower is actively streaming
// from the leader's postgres: configured for this leader, has received WAL, the WAL
// receiver is in streaming state, and keepalives are fresh (within
// wal_receiver_status_interval × multiplier, falling back to the default threshold,
// and never older than wal_receiver_timeout).
func followerStreamingFromLeader(sa *ShardAnalysis, replica *store.Pooler, primaryHost string, primaryPort int32) bool {
	if !replicaConfiguredForLeader(replica, primaryHost, primaryPort) {
		return false
	}
	rs := replica.Health().GetStatus().GetReplicationStatus()
	if rs.LastReceiveLsn == "" || rs.WalReceiverStatus != "streaming" {
		return false
	}
	if ts := rs.LastMsgReceiveTime; ts != nil {
		threshold := defaultReplicationHeartbeatStalenessThreshold
		delay := sa.Now.Sub(ts.AsTime())
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

// replicaConfiguredForLeader reports whether the replica's primary_conninfo targets
// this leader's postgres (host:port) — the shared "this replica is trying to follow
// THIS leader" test. Not restricted to cohort followers — any replica, including a
// non-cohort observer, can be checked. A replica pointed at a different primary (or
// none) indicates a deeper problem (misconfig/split-brain) and is neither streaming
// from nor cut off from this leader.
func replicaConfiguredForLeader(replica *store.Pooler, primaryHost string, primaryPort int32) bool {
	connInfo := replica.Health().GetStatus().GetReplicationStatus().GetPrimaryConnInfo()
	return connInfo.GetHost() != "" && connInfo.GetHost() == primaryHost && connInfo.GetPort() == primaryPort
}

// revocationSufficient reports whether a set of cohort members leaving the leader is
// enough to revoke its term: the members NOT in that set can no longer independently
// satisfy the durability policy. Dual of CheckSufficientRecruitment's revocation
// check — "sufficient to revoke" means the complement cannot form a quorum, NOT that
// the set itself is a quorum (those coincide only for strict-majority policies).
func revocationSufficient(policy commonconsensus.DurabilityPolicy, cohort, cutOff []*clustermetadatapb.ID) bool {
	cutKeys := make(map[topoclient.ComponentID]struct{}, len(cutOff))
	for _, m := range cutOff {
		cutKeys[topoclient.ComponentIDString(m)] = struct{}{}
	}
	remaining := make([]*clustermetadatapb.ID, 0, len(cohort))
	for _, m := range cohort {
		if _, ok := cutKeys[topoclient.ComponentIDString(m)]; !ok {
			remaining = append(remaining, m)
		}
	}
	return policy.SatisfiedBy(remaining) != nil
}
