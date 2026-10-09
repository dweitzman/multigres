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

// followerState is what one follower's fresh report says about the leader's rule.
type followerState int

const (
	// followerUnknown: no conclusion — no fresh report, its highest-known rule
	// doesn't name this leader, or it isn't pointed at this leader at all.
	followerUnknown followerState = iota
	// followerAdapting: it learned of this rule within ConnectReplicasToNewLeaderGrace,
	// so not streaming yet is expected.
	followerAdapting
	// followerStreaming: actively streaming from the leader.
	followerStreaming
	// followerRevoked: it accepted a revocation of the leader's rule.
	followerRevoked
	// followerDisconnected: pointed at the leader past the grace, yet not streaming.
	followerDisconnected
)

// classifyFollowers returns the cohort followers (the leader excluded) that have
// conclusively left the leader's rule: revoked it, or stopped streaming from it.
// Followers with no conclusive report are in neither set — silence is not evidence.
func (a *LeaderNeedsReplacementAnalyzer) classifyFollowers(sa *ShardAnalysis, cohort []*clustermetadatapb.ID, leaderID *clustermetadatapb.ID) (revoked, disconnected []*clustermetadatapb.ID) {
	if sa.Leader == nil {
		// No leader address to check followers against — no evidence either way.
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
			continue
		}
		pa, ok := byID[topoclient.ComponentIDString(member)]
		if !ok {
			continue
		}
		switch a.classifyFollower(sa, pa, leaderID, primaryHost, primaryPort) {
		case followerRevoked:
			revoked = append(revoked, member)
		case followerDisconnected:
			disconnected = append(disconnected, member)
		case followerUnknown, followerAdapting, followerStreaming:
		}
	}
	return revoked, disconnected
}

// classifyFollower answers "does this follower back the leader's rule?".
// Revocation is checked before streaming: a follower's own TermRevocation is
// authoritative bookkeeping, so a follower that reports itself revoked counts as
// revoked even if it appears to be streaming — that combination shouldn't happen
// (recruit should have stopped its WAL receiver), so it's logged as an anomaly.
//
// RecruitBlockedUntil is deliberately not consulted: a blocked recruit is still
// pointed at the leader and can report it unreachable — recruitability is a
// feasibility concern (recruitableCohort), not detection.
func (a *LeaderNeedsReplacementAnalyzer) classifyFollower(sa *ShardAnalysis, pa *store.Pooler, leaderID *clustermetadatapb.ID, primaryHost string, primaryPort int32) followerState {
	if !observationFresh(pa, sa.Now, sa.Policy.FollowerStreamFreshness) {
		return followerUnknown
	}
	streaming := followerStreamingFromLeader(sa, pa, primaryHost, primaryPort)
	if commonconsensus.IsSelfRevoked(pa.Health().GetConsensusStatus()) && revocationStrandsFollower(sa, pa) {
		if streaming {
			a.factory.Logger().Error("follower reports self-revoked yet still streaming WAL under the revoked rule",
				"pooler_id", topoclient.ComponentIDString(poolerID(pa)), "shard_key", sa.ShardKey.String())
		}
		return followerRevoked
	}
	if streaming {
		return followerStreaming
	}
	// Not streaming. Does it know it should be following THIS leader? Its highest-known
	// rule may come from its replication primary, not only its own WAL position.
	rule := commonconsensus.PossiblyUndecidedRule(
		commonconsensus.HighestKnownRule([]*clustermetadatapb.ConsensusStatus{pa.Health().GetConsensusStatus()}))
	if !commonconsensus.RuleNamesLeader(rule, leaderID) {
		return followerUnknown
	}
	if ruleWithinGrace(sa, rule) {
		return followerAdapting
	}
	if !replicaConfiguredForLeader(pa, primaryHost, primaryPort) {
		return followerUnknown // knows the leader, had time, but isn't pointed at it
	}
	return followerDisconnected
}

// ruleWithinGrace reports whether rule was created within
// ConnectReplicasToNewLeaderGrace: too recently for a cohort member's report
// not reflecting it yet to count as evidence. A rule with no creation time is
// treated as within the grace, so missing metadata never convicts.
func ruleWithinGrace(sa *ShardAnalysis, rule *clustermetadatapb.ShardRule) bool {
	created := rule.GetCreationTime()
	return created == nil || sa.Now.Sub(created.AsTime()) <= sa.Policy.ConnectReplicasToNewLeaderGrace
}

// TODO: followerStreamingFromLeader/classifyFollower should be named
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
