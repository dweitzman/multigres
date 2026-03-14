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

package consensus

// NodeHealth is the coordinator's observed health state for a single pooler.
type NodeHealth struct {
	PostgresStatus PostgresStatus
	LastHeardTick  int64 // 0 = never heard from
	// ShutdownIntent is true when the node has signalled that it intends to
	// stop soon (SIGTERM received). Treated as unhealthy for HA decisions so
	// the coordinator initiates a proactive term change while the node is still
	// cooperative, rather than waiting for PostgresStopped or a health timeout.
	ShutdownIntent bool
}

// ShardStatus is a snapshot of the coordinator's complete view of the cluster —
// both the consensus-layer term state and the per-node operational health.
// It is the input to all high-availability policy decisions, separating the
// question of "should we act?" (HA policy) from "how do we act?" (consensus
// mechanics).
type ShardStatus struct {
	Tick int64

	// HighestQuorumTerm is the highest-Seq Term for which the coordinator has
	// confirmed a write quorum: enough cohort members have reported applying
	// this version (or a later one) to satisfy the term's DurabilityPolicy.
	// This is the last known-good state of the cluster.
	// Nil if no version has confirmed quorum.
	HighestQuorumTerm *Term

	// HighestSeenTerm is the highest-Seq Term reported by any known pooler,
	// regardless of quorum. Nil if no term has been seen.
	// If HighestSeenTerm.Seq > HighestQuorumTerm.Seq, a partial leader-driven
	// term change exists and must be propagated before establishing a new primary.
	HighestSeenTerm *Term

	// NodeHealth maps each known pooler's ID to its observed operational health.
	NodeHealth map[NodeID]NodeHealth
}

// HighAvailabilityStrategy advises the coordinator on when to take action to preserve
// cluster availability. Implementations express HA policy independently of the
// consensus mechanics: they receive a ShardStatus snapshot and return a
// recommendation with no knowledge of how the coordinator will act on it.
type HighAvailabilityStrategy interface {
	// NeedsLeaderFailover returns true when the coordinator should initiate a
	// coordinator-led term change to replace the current primary.
	NeedsLeaderFailover(status ShardStatus) bool

	// CanAttemptFailover returns true when the coordinator has enough healthy
	// nodes to satisfy the effective post-failover durability policy. If
	// HighestSeenTerm.Seq > HighestQuorumTerm.Seq, a partial leader-driven
	// change exists and the coordinator will be obligated to propagate it;
	// HighestSeenTerm's policy governs. Otherwise HighestQuorumTerm's policy
	// governs. The policy is checked against only the cohort members that are
	// currently healthy and reachable.
	//
	// This is checked before starting a new recruitment to avoid premature
	// quorum revocation when there is no realistic path to completion.
	CanAttemptFailover(status ShardStatus) bool
}

// DefaultHighAvailability returns the standard HA policy: initiate a failover
// when the primary identified by the best-known term is unreachable (postgres
// stopped or status updates stale beyond HealthTimeoutTicks).
func DefaultHighAvailability() HighAvailabilityStrategy {
	return defaultHA{}
}

type defaultHA struct{}

func (defaultHA) NeedsLeaderFailover(status ShardStatus) bool {
	var primaryID NodeID
	if status.HighestQuorumTerm != nil {
		primaryID = status.HighestQuorumTerm.Primary
	} else if status.HighestSeenTerm != nil {
		primaryID = status.HighestSeenTerm.Primary
	}
	if primaryID == "" {
		return false
	}
	health, ok := status.NodeHealth[primaryID]
	if !ok {
		return true // primary not known to coordinator; treat as unreachable
	}
	return !isNodeHealthy(health, status.Tick)
}

func (defaultHA) CanAttemptFailover(status ShardStatus) bool {
	// Use the highest-seq term the coordinator has seen. If HighestSeenTerm is
	// ahead of HighestQuorumTerm, a partial leader-driven change exists and the
	// coordinator will be obligated to propagate it; that term's policy governs.
	// Otherwise fall back to HighestQuorumTerm.
	effectiveTerm := status.HighestSeenTerm
	if effectiveTerm == nil || (status.HighestQuorumTerm != nil && status.HighestSeenTerm.Seq <= status.HighestQuorumTerm.Seq) {
		effectiveTerm = status.HighestQuorumTerm
	}
	if effectiveTerm == nil {
		return false // no known cluster state; cannot safely attempt failover
	}
	if effectiveTerm.Policy == nil {
		return true // nil policy: no durability requirement
	}
	var healthy []CohortMember
	for _, m := range effectiveTerm.Members {
		if h, ok := status.NodeHealth[m.ID]; ok && isNodeHealthy(h, status.Tick) {
			healthy = append(healthy, m)
		}
	}
	return effectiveTerm.Policy.IsAchievable(healthy)
}

// NeedsLeaderFailover is a package-level convenience function that applies the
// default HA policy to the given ShardStatus. It is equivalent to
// DefaultHighAvailability().NeedsLeaderFailover(status).
func NeedsLeaderFailover(status ShardStatus) bool {
	return defaultHA{}.NeedsLeaderFailover(status)
}

// CanAttemptFailover is a package-level convenience function that applies the
// default HA policy to the given ShardStatus. It is equivalent to
// DefaultHighAvailability().CanAttemptFailover(status).
func CanAttemptFailover(status ShardStatus) bool {
	return defaultHA{}.CanAttemptFailover(status)
}

// isNodeHealthy returns true when the given node is considered reachable and
// willing to serve writes. A node is unhealthy when its postgres is stopped,
// when it has signalled shutdown intent, or when it has not sent a status
// update within HealthTimeoutTicks.
func isNodeHealthy(health NodeHealth, tick int64) bool {
	if health.PostgresStatus == PostgresStopped {
		return false
	}
	if health.ShutdownIntent {
		return false
	}
	if health.LastHeardTick > 0 && tick-health.LastHeardTick > HealthTimeoutTicks {
		return false
	}
	return true
}
