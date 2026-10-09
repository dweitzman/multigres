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

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// leaderHasResigned reports whether the leader has voluntarily signalled it
// should be replaced — cohort-eligibility INELIGIBLE or a term-matched
// REQUESTING_DEMOTION — read from its self-reported AvailabilityStatus.
func leaderHasResigned(sa *ShardAnalysis) bool {
	return sa.Leader != nil && types.LeaderNeedsReplacement(sa.Leader.Health())
}

// leaderShutdownTombstoned reports whether the shard's leader has been observed in
// LIFECYCLE_SHUTDOWN. That lifecycle is written to topology at the END of a graceful
// shutdown (after the drain), so acting on it does not preempt the drain — and it is
// durable, so it still fires when the ephemeral REQUESTING_DEMOTION health broadcast is
// lost. A SHUTDOWN pooler is tombstoned and evicted from the live cache (absent from
// sa.Leader), so we match it by ID against the cache's tombstone set; the leaderID still
// comes from the shard rule, so we can act with no cached leader. STOPPING is
// deliberately NOT consulted: it is observability-only and precedes the drain.
func leaderShutdownTombstoned(sa *ShardAnalysis, leaderID *clustermetadatapb.ID) bool {
	if leaderID == nil {
		return false
	}
	_, ok := sa.TombstoneIDs[topoclient.ComponentIDString(leaderID)]
	return ok
}

// leaderObservedLive reports whether the orchestrator holds a recent, valid
// observation of the leader's pooler — the freshness-aware liveness basis for
// failover detection. It deliberately keys off observation age (sa.Now vs the
// leader's last snapshot, bounded by sa.Policy.LeaderLivenessFreshness) rather
// than whether a particular health stream is currently connected, so a brief
// stream interruption does not read as a dead leader while a genuinely stalled
// stream does.
func leaderObservedLive(sa *ShardAnalysis) bool {
	if sa.Leader == nil {
		return false
	}
	return observationFresh(sa.Leader, sa.Now, sa.Policy.LeaderLivenessFreshness)
}

// leaderPromoting reports whether the leader's last snapshot shows pg_promote()
// in progress (postgres in the PROMOTING state).
func leaderPromoting(sa *ShardAnalysis) bool {
	return sa.Leader != nil &&
		sa.Leader.Health().GetStatus().GetPostgresStatus() == multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING
}

// leaderInRecovery reports whether the leader's last snapshot shows its postgres
// genuinely in recovery as a STANDBY (pg_is_in_recovery() = true) — a node the
// consensus rule names as leader but whose postgres never left recovery and so
// cannot accept writes. Mirrors store.LeaderWritesProgressing's rule that recovery
// mode is what actually precludes writes.
//
// This is deliberately the STANDBY state specifically, not "anything other than
// PRIMARY": a standby answers pg_isready continuously, so it keeps postgres_ready
// (and LastPostgresReadyTime) fresh and would otherwise pass both the healthy
// fast-path and the anti-flap grace forever (see leaderFitnessCause). Transient
// non-primary states (STARTING/UNKNOWN during a restart or a wedged postgres) lose
// pg_isready, so the anti-flap timeout already fails them over — treating them as
// "in recovery" here would instead fail over every primary restart. PROMOTING is
// handled upstream by inPromotionGrace.
func leaderInRecovery(sa *ShardAnalysis) bool {
	return sa.Leader != nil &&
		sa.Leader.Health().GetStatus().GetPostgresStatus() == multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY
}

// leaderPostgresReady reports the leader's last-snapshot pg_isready result.
func leaderPostgresReady(sa *ShardAnalysis) bool {
	return sa.Leader != nil && sa.Leader.Health().GetStatus().GetPostgresReady()
}

// leaderPostgresRunning reports whether the leader's last snapshot shows its
// postgres process alive (may be true even when pg_isready fails, e.g. SIGSTOP).
func leaderPostgresRunning(sa *ShardAnalysis) bool {
	return sa.Leader != nil && sa.Leader.Health().GetStatus().GetPostgresRunning()
}

// leaderLastPostgresReadyTime returns when the leader's postgres last reported
// ready per its snapshots, or the zero time if never observed ready.
func leaderLastPostgresReadyTime(sa *ShardAnalysis) time.Time {
	if sa.Leader == nil {
		return time.Time{}
	}
	if ts := sa.Leader.Health().GetLastPostgresReadyTime(); ts != nil {
		return ts.AsTime()
	}
	return time.Time{}
}

// leaderServing reports whether the leader is a healthy, currently-serving
// primary suitable to drive a leader-led change (cohort reconcile, replica
// re-pointing): a recent observation (within the policy's leader-change
// freshness), postgres accepting connections, and not resigned. This is the Q3
// gate — not latency-sensitive, so requiring freshness merely defers a
// non-urgent change when our view of the leader is stale.
func leaderServing(sa *ShardAnalysis) bool {
	if sa.Leader == nil {
		return false
	}
	return observationFresh(sa.Leader, sa.Now, sa.Policy.LeaderChangeFreshness) &&
		leaderPostgresReady(sa) &&
		!leaderHasResigned(sa)
}
