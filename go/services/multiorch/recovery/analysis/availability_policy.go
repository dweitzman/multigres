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

	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// AvailabilityPolicy is configuration that influences the orchestrator's
// decisions about when to take action and what choices to make — for example,
// how stale an observation may be before it stops counting as evidence, how
// aggressively to act on weak versus strong signals, and (in time) backoff and
// selection preferences. It exists so these knobs live in one named, injectable
// place rather than as constants scattered through the analyzers, and so
// decisions that warrant different policies can carry different values.
//
// Today it holds the observation-freshness thresholds that decide when a
// pooler's last health snapshot is too old to be trusted as a live signal. The
// set of fields will grow as more decisions become policy-driven.
//
// TODO: the failover timings that govern leader appointment are still scattered
// outside this struct: the recruitment backoff schedule and its reset window
// (ha.DefaultBackoffSchedule, ha.DefaultBackoffResetDuration), the coordinator's
// recent-acceptance back-off window (consensus.checkRecentAcceptance), and the
// AppointLeader action's timeouts. Move AvailabilityPolicy to a leaf package that
// consensus can import (analysis already imports consensus), then thread it
// through the engine, coordinator and action factory so every such time is
// defined here. Also drop the now-unused --leader-postgres-response-threshold
// flag, and define QuorumCommitStaleAfter and the freshness defaults here
// instead of copying them from consensus and store.
//
// This is an in-process Go struct, deliberately scoped to multiorch for now.
// The longer-term direction (see the orch health/failover principles doc, P6)
// is to source it from a proto distributed to poolers, with per-shard
// overrides; when that lands, callers read the same struct, populated from the
// proto instead of from DefaultAvailabilityPolicy.
type AvailabilityPolicy struct {
	// LeaderLivenessFreshness bounds how stale the leader's most recent health
	// snapshot may be before it stops counting as a live observation. This is
	// the signal that TRIGGERS failover, so it wants confidence before firing:
	// a snapshot older than this means "we no longer have current evidence the
	// leader is alive."
	LeaderLivenessFreshness time.Duration

	// FollowerStreamFreshness bounds the same for a follower's snapshot when its
	// "still streaming from the leader" report is used to SUPPRESS failover.
	// Directionality is inverted from the trigger: a stale follower snapshot
	// must NOT keep suppressing failover, so an age beyond this drops the
	// follower from the connected set.
	FollowerStreamFreshness time.Duration

	// LeaderChangeFreshness bounds how stale the leader's snapshot may be before
	// it stops counting as a serving primary suitable to drive a leader-led
	// change (cohort reconcile, replica re-pointing). These decisions are not
	// latency-sensitive (Q3): being conservative here merely defers a non-urgent
	// change, so it is a separate, independently tunable knob from the
	// failover-detection thresholds above.
	LeaderChangeFreshness time.Duration

	// ConnectReplicasToNewLeaderGrace bounds how long after a rule is created a
	// cohort member's report may still not reflect it without that counting as
	// evidence: followers need time to repoint and start streaming, and the
	// leader's own report can trail its followers' (Promote and SetPrimary are
	// sent concurrently).
	ConnectReplicasToNewLeaderGrace time.Duration

	// ObservationFreshness bounds how stale a pooler's health snapshot may be
	// before it stops counting as a trustworthy fact at all. It's the default
	// tolerance for decisions that aren't specifically about leader liveness
	// (LeaderLivenessFreshness) or follower streaming evidence
	// (FollowerStreamFreshness) — e.g. "is this replica initialized."
	ObservationFreshness time.Duration

	// QuorumCommitStaleAfter bounds how old the freshest cohort-observed
	// quorum_commit_ts may be before writes count as not provably progressing
	// (LeaderProgressUnproven).
	// Kept generous and longer than LeaderLivenessFreshness: this signal is
	// multi-hop (heartbeat interval, one-tick defer, reader poll, then
	// health-snapshot propagation), so delays stack even when nothing is
	// wrong -- a false positive here drives a real failover against a
	// healthy leader.
	QuorumCommitStaleAfter time.Duration

	// PostgresUnreadyPatience bounds how long commits may be stalled before a
	// leader whose postgres is running but not ready (starting, wedged) is
	// convicted. It must exceed a healthy shard's ordinary watermark age, which
	// in a 3-pooler cluster measured up to about 5.6s (the writer lags one write
	// behind, plus snapshot propagation steps of about 5s), and stay below
	// QuorumCommitStaleAfter, which it exists to beat.
	PostgresUnreadyPatience time.Duration

	// FollowerDisconnectPatience is the same for a durability-sufficient set of
	// followers that are pointed at the leader but not streaming from it. Longer
	// than PostgresUnreadyPatience on purpose: postgres can be slow to connect,
	// and a failover that fires while replication is still being set up
	// interrupts it before it begins, which can loop.
	FollowerDisconnectPatience time.Duration

	// WalReceiverStalenessMultiplier is applied to a follower's
	// wal_receiver_status_interval to decide whether its WAL receiver has gone
	// silent: it sends a status message every interval and the primary echoes a
	// keepalive, so this many missed intervals means the primary stopped
	// answering, well before wal_receiver_timeout would disconnect it.
	WalReceiverStalenessMultiplier int

	// WalReceiverStalenessFallback is that threshold when the follower's
	// wal_receiver_status_interval is not in its health report. It equals
	// WalReceiverStalenessMultiplier times the default interval (10s).
	WalReceiverStalenessFallback time.Duration

	// MaxPromotionTime bounds how long after a promotion is proposed Multiorch
	// waits before superseding it with a new recruit, while it still shows
	// progress (mid pg_promote(), or followers receiving its WAL). The stalled
	// promotion is reported throughout; only the failover waits. It bounds when a
	// replacement may start, not how long a promotion may run. Generous on
	// purpose: a candidate with a large WAL backlog can need minutes before its
	// first quorum commit, and failing it over hands the same backlog to the next
	// candidate.
	MaxPromotionTime time.Duration
}

// DefaultAvailabilityPolicy returns the built-in policy used when no operator
// configuration is supplied. The freshness thresholds are a small multiple of
// the default health-snapshot interval, so a brief stream interruption does not
// flip a pooler's liveness while a genuinely stalled stream is caught well
// before the staleness watchdog's much longer window.
func DefaultAvailabilityPolicy() AvailabilityPolicy {
	return AvailabilityPolicy{
		LeaderLivenessFreshness:         15 * time.Second,
		FollowerStreamFreshness:         15 * time.Second,
		LeaderChangeFreshness:           store.DefaultLeaderWriteFreshness,
		ConnectReplicasToNewLeaderGrace: 10 * time.Second,
		ObservationFreshness:            store.DefaultObservationFreshness,
		QuorumCommitStaleAfter:          consensus.DefaultQuorumCommitStaleAfter,
		PostgresUnreadyPatience:         10 * time.Second,
		FollowerDisconnectPatience:      15 * time.Second,
		WalReceiverStalenessMultiplier:  3,
		WalReceiverStalenessFallback:    30 * time.Second,
		MaxPromotionTime:                5 * time.Minute,
	}
}
