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

package snapshot

import (
	"time"

	"google.golang.org/protobuf/proto"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// PoolerHealthState is multiorch's in-memory view of one pooler's health: what the
// pooler reported, plus when and over which channel. A published value is shared by
// readers and never edited; change one through its Clone.
//
// The getters are nil-safe, like generated protobuf getters, so a missing pooler or
// a field not yet reported reads as the zero value.
type PoolerHealthState struct {
	// Multipooler is the pooler's topology record: id, shard key, type, hostname
	// and port map.
	Multipooler *clustermetadatapb.Multipooler

	// LastSeen is when multiorch last received a successful health snapshot from the
	// pooler, for staleness detection. The zero time means never.
	LastSeen time.Time

	// ConsensusStatus is the pooler's consensus state, for divergence detection.
	ConsensusStatus *clustermetadatapb.ConsensusStatus

	// LastPostgresReadyTime is the last time PostgreSQL specifically responded as
	// healthy (pg_isready passed). Never cleared on failure; callers compare it
	// against the evaluation time to detect staleness. The zero time means never.
	LastPostgresReadyTime time.Time

	// AvailabilityStatus is from the most recent status response. Non-nil only for
	// nodes that are or have been primary. Used to detect voluntary demotions
	// (REQUESTING_DEMOTION) without a separate RPC.
	AvailabilityStatus *clustermetadatapb.AvailabilityStatus

	// StreamConnected is whether a health stream is currently active for this pooler.
	StreamConnected bool

	// Status is the full health status from the most recent successful Status RPC
	// or health stream snapshot. Nil until the first successful check.
	Status *multipoolermanagerdatapb.Status

	// StreamSnapshotsReceived counts health snapshots received on the current or
	// past streams.
	StreamSnapshotsReceived int64
}

// Clone returns a deep copy, so the result can be edited without affecting readers
// of the original.
func (h *PoolerHealthState) Clone() *PoolerHealthState {
	if h == nil {
		return nil
	}
	return &PoolerHealthState{
		Multipooler:             cloneProto(h.Multipooler),
		LastSeen:                h.LastSeen,
		ConsensusStatus:         cloneProto(h.ConsensusStatus),
		LastPostgresReadyTime:   h.LastPostgresReadyTime,
		AvailabilityStatus:      cloneProto(h.AvailabilityStatus),
		StreamConnected:         h.StreamConnected,
		Status:                  cloneProto(h.Status),
		StreamSnapshotsReceived: h.StreamSnapshotsReceived,
	}
}

// cloneProto deep-copies m; a nil message stays nil.
func cloneProto[M proto.Message](m M) M {
	return proto.Clone(m).(M)
}

func (h *PoolerHealthState) GetMultipooler() *clustermetadatapb.Multipooler {
	if h == nil {
		return nil
	}
	return h.Multipooler
}

func (h *PoolerHealthState) GetLastSeen() time.Time {
	if h == nil {
		return time.Time{}
	}
	return h.LastSeen
}

func (h *PoolerHealthState) GetConsensusStatus() *clustermetadatapb.ConsensusStatus {
	if h == nil {
		return nil
	}
	return h.ConsensusStatus
}

func (h *PoolerHealthState) GetLastPostgresReadyTime() time.Time {
	if h == nil {
		return time.Time{}
	}
	return h.LastPostgresReadyTime
}

func (h *PoolerHealthState) GetAvailabilityStatus() *clustermetadatapb.AvailabilityStatus {
	if h == nil {
		return nil
	}
	return h.AvailabilityStatus
}

func (h *PoolerHealthState) GetStreamConnected() bool {
	if h == nil {
		return false
	}
	return h.StreamConnected
}

func (h *PoolerHealthState) GetStatus() *multipoolermanagerdatapb.Status {
	if h == nil {
		return nil
	}
	return h.Status
}

func (h *PoolerHealthState) GetStreamSnapshotsReceived() int64 {
	if h == nil {
		return 0
	}
	return h.StreamSnapshotsReceived
}

// ID returns the pooler's ID, or nil if the record has none.
func (h *PoolerHealthState) ID() *clustermetadatapb.ID {
	return h.GetMultipooler().GetId()
}

// Endpoint returns the pooler's postgres host and port; host is "" when it has not
// reported a hostname.
func (h *PoolerHealthState) Endpoint() (host string, port int32) {
	return h.GetMultipooler().GetHostname(), h.GetMultipooler().GetPortMap()["postgres"]
}

// IntendedPrimary returns the primary the pooler reports it was told to use, or nil
// if it has none (an unset or placeholder entry reads as nil).
func (h *PoolerHealthState) IntendedPrimary() *clustermetadatapb.IntendedPrimary {
	return commonconsensus.IntendedPrimaryOrNil(h.GetConsensusStatus())
}

// AsIntendedPrimary describes this pooler as the primary a follower should use under
// rule: the rule's position, this pooler's own topology address and its own
// rewind_ready. Call it on the leader, never on a follower; the result points at the
// receiver. rewind_ready is the leader's self-report with no global equivalent, so it
// is relayed rather than derived: a follower that needs a pg_rewind defers it until
// the leader has checkpointed onto its current timeline.
func (h *PoolerHealthState) AsIntendedPrimary(rule commonconsensus.RuleTransition) *clustermetadatapb.IntendedPrimary {
	return &clustermetadatapb.IntendedPrimary{
		Position:    rule.Position(),
		Primary:     topoclient.PoolerAddressFor(h.GetMultipooler()),
		RewindReady: h.IntendedPrimary().GetRewindReady(),
	}
}

// ObservedWithin reports whether the pooler's most recent successful health snapshot
// is within maxAge of now on the orchestrator clock. A pooler never observed is not
// within any age: with no observation we have no evidence it is alive. LastSeen
// advances only on a successful snapshot, never on a disconnect, so this alone
// captures "recently, successfully observed"; a brief stream interruption does not
// change it until the observation genuinely ages out.
func (h *PoolerHealthState) ObservedWithin(now time.Time, maxAge time.Duration) bool {
	age, ok := ObservationAge(h, now)
	return ok && age <= maxAge
}

// PostgresReadyWithin reports whether the pooler's postgres last reported ready within
// maxAge of now. Never having reported ready is not within any age.
func (h *PoolerHealthState) PostgresReadyWithin(now time.Time, maxAge time.Duration) bool {
	ready := h.GetLastPostgresReadyTime()
	return !ready.IsZero() && now.Sub(ready) <= maxAge
}

// WalReplayNotPaused reports whether the standby's WAL replay is active. A pooler
// with no replication status returns false; callers that distinguish an unavailable
// observation from a negative one must check the status for nil first.
func (h *PoolerHealthState) WalReplayNotPaused() bool {
	rs := h.GetStatus().GetReplicationStatus()
	if rs == nil {
		return false
	}
	return !rs.GetIsWalReplayPaused()
}

// PrimaryConnInfoHost returns the standby's configured primary host, or "" if
// replication is not configured.
func (h *PoolerHealthState) PrimaryConnInfoHost() string {
	return h.GetStatus().GetReplicationStatus().GetPrimaryConnInfo().GetHost()
}

// WalReceiverActive reports whether the standby's WAL receiver is actively streaming
// or waiting. "waiting" is healthy: the receiver is connected but the primary has no
// new WAL to send. Any other status (including "") means the receiver is not running;
// this covers timeline divergence where the receiver connects, gets FATAL, and exits,
// leaving primary_conninfo on disk.
func (h *PoolerHealthState) WalReceiverActive() bool {
	rs := h.GetStatus().GetReplicationStatus()
	if rs == nil {
		return false
	}
	return rs.GetWalReceiverStatus() == "streaming" || rs.GetWalReceiverStatus() == "waiting"
}

// WalReceiverStreaming is a stricter WalReceiverActive: it also rejects the brief
// window after a receiver reconnect where postgres reports "streaming" before any WAL
// has arrived (LastReceiveLsn still empty), the same FATAL-retry flicker FixReplication
// guards against. It is the signal that a standby has bridged any initial archive
// catch-up and is genuinely pulling WAL from the leader, which is what cohort
// admission requires (a cohort member must advance only by streaming, never the
// archive). "waiting" stays healthy: the receiver is connected and current, the
// primary just has nothing new to send.
func (h *PoolerHealthState) WalReceiverStreaming() bool {
	rs := h.GetStatus().GetReplicationStatus()
	if rs == nil {
		return false
	}
	switch rs.GetWalReceiverStatus() {
	case "waiting":
		return true
	case "streaming":
		return rs.GetLastReceiveLsn() != ""
	default:
		return false
	}
}
