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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

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
	// pooler, for staleness detection.
	LastSeen *timestamppb.Timestamp

	// ConsensusStatus is the pooler's consensus state, for divergence detection.
	ConsensusStatus *clustermetadatapb.ConsensusStatus

	// LastPostgresReadyTime is the last time PostgreSQL specifically responded as
	// healthy (pg_isready passed). Never cleared on failure; callers compare it
	// against the evaluation time to detect staleness.
	LastPostgresReadyTime *timestamppb.Timestamp

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
		LastSeen:                cloneProto(h.LastSeen),
		ConsensusStatus:         cloneProto(h.ConsensusStatus),
		LastPostgresReadyTime:   cloneProto(h.LastPostgresReadyTime),
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

func (h *PoolerHealthState) GetLastSeen() *timestamppb.Timestamp {
	if h == nil {
		return nil
	}
	return h.LastSeen
}

func (h *PoolerHealthState) GetConsensusStatus() *clustermetadatapb.ConsensusStatus {
	if h == nil {
		return nil
	}
	return h.ConsensusStatus
}

func (h *PoolerHealthState) GetLastPostgresReadyTime() *timestamppb.Timestamp {
	if h == nil {
		return nil
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
