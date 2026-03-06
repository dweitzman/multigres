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

// Request is implemented by all outgoing messages emitted by consensus nodes.
// The RequestHandler converts Requests into Indicators delivered to target nodes.
type Request interface {
	consensusRequest()
}

// --- Requests from CoordNode ---

// WritePolicyRequest asks the RequestHandler to deliver a DurabilityPolicyRecord
// write to the target pooler (which must be the current primary). The primary
// validates the compare-and-swap: Record.PreviousID must match its current
// Policy.ID. If it does not match, the write is rejected and the primary returns
// its current ID so the coord can retry with the correct PreviousID.
type WritePolicyRequest struct {
	TargetPooler NodeID
	FromCoord    NodeID
	Record       DurabilityPolicyRecord // Record.PreviousID is the CAS key
}

func (WritePolicyRequest) consensusRequest() {}

// --- Requests from PoolerNode ---

// PolicyRecordApplyRequest is emitted by the primary PoolerNode when it needs
// the local postgres driver to apply a DurabilityPolicyRecord change. The
// driver is responsible for the full apply sequence: updating
// synchronous_standby_names and then committing the SQL transaction. If the
// transaction fails, the driver must roll back the replication settings change
// and deliver a failure indicator.
//
// In production, the local driver goroutine handles this and delivers a
// PolicyRecordAppliedIndicator back to the PoolerNode on success.
// In simulation, the simPooler wrapper intercepts this request (before it
// reaches the RequestHandler), simulates the apply, and queues the result
// indicator for the next tick.
type PolicyRecordApplyRequest struct {
	Record DurabilityPolicyRecord
}

func (PolicyRecordApplyRequest) consensusRequest() {}

// WritePolicyResponseRequest is emitted by a PoolerNode in response to a
// WritePolicyIndicator, after the local postgres write has either succeeded or
// failed. Accepted is true if the record was durably committed. When false,
// CurrentID carries the primary's actual current policy version so the coord
// can correct its PreviousID on retry without a separate status round-trip.
type WritePolicyResponseRequest struct {
	ToCoord   NodeID
	Accepted  bool
	CurrentID PolicyID // primary's current policy version when Accepted=false
}

func (WritePolicyResponseRequest) consensusRequest() {}

// PoolerStatusUpdateRequest is emitted by a PoolerNode whenever its committed
// state changes (policy write committed, WAL record applied, postgres stopped,
// or after a crash-restart). The RequestHandler delivers it to all known
// CoordNodes as a PoolerStatusIndicator so the coord can track cluster state.
type PoolerStatusUpdateRequest struct {
	State          PoolerPersistentState
	PostgresStatus PostgresStatus
}

func (PoolerStatusUpdateRequest) consensusRequest() {}

// --- Requests from driver/simulation nodes ---

// TerminateRequest is emitted by a driver node to signal graceful shutdown of
// a specific pooler. The RequestHandler converts it to a TerminateIndicator
// delivered to the target. In production this corresponds to a SIGTERM signal.
type TerminateRequest struct {
	Target NodeID
}

func (TerminateRequest) consensusRequest() {}

// PoolerMembershipRequest is emitted by the discovery node when the set of
// registered poolers changes. The RequestHandler converts it into
// PoolerDiscoveredIndicator and PoolerRemovedIndicator messages delivered to
// CoordNodes. In production this is driven by an etcd watch stream.
//
// TargetCoord, if non-empty, restricts delivery to a single coord. This allows
// each coord to get an independent stream (with independent delivery delays)
// rather than a single broadcast.
type PoolerMembershipRequest struct {
	TargetCoord NodeID   // empty = all coords
	Discovered  []NodeID // poolers that newly appeared
	Removed     []NodeID // poolers that departed
}

func (PoolerMembershipRequest) consensusRequest() {}
