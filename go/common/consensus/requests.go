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

// RequestCorrelation can be embedded in any Request to associate an opaque
// correlation token with the exchange. The RequestHandler uses it to route the
// eventual response Request back to the node that originated the exchange
// without requiring production code to carry an explicit return address.
type RequestCorrelation struct {
	CorrelationID string // empty = no response routing needed
}

// --- Requests from CoordNode ---

// WritePolicyRequest asks the RequestHandler to deliver a DurabilityRules write
// to the target pooler (which must be the current primary). The primary validates
// the compare-and-swap: Rules.Seq must equal its current PolicySeq + 1. If it does
// not, the write is rejected and the primary returns its current seq so the coord
// can retry with the correct next seq.
//
// The RequestHandler auto-generates a correlation ID for each request and uses
// it to route the eventual WritePolicyResponseRequest back to FromCoord without
// requiring any extra routing state in production code.
type WritePolicyRequest struct {
	TargetPooler NodeID
	FromCoord    NodeID
	Rules        DurabilityRules // Rules.Seq is the CAS key (must be currentSeq+1)
}

func (WritePolicyRequest) consensusRequest() {}

// --- Requests from PoolerNode ---

// PolicyRecordApplyRequest is emitted by the primary PoolerNode when it needs
// the local postgres driver to apply a DurabilityRules change. The driver is
// responsible for the full apply sequence: updating synchronous_standby_names
// and then committing the SQL transaction. If the transaction fails, the driver
// must roll back the replication settings change and deliver a failure indicator.
//
// In production, the local driver goroutine handles this and delivers an
// ApplyRulesResponseIndicator back to the PoolerNode on completion.
// In simulation, the SimPooler wrapper intercepts this request (before it
// reaches the RequestHandler), simulates the apply, and queues the result
// indicator for the next tick.
type PolicyRecordApplyRequest struct {
	Rules DurabilityRules
}

func (PolicyRecordApplyRequest) consensusRequest() {}

// WritePolicyResponseRequest is emitted by a PoolerNode in response to a
// WritePolicyIndicator, after the local postgres write has either succeeded or
// failed. Accepted is true if the record was durably committed. When false,
// CurrentSeq carries the primary's actual current policy seq so the coord can
// correct its next seq on retry without a separate status round-trip.
//
// CorrelationID is echoed from the WritePolicyIndicator that triggered this
// response. The RequestHandler uses it to route the response back to the
// originating node.
type WritePolicyResponseRequest struct {
	RequestCorrelation
	Accepted   bool
	CurrentSeq int64 // primary's current policy seq when Accepted=false
}

func (WritePolicyResponseRequest) consensusRequest() {}

// PoolerStatusUpdateRequest is emitted by a PoolerNode whenever its committed
// state changes (rules write committed, WAL record applied, postgres stopped,
// or after a crash-restart). The RequestHandler delivers it to all known
// CoordNodes as a PoolerStatusIndicator so the coord can track cluster state.
type PoolerStatusUpdateRequest struct {
	State          PoolerPersistentState
	PostgresStatus PostgresStatus
	Properties     NodeProperties
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
