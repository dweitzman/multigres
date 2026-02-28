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

// Request is implemented by all outgoing desires emitted by consensus nodes.
// The RequestHandler converts Requests into Indicators delivered to target nodes.
type Request interface {
	consensusRequest()
}

// --- Requests from OrchNode ---

// BroadcastStateRequest asks the RequestHandler to deliver the given ConsensusState
// to each target pooler as an OrchStateIndicator.
// Targets is the list of pooler NodeIDs to address; nil means all known poolers.
// ExpectedPrimaryTerm is forwarded verbatim into each OrchStateIndicator.
type BroadcastStateRequest struct {
	State               ConsensusState
	ExpectedPrimaryTerm int64    // forwarded to poolers for CAS validation
	Targets             []NodeID // nil = all known poolers
}

func (BroadcastStateRequest) consensusRequest() {}

// --- Requests from PoolerNode ---

// PoolerResponseRequest asks the RequestHandler to deliver the pooler's vote/rejection
// back to the originating orch as a PoolerResponseIndicator.
//
// VotingTerm and SeqNum echo back the proposal being responded to so the orch can
// discard late responses from earlier rounds or terms.
type PoolerResponseRequest struct {
	ToOrch       NodeID
	VotingTerm   int64 // term of the proposal being responded to
	SeqNum       int64 // seq num of the proposal being responded to
	Accepted     bool
	KnownTerm    int64  // if rejected: the term the pooler is currently on
	KnownCoordID NodeID // if rejected at the same term: which coord won it
}

func (PoolerResponseRequest) consensusRequest() {}

// PoolerStatusUpdateRequest is emitted by a PoolerNode whenever its status changes
// (e.g., after committing a new state, after applying a role change, or after receiving
// a TerminateIndicator). The RequestHandler delivers this to all known orch nodes as
// a PoolerStatusIndicator so the orch can track applied state and postgres health.
type PoolerStatusUpdateRequest struct {
	Applied        bool
	PostgresStatus PostgresStatus
	State          PoolerPersistentState // the committed (goal) state
	// LastApplied is the postgres configuration currently in effect on disk —
	// the last state for which Apply() succeeded. When Applied=false, this
	// differs from State and tells the orch what role postgres is actually
	// running right now (recoverable from postgresql.conf / standby.signal).
	// Zero value (Role==RoleUnknown) means nothing has been applied yet.
	LastApplied PoolerPersistentState
}

func (PoolerStatusUpdateRequest) consensusRequest() {}

// --- Requests from test/driver nodes (simulation only) ---

// PoolerMembershipRequest is emitted by the discovery node when the set of registered
// poolers changes. The RequestHandler converts this into PoolerDiscoveredIndicator and
// PoolerRemovedIndicator messages delivered to all known orch nodes.
//
// In production the equivalent information is delivered via an etcd watch; this request
// type exists only in simulation to drive orch discovery without a real etcd connection.
type PoolerMembershipRequest struct {
	Discovered []NodeID // poolers that newly appeared
	Removed    []NodeID // poolers that departed
}

func (PoolerMembershipRequest) consensusRequest() {}

// TerminateRequest is emitted by a driver/test node to request that a specific pooler
// shuts down gracefully. The RequestHandler converts this to a TerminateIndicator
// delivered to the target. In production, this corresponds to a SIGTERM signal.
type TerminateRequest struct {
	Target NodeID
}

func (TerminateRequest) consensusRequest() {}
