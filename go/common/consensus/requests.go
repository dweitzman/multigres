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
// TODO: Consider adding SeqNum (the sequence number from the OrchStateIndicator being
// responded to) so the orch can correlate responses to a specific proposal and discard
// late responses from earlier rounds. Currently the orch uses term numbers for this,
// but SeqNum would allow finer-grained correlation within a term.
type PoolerResponseRequest struct {
	ToOrch       NodeID
	Accepted     bool
	KnownTerm    int64  // if rejected: the term the pooler is currently on
	KnownCoordID NodeID // if rejected at the same term: which coord won it
}

func (PoolerResponseRequest) consensusRequest() {}

// --- Requests from the discovery node (simulation only) ---

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
