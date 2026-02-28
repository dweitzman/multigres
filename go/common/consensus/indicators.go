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

// Indicator is implemented by all incoming events in the consensus protocol.
// OrchNode and PoolerNode both receive Indicator values in their Step() calls.
type Indicator interface {
	consensusIndicator()
}

// --- Indicators for OrchNode (from local etcd watcher — reliable, ordered) ---

// PoolerDiscoveredIndicator is delivered to OrchNode when a new pooler registers in etcd.
type PoolerDiscoveredIndicator struct {
	PoolerID NodeID
}

func (PoolerDiscoveredIndicator) consensusIndicator() {}

// PoolerRemovedIndicator is delivered to OrchNode when a pooler deregisters from etcd.
type PoolerRemovedIndicator struct {
	PoolerID NodeID
}

func (PoolerRemovedIndicator) consensusIndicator() {}

// PoolerStatusIndicator is delivered to OrchNode when a pooler broadcasts its status.
// It carries the pooler's committed state, whether it has been applied, and the
// current PostgreSQL operational status so the orch can make informed decisions.
type PoolerStatusIndicator struct {
	PoolerID       NodeID
	StatusSeq      int64 // monotonically increasing; orch discards stale updates
	State          PoolerPersistentState
	Applied        bool           // true if the committed state has been operationally executed
	PostgresStatus PostgresStatus // current postgres operational status
	// LastApplied is the postgres configuration currently in effect on disk.
	// Zero value (Role==RoleUnknown) means nothing has been applied yet.
	LastApplied PoolerPersistentState
}

func (PoolerStatusIndicator) consensusIndicator() {}

// PoolerResponseIndicator is delivered to OrchNode when a pooler votes on a proposal.
type PoolerResponseIndicator struct {
	FromPooler   NodeID
	VotingTerm   int64 // term of the proposal being responded to
	SeqNum       int64 // seq num of the proposal being responded to
	Accepted     bool
	KnownTerm    int64  // if rejected: the term the pooler is currently on
	KnownCoordID NodeID // if rejected at the same term: which coordinator won that term
}

func (PoolerResponseIndicator) consensusIndicator() {}

// --- Indicators for PoolerNode (from orch over the network — unreliable) ---

// OrchStateIndicator is delivered to PoolerNode when orch broadcasts a ConsensusState.
// ExpectedPrimaryTerm is a CAS check: if > 0 the pooler rejects the state unless its
// current PrimaryTerm matches, protecting against an orch acting on stale cluster info.
type OrchStateIndicator struct {
	FromOrch            NodeID
	State               ConsensusState
	ExpectedPrimaryTerm int64
}

func (OrchStateIndicator) consensusIndicator() {}

// TerminateIndicator is delivered to a PoolerNode to request graceful shutdown.
// The pooler sets its PostgresStatus to Stopped and stops applying replication-role
// changes. In production this corresponds to a SIGTERM sent to the multipooler process.
type TerminateIndicator struct{}

func (TerminateIndicator) consensusIndicator() {}
