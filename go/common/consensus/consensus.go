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

// Package consensus implements a pure state machine library for distributed
// primary selection and configuration management in a PostgreSQL pooler cluster.
//
// The two node types are:
//   - OrchNode: coordinator (orch service); selects primaries and propagates
//     cluster configuration to poolers. Ephemeral — no disk state needed.
//   - PoolerNode: pool worker; votes on proposed states and tracks its own role.
//     Persistent — state survives crashes via an injected PoolerStorage.
//
// Both implement dstsim.Node[Indicator, Request, NodeID], so they can be registered
// in the same Simulator for deterministic testing with chaos injection.
//
// Safety invariant: at most one coordinator may successfully commit a state at any
// given VotingTerm. A pooler that has voted for term T under coordinator C1 will
// reject any proposal at term T from a different coordinator C2, reporting C1 as
// the winner so C2 can escalate to term T+1.
package consensus

// NodeID identifies any node in the simulation: both orch nodes and pooler nodes.
// Using a single ID type lets both node kinds live in one dstsim.Simulator instance.
type NodeID string

// PoolerRole is the role a pooler currently holds in the cluster.
type PoolerRole int8

const (
	RoleUnknown PoolerRole = 0
	RolePrimary PoolerRole = 1
	RoleReplica PoolerRole = 2
	RoleStandby PoolerRole = 3 // known to orch but not in the sync-replica set
)

// ConsensusState is the complete cluster configuration at a given term and sequence.
// Orch broadcasts this; poolers vote to accept or reject it.
type ConsensusState struct {
	VotingTerm   int64    // monotonically increasing; owned by the proposing coordinator
	CoordID      NodeID   // which orch owns this voting term
	SeqNum       int64    // orders states within a term
	PrimaryTerm  int64    // voting term when the current primary was established (0 = no primary)
	Primary      NodeID   // zero value means no primary appointed; check PrimaryTerm > 0
	SyncReplicas []NodeID // poolers that must acknowledge writes to the primary
}

// StateID identifies a specific ConsensusState proposal by its term + sequence number.
// Because orch may issue multiple proposals within a single VotingTerm (e.g. term 5
// seq 1 = revoke primary, term 5 seq 2 = appoint new primary), the sequence number
// is needed alongside the term to unambiguously identify which state was committed
// or applied.
type StateID struct {
	VotingTerm int64
	SeqNum     int64
}

// PoolerPersistentState is the durable state a PoolerNode writes to storage.
// It survives process restarts and is loaded on startup via PoolerStorage.Load.
type PoolerPersistentState struct {
	VotedTerm    int64  // highest VotingTerm this pooler has accepted
	VotedSeqNum  int64  // SeqNum within VotedTerm of the last accepted proposal
	VotedCoord   NodeID // coordinator that owns VotedTerm
	PrimaryTerm  int64  // VotingTerm at which this pooler was promoted (0 = not primary)
	Primary      NodeID // who is primary (may be a different node)
	Role         PoolerRole
	SyncReplicas []NodeID
}

// VotedStateID returns the StateID of the last accepted proposal.
func (s PoolerPersistentState) VotedStateID() StateID {
	return StateID{VotingTerm: s.VotedTerm, SeqNum: s.VotedSeqNum}
}

// PoolerStorage is implemented by anything that durably persists PoolerPersistentState.
// In simulation it writes to an in-memory struct; in production it uses atomic fsync.
// PoolerNode calls Save synchronously within Step() before emitting any response.
type PoolerStorage interface {
	Save(state PoolerPersistentState) error
}
