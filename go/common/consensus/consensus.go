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
// Primary is the read/write leader. Replica is any other pooler known to the orch,
// whether or not it is currently in the synchronous-replication quorum.
// The SyncReplicas field in ConsensusState identifies which replicas must acknowledge
// writes; that is the sync-quorum membership, separate from the role itself.
type PoolerRole int8

const (
	RoleUnknown PoolerRole = 0
	RolePrimary PoolerRole = 1
	RoleReplica PoolerRole = 2
)

func (r PoolerRole) String() string {
	switch r {
	case RolePrimary:
		return "primary"
	case RoleReplica:
		return "replica"
	default:
		return "unknown"
	}
}

// PostgresStatus is the operational state of a pooler's PostgreSQL instance.
// It is ephemeral (not persisted to disk) and is reported alongside the
// committed consensus state so the orch knows whether the pooler can apply changes.
type PostgresStatus int8

const (
	PostgresUnknown PostgresStatus = 0 // not yet reported; orch treats as potentially running
	PostgresRunning PostgresStatus = 1 // postgres is up; pooler can apply replication changes
	PostgresStopped PostgresStatus = 2 // postgres is down (e.g., received SIGTERM)
)

func (s PostgresStatus) String() string {
	switch s {
	case PostgresRunning:
		return "running"
	case PostgresStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// ConsensusPhase is the stage of the three-phase consensus protocol for a given
// voting term. Orch cycles through Begin → Revoke → Establish within a single term.
//
//   - Begin: establish this orch as the coordinator for the term; no topology change yet.
//   - Revoke: revoke the current primary (set PrimaryTerm=0, Primary=""); poolers must
//     apply this before establishment can proceed.
//   - Establish: appoint the new primary and sync-replica set.
type ConsensusPhase int8

const (
	PhaseBegin     ConsensusPhase = 1
	PhaseRevoke    ConsensusPhase = 2
	PhaseEstablish ConsensusPhase = 3
)

func (p ConsensusPhase) String() string {
	switch p {
	case PhaseBegin:
		return "begin"
	case PhaseRevoke:
		return "revoke"
	case PhaseEstablish:
		return "establish"
	default:
		return "unknown"
	}
}

// ConsensusState is the complete cluster configuration at a given term, sequence,
// and phase. Orch broadcasts this; poolers vote to accept or reject it.
type ConsensusState struct {
	VotingTerm   int64  // monotonically increasing; owned by the proposing coordinator
	CoordID      NodeID // which orch owns this voting term
	SeqNum       int64  // orders states within a term (1=begin, 2=revoke, 3=establish)
	Phase        ConsensusPhase
	PrimaryTerm  int64    // voting term when the current primary was established (0 = no primary)
	Primary      NodeID   // zero value means no primary appointed; check PrimaryTerm > 0
	SyncReplicas []NodeID // poolers that must acknowledge writes to the primary
}

// StateID identifies a specific ConsensusState proposal by its term + sequence number.
type StateID struct {
	VotingTerm int64
	SeqNum     int64
}

// PoolerPersistentState is the durable state a PoolerNode writes to storage.
// It survives process restarts and is loaded on startup via LoadCommittedState.
//
// Two invariants must hold at all times:
//
//  1. A committed state change must be remembered across crashes: the node must
//     persist this struct to stable storage (via PoolerStorage.Save) before
//     acknowledging any proposal. Consensus safety depends on committed votes
//     surviving restarts.
//
//  2. Once Applied=true is persisted, it must never be silently reverted: applied
//     state represents replication configuration written to disk (e.g.
//     primary_conninfo in postgresql.conf). That configuration survives process
//     restarts and is the ground truth for what role this node will take on next
//     start. Only a new accepted proposal (writing a new PoolerPersistentState
//     with its own Applied value) may supersede it.
type PoolerPersistentState struct {
	VotedTerm    int64  // highest VotingTerm this pooler has accepted
	VotedSeqNum  int64  // SeqNum within VotedTerm of the last accepted proposal
	VotedCoord   NodeID // coordinator that owns VotedTerm
	PrimaryTerm  int64  // VotingTerm at which the current primary was established (0 = none)
	Primary      NodeID // who is primary (may be a different node)
	Role         PoolerRole
	SyncReplicas []NodeID
	Applied      bool // true if the committed role change has been operationally executed
}

// VotedStateID returns the StateID of the last accepted proposal.
func (s PoolerPersistentState) VotedStateID() StateID {
	return StateID{VotingTerm: s.VotedTerm, SeqNum: s.VotedSeqNum}
}

// PoolerStorage is implemented by anything that durably persists PoolerPersistentState.
// In simulation it reads and writes an in-memory struct; in production it uses
// atomic write-rename+fsync to ensure committed votes survive crashes.
//
// PoolerNode calls Save synchronously within Step() before emitting any response.
// Load is called by NewPoolerNode (initial startup) and Restart() (crash recovery)
// to restore the last persisted state from durable storage.
type PoolerStorage interface {
	Save(state PoolerPersistentState) error
	Load() (PoolerPersistentState, error)
}

// RoleApplier executes the operational part of a committed role change on behalf of
// a PoolerNode. Apply is called on each tick when committed state has not been applied
// and postgres is running. It returns true if the role change completed successfully
// this tick, false to signal a transient failure—the pooler will retry next tick.
//
// In production, implementing Apply for a replica means writing primary_conninfo to
// postgresql.conf (or ALTER SYSTEM SET), ensuring standby.signal exists, and calling
// pg_reload_conf() to reconnect streaming replication without a full restart.
// Implementing Apply for a primary means setting synchronous_standby_names and calling
// pg_ctl promote (or SELECT pg_promote()) to exit standby mode.
//
// In simulation, use a fake implementation with a configurable failure rate.
//
// TODO: Consider binding replica replication configuration to a specific
// (NodeID, PrimaryTerm) pair rather than just NodeID — e.g. by using a
// term-specific replication password. This would prevent a replica that was
// configured for node X at term 5 from inadvertently participating in a quorum
// if node X is elected at a later term with different sync-replica membership.
// Implementation requires an invasive postgres change (term-keyed credentials)
// and is deferred until the benefit is better understood.
type RoleApplier interface {
	// Apply executes the committed role change. Returns true on success, false for
	// a transient failure (PoolerNode retries on the next tick while postgres is running).
	Apply(state PoolerPersistentState) bool

	// AppliedState returns the postgres configuration currently in effect on disk —
	// the last state for which Apply() returned true. In production this is read from
	// postgresql.conf / standby.signal (the postgres GUC files), which survive crashes
	// and are therefore recoverable on restart without re-running Apply().
	// Returns false if no role change has been applied yet (clean first-start).
	AppliedState() (PoolerPersistentState, bool)
}
