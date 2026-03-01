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
// given CoordTerm. A pooler that has voted for term T under coordinator C1 will
// reject any proposal at term T from a different coordinator C2, reporting C1 as
// the winner so C2 can escalate to term T+1.
package consensus

// NodeID identifies any node in the simulation: both orch nodes and pooler nodes.
// Using a single ID type lets both node kinds live in one dstsim.Simulator instance.
type NodeID string

// PoolerRole is the role a pooler currently holds in the cluster.
// Primary is the read/write leader. Replica is any other pooler known to the orch,
// whether or not it is currently in the synchronous-replication quorum.
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

// ProposalID uniquely identifies a consensus proposal.
// CoordTerm is the coordinator term (monotonically increasing per election round).
// CoordID is the orch node that owns this coordinator term.
// SeqNum orders proposals within a term (1=begin, 2=revoke, 3=establish).
type ProposalID struct {
	CoordTerm int64
	CoordID   NodeID
	SeqNum    int64
}

// ConsensusState is the complete cluster configuration at a given term, sequence,
// and phase. Orch broadcasts this; poolers vote to accept or reject it.
type ConsensusState struct {
	ProposalID  // embedded: CoordTerm, CoordID, SeqNum
	Phase       ConsensusPhase
	PrimaryTerm int64  // CoordTerm when the current primary was established (0 = no primary)
	Primary     NodeID // zero value means no primary appointed; check PrimaryTerm > 0

	// QuorumSpec is the serialized Quorum for this Establish proposal, produced by
	// Quorum.Serialize(). Forwarded verbatim to poolers so they can persist it and
	// reconstruct the historical Quorum on orch restart. Non-nil only on PhaseEstablish.
	QuorumSpec []byte

	// CohortMembers is the voting cohort as known to the orch at proposal time.
	//
	// On Begin and Revoke proposals, CohortMembers contains all poolers that have
	// already reported CohortMember=true in their status — i.e. the set of nodes
	// that participated in a previous successful Establish. An empty list signals
	// that the orch has no confirmed cohort, which is the case on a fresh cluster
	// bootstrap or on a restarted orch that has not yet received pooler status updates.
	//
	// On Establish proposals, CohortMembers contains all currently known poolers,
	// admitting them into the voting cohort. A pooler sets its persistent CohortMember
	// flag when it commits an Establish that lists its own ID here.
	//
	// Safety: a pooler with CohortMember=true rejects any proposal that does not
	// include its own ID in CohortMembers. This prevents a new or partitioned orch
	// that hasn't discovered the existing cohort from bootstrapping over a healthy
	// cluster — such an orch will send an empty CohortMembers list until it receives
	// pooler status updates and calls learnEstablishedQuorum.
	CohortMembers []NodeID
}

// StateID identifies a specific ConsensusState proposal by its coordinator term + sequence number.
type StateID struct {
	CoordTerm int64
	SeqNum    int64
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
	// Committed is the proposal identity of the last accepted proposal.
	Committed   ProposalID
	PrimaryTerm int64  // CoordTerm at which the current primary was established (0 = none)
	Primary     NodeID // who is primary (may be a different node)
	Role        PoolerRole
	Applied     bool // true if the committed role change has been operationally executed
	// QuorumSpec is the serialized Quorum from the Establish proposal. Persisted so the
	// write quorum can be reconstructed on orch restart. See ConsensusState.QuorumSpec.
	QuorumSpec []byte
	// CohortMember is true once this pooler has committed an Establish proposal that
	// listed its own ID in CohortMembers. It is sticky: once set it is never cleared by
	// subsequent proposals. A pooler with CohortMember=true rejects any proposal that
	// does not include its ID in CohortMembers (see ConsensusState.CohortMembers).
	CohortMember bool
}

// RejectionReason is a best-effort hint carried in a rejection response indicating
// why a pooler rejected a proposal. The orch must not rely on this for correctness —
// message loss or reordering means it may never arrive — but it can act on it to
// recover more quickly (e.g. re-fetching status before the next retry).
type RejectionReason int8

const (
	// RejectionReasonUnknown is the zero value; used when the response is an
	// acceptance or when no specific reason is provided.
	RejectionReasonUnknown RejectionReason = 0
	// RejectionReasonStaleTerm: the orch's CoordTerm is behind the pooler's
	// committed CoordTerm. KnownTerm and KnownCoordID in the response identify the winner.
	RejectionReasonStaleTerm RejectionReason = 1
	// RejectionReasonCoordConflict: same CoordTerm but a different coordinator
	// already won it. KnownCoordID in the response identifies the winner.
	RejectionReasonCoordConflict RejectionReason = 2
	// RejectionReasonStaleSeqNum: SeqNum goes backwards within the same term
	// and coordinator — the proposal is an out-of-order duplicate.
	RejectionReasonStaleSeqNum RejectionReason = 3
	// RejectionReasonPrimaryTermMismatch: the orch's ExpectedPrimaryTerm does
	// not match the pooler's committed PrimaryTerm. The orch is acting on a
	// stale cluster view; the piggybacked status update carries the fresh state.
	RejectionReasonPrimaryTermMismatch RejectionReason = 4
	// RejectionReasonCohortMembership: the pooler is a cohort member but the
	// orch did not list it in CohortMembers, indicating the orch has not yet
	// discovered the full cohort. The piggybacked status update carries the
	// fresh state so the orch can include this pooler on its next attempt.
	RejectionReasonCohortMembership RejectionReason = 5
)

func (r RejectionReason) String() string {
	switch r {
	case RejectionReasonStaleTerm:
		return "stale_term"
	case RejectionReasonCoordConflict:
		return "coord_conflict"
	case RejectionReasonStaleSeqNum:
		return "stale_seq_num"
	case RejectionReasonPrimaryTermMismatch:
		return "primary_term_mismatch"
	case RejectionReasonCohortMembership:
		return "cohort_membership"
	default:
		return "unknown"
	}
}

// CommittedStateID returns the StateID of the last accepted proposal.
func (s PoolerPersistentState) CommittedStateID() StateID {
	return StateID{CoordTerm: s.Committed.CoordTerm, SeqNum: s.Committed.SeqNum}
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
